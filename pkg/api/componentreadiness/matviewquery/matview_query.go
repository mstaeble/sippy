package matviewquery

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	fischer "github.com/glycerine/golang-fisher-exact"

	"github.com/openshift/sippy/pkg/apis/api/componentreport/crstatus"
	"github.com/openshift/sippy/pkg/apis/api/componentreport/crtest"
	"github.com/openshift/sippy/pkg/apis/api/componentreport/reqopts"
	"github.com/openshift/sippy/pkg/db"
)

var (
	dimensionMu     sync.RWMutex
	cachedVCMap     map[uint]variantCombo
	cachedOwnership map[ownershipKey]ownershipEntry
)

// RefreshDimensionCache reloads variant combinations and test ownerships.
// Call this after matview refresh so subsequent queries use fresh data.
func RefreshDimensionCache(ctx context.Context, dbc *db.DB) error {
	vcMap, err := loadVariantCombinations(ctx, dbc)
	if err != nil {
		return err
	}
	ownerships, err := loadTestOwnerships(ctx, dbc)
	if err != nil {
		return err
	}
	dimensionMu.Lock()
	cachedVCMap = vcMap
	cachedOwnership = ownerships
	dimensionMu.Unlock()
	log.WithField("variant_combinations", len(vcMap)).
		WithField("test_ownerships", len(ownerships)).
		Info("refreshed dimension cache")
	return nil
}

func getDimensionData(ctx context.Context, dbc *db.DB) (map[uint]variantCombo, map[ownershipKey]ownershipEntry, error) {
	dimensionMu.RLock()
	vc, own := cachedVCMap, cachedOwnership
	dimensionMu.RUnlock()
	if vc != nil && own != nil {
		return vc, own, nil
	}
	return loadAndCacheDimensions(ctx, dbc)
}

func loadAndCacheDimensions(ctx context.Context, dbc *db.DB) (map[uint]variantCombo, map[ownershipKey]ownershipEntry, error) {
	if err := RefreshDimensionCache(ctx, dbc); err != nil {
		return nil, nil, err
	}
	dimensionMu.RLock()
	defer dimensionMu.RUnlock()
	return cachedVCMap, cachedOwnership, nil
}

// CandidateRow holds one row from the dbGroupBy-aggregated candidate query.
// Counts are already aggregated across variant combinations sharing the same
// dbGroupBy key. The VariantKey is a comma-separated string of "Key:Value" pairs.
type CandidateRow struct {
	TestID        uint   `gorm:"column:test_id"`
	SuiteID       uint   `gorm:"column:suite_id"`
	VariantKey    string `gorm:"column:variant_key"`
	SampleTotal   int    `gorm:"column:sample_total"`
	SampleSuccess int    `gorm:"column:sample_success"`
	SampleFlake   int    `gorm:"column:sample_flake"`
	BaseTotal     *int   `gorm:"column:base_total"`
	BaseSuccess   *int   `gorm:"column:base_success"`
	BaseFlake     *int   `gorm:"column:base_flake"`
}

// CellGridRow identifies a (component, variant_key) cell from the pre-computed grid.
type CellGridRow struct {
	Component  string `gorm:"column:component"`
	VariantKey string `gorm:"column:variant_key"`
}

// QueryResult contains everything the report generator needs from the matview path.
type QueryResult struct {
	BaseStatus   map[string]crstatus.TestStatus
	SampleStatus map[string]crstatus.TestStatus
	CellGrid     []CellGridEntry
}

// CellGridEntry is a (component, variants) cell in the report grid.
type CellGridEntry struct {
	Component string
	Variants  map[string]string
}

// resolveMatchingVCIDs returns the variant_combination_ids that match the include_variants filter.
func resolveMatchingVCIDs(ctx context.Context, dbc *db.DB, includeVariants map[string][]string) ([]int, error) {
	if len(includeVariants) == 0 {
		var ids []int
		if err := dbc.DB.WithContext(ctx).Raw("SELECT id FROM variant_combinations").Pluck("id", &ids).Error; err != nil {
			return nil, err
		}
		return ids, nil
	}

	var clauses []string
	var args []interface{}
	for key, values := range includeVariants {
		formatted := make([]string, len(values))
		for i, v := range values {
			formatted[i] = key + ":" + v
		}
		if len(formatted) == 1 {
			clauses = append(clauses, "variants @> ?::text[]")
			args = append(args, pq.StringArray(formatted))
		} else {
			clauses = append(clauses, "variants && ?::text[]")
			args = append(args, pq.StringArray(formatted))
		}
	}

	var ids []int
	q := dbc.DB.WithContext(ctx).Raw(
		"SELECT id FROM variant_combinations WHERE "+strings.Join(clauses, " AND "), args...)
	if err := q.Pluck("id", &ids).Error; err != nil {
		return nil, fmt.Errorf("resolving matching variant combination IDs: %w", err)
	}
	return ids, nil
}

type variantCombo struct {
	ID       uint
	Variants map[string]string
}

type ownershipEntry struct {
	UniqueID     string
	Component    string
	Capabilities []string
}

type ownershipKey struct {
	TestID  uint
	SuiteID uint
}

// crDataSource describes where to read CR test status data.
// If Table is set, it names a matview or the GA table directly.
// If Table is empty, query test_daily_summaries with the date range.
type crDataSource struct {
	Table   string
	Release string
	Start   time.Time
	End     time.Time
}

func (s crDataSource) isDailySummary() bool { return s.Table == "" }

// SelectCRMatview picks the right time-windowed matview for the given window,
// or returns empty string if no standard matview matches.
func SelectCRMatview(start, end time.Time) string {
	days := end.Sub(start).Hours() / 24
	switch {
	case days >= 5 && days <= 9:
		return "cr_test_status_7d_matview"
	case days >= 25 && days <= 35:
		return "cr_test_status_30d_matview"
	case days >= 80 && days <= 100:
		return "cr_test_status_90d_matview"
	default:
		return ""
	}
}

// checkDateCoverage verifies that test_daily_summaries has data covering
// the start of the requested date range for a release.
func checkDateCoverage(ctx context.Context, dbc *db.DB, release string, start time.Time) error {
	var minDate *time.Time
	err := dbc.DB.WithContext(ctx).Raw(
		"SELECT MIN(summary_date) FROM test_daily_summaries WHERE release = ?",
		release).Row().Scan(&minDate)
	if err != nil {
		return fmt.Errorf("checking date coverage for %s: %w", release, err)
	}
	if minDate == nil {
		return fmt.Errorf("no daily summary data for release %s", release)
	}
	startDate := start.Truncate(24 * time.Hour)
	if minDate.After(startDate) {
		return fmt.Errorf("daily summary data for %s starts at %s, need %s",
			release, minDate.Format("2006-01-02"), startDate.Format("2006-01-02"))
	}
	return nil
}

// QueryMatviewTestStatus queries the matview (or daily summaries) for failure
// candidates with dbGroupBy aggregation done in SQL, then applies Fisher
// Exact Test in Go.
func QueryMatviewTestStatus(ctx context.Context, dbc *db.DB, opts reqopts.RequestOptions) (*QueryResult, error) {
	before := time.Now()
	fLog := log.WithField("func", "QueryMatviewTestStatus")

	var gaExists bool
	dbc.DB.WithContext(ctx).Raw(
		"SELECT EXISTS(SELECT 1 FROM prow_ga_test_statuses_matview WHERE release = @release)",
		sql.Named("release", opts.BaseRelease.Name)).Scan(&gaExists)

	covG, covCtx := errgroup.WithContext(ctx)
	covG.Go(func() error {
		return checkDateCoverage(covCtx, dbc, opts.SampleRelease.Name, opts.SampleRelease.Start)
	})
	if !gaExists {
		covG.Go(func() error {
			return checkDateCoverage(covCtx, dbc, opts.BaseRelease.Name, opts.BaseRelease.Start)
		})
	}
	if err := covG.Wait(); err != nil {
		fLog.WithError(err).Warn("insufficient daily summary coverage")
		return nil, err
	}

	matchingVCIDs, err := resolveMatchingVCIDs(ctx, dbc, opts.VariantOption.IncludeVariants)
	if err != nil {
		return nil, err
	}
	if len(matchingVCIDs) == 0 {
		fLog.Warn("no variant combinations match include_variants filter")
		return &QueryResult{}, nil
	}

	_, ownerships, err := getDimensionData(ctx, dbc)
	if err != nil {
		return nil, err
	}

	dbGroupByKeys := opts.VariantOption.DBGroupBy.List()
	sort.Strings(dbGroupByKeys)

	sampleSource := crDataSource{
		Table:   SelectCRMatview(opts.SampleRelease.Start, opts.SampleRelease.End),
		Release: opts.SampleRelease.Name,
		Start:   opts.SampleRelease.Start,
		End:     opts.SampleRelease.End,
	}

	baseSource := crDataSource{
		Release: opts.BaseRelease.Name,
		Start:   opts.BaseRelease.Start,
		End:     opts.BaseRelease.End,
	}
	if gaExists {
		baseSource.Table = "prow_ga_test_statuses_matview"
		fLog.WithField("release", opts.BaseRelease.Name).Info("using GA test status table for base data")
	} else {
		baseSource.Table = SelectCRMatview(opts.BaseRelease.Start, opts.BaseRelease.End)
	}

	fLog.WithField("sample_source", sourceLabel(sampleSource)).
		WithField("base_source", sourceLabel(baseSource)).
		Info("selected data sources")

	var candidates []CandidateRow
	var gridRows []CellGridRow

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		candidates, err = queryCandidates(gctx, dbc, sampleSource, baseSource, matchingVCIDs, dbGroupByKeys)
		return err
	})
	g.Go(func() error {
		var err error
		gridRows, err = queryCellGrid(gctx, dbc, sampleSource, baseSource, matchingVCIDs, dbGroupByKeys)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	fLog.WithField("candidates", len(candidates)).Info("queried failure candidates")

	result := analyzeAndBuild(candidates, gridRows, ownerships, opts)

	fLog.WithField("base_status", len(result.BaseStatus)).
		WithField("sample_status", len(result.SampleStatus)).
		WithField("grid_cells", len(result.CellGrid)).
		WithField("elapsed", time.Since(before)).
		Info("matview query complete")

	return result, nil
}

func sourceLabel(s crDataSource) string {
	if s.Table != "" {
		return s.Table
	}
	return fmt.Sprintf("daily_summaries[%s..%s]", s.Start.Format("2006-01-02"), s.End.Format("2006-01-02"))
}

// buildVCParsedCTE generates a CTE that parses variant_combinations.variants
// into individual columns for the given dbGroupBy keys.
func buildVCParsedCTE(dbGroupByKeys []string) string {
	var cols []string
	for _, k := range dbGroupByKeys {
		cols = append(cols, fmt.Sprintf(
			"MAX(CASE WHEN v LIKE '%s:%%' THEN split_part(v, ':', 2) END) AS %s",
			k, quoteIdent(k)))
	}
	return fmt.Sprintf(`vc_parsed AS (
		SELECT id, %s
		FROM variant_combinations, unnest(variants) v
		GROUP BY id
	)`, strings.Join(cols, ", "))
}

func quoteIdent(s string) string {
	return fmt.Sprintf(`"%s"`, s)
}

// variantKeyExpr generates a SQL expression that builds a comma-separated
// variant key string from the dbGroupBy columns.
func variantKeyExpr(prefix string, dbGroupByKeys []string) string {
	var parts []string
	for _, k := range dbGroupByKeys {
		parts = append(parts, fmt.Sprintf("'%s:' || COALESCE(%s.%s, '')", k, prefix, quoteIdent(k)))
	}
	return "concat_ws(',', " + strings.Join(parts, ", ") + ")"
}

// dbGroupByColumns returns a comma-separated list of prefixed column references.
func dbGroupByColumns(prefix string, dbGroupByKeys []string) string {
	var cols []string
	for _, k := range dbGroupByKeys {
		cols = append(cols, prefix+"."+quoteIdent(k))
	}
	return strings.Join(cols, ", ")
}

// dbGroupByMatchClauses generates IS NOT DISTINCT FROM clauses for a LATERAL JOIN.
func dbGroupByMatchClauses(leftPrefix, rightPrefix string, dbGroupByKeys []string) string {
	var clauses []string
	for _, k := range dbGroupByKeys {
		clauses = append(clauses, fmt.Sprintf("%s.%s IS NOT DISTINCT FROM %s.%s",
			leftPrefix, quoteIdent(k), rightPrefix, quoteIdent(k)))
	}
	return strings.Join(clauses, " AND ")
}

// sourceTableOrCTE returns the table/CTE name for the data source, adding a
// daily summary CTE to ctes if needed. The returned ref can be used in FROM.
func sourceTableOrCTE(alias string, src crDataSource, matchingVCIDs []int, ctes *[]string, params map[string]interface{}) string {
	if !src.isDailySummary() {
		return src.Table
	}
	startParam := alias + "_start"
	endParam := alias + "_end"
	releaseParam := alias + "_release"
	vcidsParam := alias + "_vcids"

	cte := fmt.Sprintf(`%s AS (
		SELECT tds.test_id, tds.suite_id, pj.variant_combination_id,
		       tds.release,
		       SUM(tds.runs)::int AS total_count,
		       SUM(tds.successes + tds.flakes)::int AS success_count,
		       SUM(tds.flakes)::int AS flake_count
		FROM test_daily_summaries tds
		JOIN prow_jobs pj ON tds.prow_job_id = pj.id
		WHERE tds.release = @%s
		  AND tds.summary_date >= @%s AND tds.summary_date < @%s
		  AND pj.variant_combination_id = ANY(@%s)
		GROUP BY tds.test_id, tds.suite_id, pj.variant_combination_id, tds.release
	)`, alias, releaseParam, startParam, endParam, vcidsParam)

	*ctes = append(*ctes, cte)
	params[startParam] = src.Start
	params[endParam] = src.End
	params[releaseParam] = src.Release
	params[vcidsParam] = pq.Array(matchingVCIDs)
	return alias
}

func queryCandidates(ctx context.Context, dbc *db.DB, sample, base crDataSource, matchingVCIDs []int, dbGroupByKeys []string) ([]CandidateRow, error) {
	var ctes []string
	params := map[string]interface{}{}

	ctes = append(ctes, buildVCParsedCTE(dbGroupByKeys))

	sampleRef := sourceTableOrCTE("sample_data", sample, matchingVCIDs, &ctes, params)
	baseRef := sourceTableOrCTE("base_data", base, matchingVCIDs, &ctes, params)

	if !sample.isDailySummary() {
		params["sample_release"] = sample.Release
		params["matching_vcids"] = pq.Array(matchingVCIDs)
	}
	if !base.isDailySummary() {
		params["base_release"] = base.Release
	}

	sampleFilter := ""
	if !sample.isDailySummary() {
		sampleFilter = "AND mv.release = @sample_release AND mv.variant_combination_id = ANY(@matching_vcids)"
	}

	baseFilter := ""
	if !base.isDailySummary() {
		baseFilter = "AND mv.release = @base_release"
	}

	dbCols := dbGroupByColumns("vp", dbGroupByKeys)
	variantKey := variantKeyExpr("s", dbGroupByKeys)
	matchClauses := dbGroupByMatchClauses("s", "vp2", dbGroupByKeys)

	query := fmt.Sprintf(`WITH %s,
		sample_agg AS (
			SELECT mv.test_id, mv.suite_id, %s,
				SUM(mv.total_count) AS total_count,
				SUM(mv.success_count) AS success_count,
				SUM(mv.flake_count) AS flake_count
			FROM %s mv
			JOIN vc_parsed vp ON mv.variant_combination_id = vp.id
			WHERE TRUE %s
			GROUP BY mv.test_id, mv.suite_id, %s
			HAVING SUM(mv.total_count) > SUM(mv.success_count)
		)
		SELECT s.test_id, s.suite_id,
			%s AS variant_key,
			s.total_count AS sample_total,
			s.success_count AS sample_success,
			s.flake_count AS sample_flake,
			b.base_total, b.base_success, b.base_flake
		FROM sample_agg s
		LEFT JOIN LATERAL (
			SELECT SUM(mv.total_count)::int AS base_total,
			       SUM(mv.success_count)::int AS base_success,
			       SUM(mv.flake_count)::int AS base_flake
			FROM %s mv
			JOIN vc_parsed vp2 ON mv.variant_combination_id = vp2.id
			WHERE mv.test_id = s.test_id AND mv.suite_id = s.suite_id
			  AND %s
			  %s
		) b ON true`,
		strings.Join(ctes, ", "),
		dbCols,
		sampleRef, sampleFilter, dbCols,
		variantKey,
		baseRef, matchClauses, baseFilter)

	var namedArgs []interface{}
	for k, v := range params {
		namedArgs = append(namedArgs, sql.Named(k, v))
	}

	var rows []CandidateRow
	if err := dbc.DB.WithContext(ctx).Raw(query, namedArgs...).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("querying candidates: %w", err)
	}
	return rows, nil
}

func queryCellGrid(ctx context.Context, dbc *db.DB, sample, base crDataSource, matchingVCIDs []int, dbGroupByKeys []string) ([]CellGridRow, error) {
	vcParsedCTE := buildVCParsedCTE(dbGroupByKeys)
	variantKey := variantKeyExpr("vp", dbGroupByKeys)

	if !sample.isDailySummary() && !base.isDailySummary() {
		var rows []CellGridRow
		err := dbc.DB.WithContext(ctx).Raw(fmt.Sprintf(`
			WITH %s
			SELECT DISTINCT cg.component, %s AS variant_key
			FROM cr_cell_grids cg
			JOIN vc_parsed vp ON cg.variant_combination_id = vp.id
			WHERE cg.release IN (@base_release, @sample_release)
			  AND cg.variant_combination_id = ANY(@matching_vcids)
		`, vcParsedCTE, variantKey),
			sql.Named("base_release", base.Release),
			sql.Named("sample_release", sample.Release),
			sql.Named("matching_vcids", pq.Array(matchingVCIDs)),
		).Scan(&rows).Error
		if err != nil {
			return nil, fmt.Errorf("querying cell grid: %w", err)
		}
		return rows, nil
	}

	var rows []CellGridRow
	err := dbc.DB.WithContext(ctx).Raw(fmt.Sprintf(`
		WITH %s
		SELECT DISTINCT tow.component, %s AS variant_key
		FROM test_daily_summaries tds
		JOIN prow_jobs pj ON tds.prow_job_id = pj.id
		JOIN vc_parsed vp ON pj.variant_combination_id = vp.id
		JOIN test_ownerships tow ON tow.test_id = tds.test_id
		    AND (tow.suite_id = tds.suite_id OR (tow.suite_id IS NULL AND tds.suite_id = 0))
		WHERE tds.release IN (@base_release, @sample_release)
		  AND tds.summary_date >= @start AND tds.summary_date < @end
		  AND pj.variant_combination_id = ANY(@matching_vcids)
		  AND tow.staff_approved_obsolete = false
		  AND pj.variant_combination_id IS NOT NULL
	`, vcParsedCTE, variantKey),
		sql.Named("base_release", base.Release),
		sql.Named("sample_release", sample.Release),
		sql.Named("start", earliest(sample.Start, base.Start)),
		sql.Named("end", latest(sample.End, base.End)),
		sql.Named("matching_vcids", pq.Array(matchingVCIDs)),
	).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("querying cell grid from daily summaries: %w", err)
	}
	return rows, nil
}

func earliest(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func latest(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func loadVariantCombinations(ctx context.Context, dbc *db.DB) (map[uint]variantCombo, error) {
	type row struct {
		ID       uint           `gorm:"column:id"`
		Variants pq.StringArray `gorm:"column:variants;type:text[]"`
	}
	var rows []row
	if err := dbc.DB.WithContext(ctx).Raw("SELECT id, variants FROM variant_combinations").Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("loading variant_combinations: %w", err)
	}
	result := make(map[uint]variantCombo, len(rows))
	for _, r := range rows {
		parsed := make(map[string]string, len(r.Variants))
		for _, v := range r.Variants {
			if k, val, ok := strings.Cut(v, ":"); ok {
				parsed[k] = val
			}
		}
		result[r.ID] = variantCombo{ID: r.ID, Variants: parsed}
	}
	return result, nil
}

func loadTestOwnerships(ctx context.Context, dbc *db.DB) (map[ownershipKey]ownershipEntry, error) {
	type row struct {
		TestID       uint           `gorm:"column:test_id"`
		SuiteID      *uint          `gorm:"column:suite_id"`
		UniqueID     string         `gorm:"column:unique_id"`
		Component    string         `gorm:"column:component"`
		Capabilities pq.StringArray `gorm:"column:capabilities;type:text[]"`
	}
	var rows []row
	if err := dbc.DB.WithContext(ctx).
		Raw("SELECT test_id, suite_id, unique_id, component, capabilities FROM test_ownerships WHERE staff_approved_obsolete = false").
		Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("loading test_ownerships: %w", err)
	}
	result := make(map[ownershipKey]ownershipEntry, len(rows))
	for _, r := range rows {
		sid := uint(0)
		if r.SuiteID != nil {
			sid = *r.SuiteID
		}
		result[ownershipKey{TestID: r.TestID, SuiteID: sid}] = ownershipEntry{
			UniqueID:     r.UniqueID,
			Component:    r.Component,
			Capabilities: r.Capabilities,
		}
	}
	return result, nil
}

func parseVariantKey(variantKey string) map[string]string {
	result := make(map[string]string)
	for _, pair := range strings.Split(variantKey, ",") {
		if k, v, ok := strings.Cut(pair, ":"); ok {
			result[k] = v
		}
	}
	return result
}

func variantMapToSortedSlice(m map[string]string) []string {
	result := make([]string, 0, len(m))
	for k, v := range m {
		result = append(result, k+":"+v)
	}
	sort.Strings(result)
	return result
}

// analyzeAndBuild applies Fisher Exact Test to the dbGroupBy-aggregated
// candidates and builds the report status maps. No Go-side dbGroupBy
// aggregation is needed since SQL already did it.
func analyzeAndBuild(
	candidates []CandidateRow,
	gridRows []CellGridRow,
	ownerships map[ownershipKey]ownershipEntry,
	opts reqopts.RequestOptions,
) *QueryResult {
	minimumFailure := opts.AdvancedOption.MinimumFailure
	pityFactor := float64(opts.AdvancedOption.PityFactor) / 100.0
	confidence := opts.AdvancedOption.Confidence

	baseStatus := make(map[string]crstatus.TestStatus)
	sampleStatus := make(map[string]crstatus.TestStatus)

	for _, c := range candidates {
		owner := ownerships[ownershipKey{TestID: c.TestID, SuiteID: c.SuiteID}]
		if owner.UniqueID == "" {
			continue
		}
		variants := parseVariantKey(c.VariantKey)

		sampleFailures := c.SampleTotal - c.SampleSuccess
		if sampleFailures < minimumFailure {
			continue
		}
		baseTotal := 0
		baseSuccess := 0
		baseFlake := 0
		if c.BaseTotal != nil {
			baseTotal = *c.BaseTotal
			baseSuccess = *c.BaseSuccess
			baseFlake = *c.BaseFlake
		}
		if baseTotal == 0 {
			continue
		}

		basePassRate := float64(baseSuccess) / float64(baseTotal)
		samplePassRate := float64(c.SampleSuccess) / float64(c.SampleTotal)
		if basePassRate-samplePassRate <= pityFactor {
			continue
		}

		_, _, fisherP, _ := fischer.FisherExactTest(
			c.SampleTotal-c.SampleSuccess, c.SampleSuccess,
			baseTotal-baseSuccess, baseSuccess,
		)
		if fisherP >= 1.0-float64(confidence)/100.0 {
			continue
		}

		testKey := crtest.KeyWithVariants{
			TestID:   owner.UniqueID,
			Variants: variants,
		}
		keyStr := testKey.KeyOrDie()
		variantSlice := variantMapToSortedSlice(variants)

		sampleStatus[keyStr] = crstatus.TestStatus{
			Component:    owner.Component,
			Capabilities: owner.Capabilities,
			Variants:     variantSlice,
			Count: crtest.Count{
				TotalCount:   c.SampleTotal,
				SuccessCount: c.SampleSuccess,
				FlakeCount:   c.SampleFlake,
			},
		}
		baseStatus[keyStr] = crstatus.TestStatus{
			Component:    owner.Component,
			Capabilities: owner.Capabilities,
			Variants:     variantSlice,
			Count: crtest.Count{
				TotalCount:   baseTotal,
				SuccessCount: baseSuccess,
				FlakeCount:   baseFlake,
			},
		}
	}

	seen := make(map[string]bool)
	var cellGrid []CellGridEntry
	for _, g := range gridRows {
		if seen[g.Component+"|"+g.VariantKey] {
			continue
		}
		seen[g.Component+"|"+g.VariantKey] = true
		cellGrid = append(cellGrid, CellGridEntry{
			Component: g.Component,
			Variants:  parseVariantKey(g.VariantKey),
		})
	}

	return &QueryResult{
		BaseStatus:   baseStatus,
		SampleStatus: sampleStatus,
		CellGrid:     cellGrid,
	}
}

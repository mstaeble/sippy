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

// CandidateRow holds one row from the failure-only matview query.
// Both sample and base counts are included for the same (test_id, suite_id, variant_combination_id).
type CandidateRow struct {
	TestID               uint `gorm:"column:test_id"`
	SuiteID              uint `gorm:"column:suite_id"`
	VariantCombinationID uint `gorm:"column:variant_combination_id"`
	SampleTotal          int  `gorm:"column:sample_total"`
	SampleSuccess        int  `gorm:"column:sample_success"`
	SampleFlake          int  `gorm:"column:sample_flake"`
	BaseTotal            *int `gorm:"column:base_total"`
	BaseSuccess          *int `gorm:"column:base_success"`
	BaseFlake            *int `gorm:"column:base_flake"`
}

// CellGridRow identifies a (component, variant_combination_id) cell from the pre-computed grid.
type CellGridRow struct {
	Component            string `gorm:"column:component"`
	VariantCombinationID uint   `gorm:"column:variant_combination_id"`
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
			clauses = append(clauses, "variants @> ARRAY[?]")
			args = append(args, formatted[0])
		} else {
			clauses = append(clauses, "variants && ARRAY[?]::text[]")
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
	if time.Since(end) > 24*time.Hour {
		return ""
	}
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

// QueryMatviewTestStatus runs the failure-only matview query and performs
// dbGroupBy aggregation and Fisher Exact Test in Go.
func QueryMatviewTestStatus(ctx context.Context, dbc *db.DB, opts reqopts.RequestOptions) (*QueryResult, error) {
	before := time.Now()
	fLog := log.WithField("func", "QueryMatviewTestStatus")

	matchingVCIDs, err := resolveMatchingVCIDs(ctx, dbc, opts.VariantOption.IncludeVariants)
	if err != nil {
		return nil, err
	}
	if len(matchingVCIDs) == 0 {
		fLog.Warn("no variant combinations match include_variants filter")
		return &QueryResult{}, nil
	}

	vcMap, ownerships, err := getDimensionData(ctx, dbc)
	if err != nil {
		return nil, err
	}

	dbGroupByKeys := opts.VariantOption.DBGroupBy.List()

	vcidToDBGroup := make(map[uint]map[string]string, len(vcMap))
	for _, vc := range vcMap {
		vcidToDBGroup[vc.ID] = filterVariantsByKeys(vc.Variants, dbGroupByKeys)
	}

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
	var gaExists bool
	dbc.DB.WithContext(ctx).Raw(
		"SELECT EXISTS(SELECT 1 FROM prow_ga_test_statuses WHERE release = @release)",
		sql.Named("release", opts.BaseRelease.Name)).Scan(&gaExists)
	if gaExists {
		baseSource.Table = "prow_ga_test_statuses"
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
		candidates, err = queryCandidates(gctx, dbc, sampleSource, baseSource, matchingVCIDs)
		return err
	})
	g.Go(func() error {
		var err error
		gridRows, err = queryCellGrid(gctx, dbc, sampleSource, baseSource, matchingVCIDs)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	fLog.WithField("candidates", len(candidates)).Info("queried failure candidates")

	result := aggregateAndAnalyze(candidates, gridRows, vcidToDBGroup, ownerships, dbGroupByKeys, opts)

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

// dailySummaryCTE returns a CTE clause and its named parameters for aggregating
// test_daily_summaries over a date range. The CTE output matches the matview
// schema: (test_id, suite_id, variant_combination_id, release, total_count, success_count, flake_count).
func dailySummaryCTE(alias string, src crDataSource, matchingVCIDs []int) (string, map[string]interface{}) {
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

	params := map[string]interface{}{
		startParam:   src.Start,
		endParam:     src.End,
		releaseParam: src.Release,
		vcidsParam:   pq.Array(matchingVCIDs),
	}
	return cte, params
}

func queryCandidates(ctx context.Context, dbc *db.DB, sample, base crDataSource, matchingVCIDs []int) ([]CandidateRow, error) {
	var ctes []string
	params := map[string]interface{}{}

	sampleRef := sample.Table
	if sample.isDailySummary() {
		cte, p := dailySummaryCTE("sample_data", sample, matchingVCIDs)
		ctes = append(ctes, cte)
		for k, v := range p {
			params[k] = v
		}
		sampleRef = "sample_data"
	} else {
		params["sample_release"] = sample.Release
		params["matching_vcids"] = pq.Array(matchingVCIDs)
	}

	baseRef := base.Table
	if base.isDailySummary() {
		cte, p := dailySummaryCTE("base_data", base, matchingVCIDs)
		ctes = append(ctes, cte)
		for k, v := range p {
			params[k] = v
		}
		baseRef = "base_data"
	} else {
		params["base_release"] = base.Release
	}

	var withClause string
	if len(ctes) > 0 {
		withClause = "WITH " + strings.Join(ctes, ", ")
	}

	sampleFilter := ""
	if !sample.isDailySummary() {
		sampleFilter = "AND sf.release = @sample_release AND sf.variant_combination_id = ANY(@matching_vcids)"
	}

	query := fmt.Sprintf(`%s
		SELECT sf.test_id, sf.suite_id, sf.variant_combination_id,
		       sf.total_count AS sample_total,
		       sf.success_count AS sample_success,
		       sf.flake_count AS sample_flake,
		       b.total_count AS base_total,
		       b.success_count AS base_success,
		       b.flake_count AS base_flake
		FROM %s sf
		LEFT JOIN %s b
		    ON b.test_id = sf.test_id
		    AND b.suite_id = sf.suite_id
		    AND b.variant_combination_id = sf.variant_combination_id
		    AND b.release = @base_release
		WHERE sf.total_count > sf.success_count
		  %s
	`, withClause, sampleRef, baseRef, sampleFilter)

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

func queryCellGrid(ctx context.Context, dbc *db.DB, sample, base crDataSource, matchingVCIDs []int) ([]CellGridRow, error) {
	if !sample.isDailySummary() && !base.isDailySummary() {
		var rows []CellGridRow
		err := dbc.DB.WithContext(ctx).Raw(`
			SELECT DISTINCT component, variant_combination_id
			FROM cr_cell_grids
			WHERE release IN (@base_release, @sample_release)
			  AND variant_combination_id = ANY(@matching_vcids)
		`,
			sql.Named("base_release", base.Release),
			sql.Named("sample_release", sample.Release),
			sql.Named("matching_vcids", pq.Array(matchingVCIDs)),
		).Scan(&rows).Error
		if err != nil {
			return nil, fmt.Errorf("querying cell grid: %w", err)
		}
		return rows, nil
	}

	// For daily summary fallback, compute the cell grid dynamically.
	var rows []CellGridRow
	err := dbc.DB.WithContext(ctx).Raw(`
		SELECT DISTINCT tow.component, pj.variant_combination_id
		FROM test_daily_summaries tds
		JOIN prow_jobs pj ON tds.prow_job_id = pj.id
		JOIN test_ownerships tow ON tow.test_id = tds.test_id
		    AND (tow.suite_id = tds.suite_id OR (tow.suite_id IS NULL AND tds.suite_id = 0))
		WHERE tds.release IN (@base_release, @sample_release)
		  AND tds.summary_date >= @start AND tds.summary_date < @end
		  AND pj.variant_combination_id = ANY(@matching_vcids)
		  AND tow.staff_approved_obsolete = false
		  AND pj.variant_combination_id IS NOT NULL
	`,
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

func filterVariantsByKeys(variants map[string]string, keys []string) map[string]string {
	filtered := make(map[string]string, len(keys))
	for _, k := range keys {
		if v, ok := variants[k]; ok {
			filtered[k] = v
		}
	}
	return filtered
}

func variantMapToSortedSlice(m map[string]string) []string {
	result := make([]string, 0, len(m))
	for k, v := range m {
		result = append(result, k+":"+v)
	}
	sort.Strings(result)
	return result
}

type aggregationKey struct {
	TestID    uint
	SuiteID   uint
	DBGroupBy string // JSON-serialized sorted variant map for map key
}

type aggregatedCounts struct {
	SampleTotal   int
	SampleSuccess int
	SampleFlake   int
	BaseTotal     int
	BaseSuccess   int
	BaseFlake     int
}

func aggregateAndAnalyze(
	candidates []CandidateRow,
	gridRows []CellGridRow,
	vcidToDBGroup map[uint]map[string]string,
	ownerships map[ownershipKey]ownershipEntry,
	dbGroupByKeys []string,
	opts reqopts.RequestOptions,
) *QueryResult {
	// Aggregate candidates by (test_id, suite_id, dbgroup)
	agg := make(map[aggregationKey]*aggregatedCounts)
	aggVariants := make(map[aggregationKey]map[string]string) // preserve variant map

	for _, c := range candidates {
		dbgroupVariants := vcidToDBGroup[c.VariantCombinationID]
		key := aggregationKey{
			TestID:    c.TestID,
			SuiteID:   c.SuiteID,
			DBGroupBy: fmt.Sprint(variantMapToSortedSlice(dbgroupVariants)),
		}
		if _, ok := aggVariants[key]; !ok {
			aggVariants[key] = dbgroupVariants
		}
		existing, ok := agg[key]
		if !ok {
			existing = &aggregatedCounts{}
			agg[key] = existing
		}
		existing.SampleTotal += c.SampleTotal
		existing.SampleSuccess += c.SampleSuccess
		existing.SampleFlake += c.SampleFlake
		if c.BaseTotal != nil {
			existing.BaseTotal += *c.BaseTotal
			existing.BaseSuccess += *c.BaseSuccess
			existing.BaseFlake += *c.BaseFlake
		}
	}

	// Apply pre-filters and Fisher test, build status maps
	minimumFailure := opts.AdvancedOption.MinimumFailure
	pityFactor := float64(opts.AdvancedOption.PityFactor) / 100.0
	confidence := opts.AdvancedOption.Confidence

	baseStatus := make(map[string]crstatus.TestStatus)
	sampleStatus := make(map[string]crstatus.TestStatus)

	for key, counts := range agg {
		owner := ownerships[ownershipKey{TestID: key.TestID, SuiteID: key.SuiteID}]
		if owner.UniqueID == "" {
			continue
		}
		variants := aggVariants[key]

		sampleFailures := counts.SampleTotal - counts.SampleSuccess
		if sampleFailures < minimumFailure {
			continue
		}
		if counts.BaseTotal == 0 {
			continue
		}

		basePassRate := float64(counts.BaseSuccess) / float64(counts.BaseTotal)
		samplePassRate := float64(counts.SampleSuccess) / float64(counts.SampleTotal)
		if basePassRate-samplePassRate <= pityFactor {
			continue
		}

		_, _, fisherP, _ := fischer.FisherExactTest(
			counts.SampleTotal-counts.SampleSuccess, counts.SampleSuccess,
			counts.BaseTotal-counts.BaseSuccess, counts.BaseSuccess,
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
				TotalCount:   counts.SampleTotal,
				SuccessCount: counts.SampleSuccess,
				FlakeCount:   counts.SampleFlake,
			},
		}
		baseStatus[keyStr] = crstatus.TestStatus{
			Component:    owner.Component,
			Capabilities: owner.Capabilities,
			Variants:     variantSlice,
			Count: crtest.Count{
				TotalCount:   counts.BaseTotal,
				SuccessCount: counts.BaseSuccess,
				FlakeCount:   counts.BaseFlake,
			},
		}
	}

	// Build cell grid
	seen := make(map[string]bool)
	var cellGrid []CellGridEntry
	for _, g := range gridRows {
		dbgroupVariants := vcidToDBGroup[g.VariantCombinationID]
		cellKey := g.Component + "|" + fmt.Sprint(variantMapToSortedSlice(dbgroupVariants))
		if seen[cellKey] {
			continue
		}
		seen[cellKey] = true
		cellGrid = append(cellGrid, CellGridEntry{
			Component: g.Component,
			Variants:  dbgroupVariants,
		})
	}

	return &QueryResult{
		BaseStatus:   baseStatus,
		SampleStatus: sampleStatus,
		CellGrid:     cellGrid,
	}
}

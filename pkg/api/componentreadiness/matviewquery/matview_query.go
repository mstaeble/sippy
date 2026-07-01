package matviewquery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/civil"
	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/openshift/sippy/pkg/api/componentreadiness/utils"
	"github.com/openshift/sippy/pkg/apis/api/componentreport/crstatus"
	"github.com/openshift/sippy/pkg/apis/api/componentreport/crtest"
	"github.com/openshift/sippy/pkg/apis/api/componentreport/reqopts"
	"github.com/openshift/sippy/pkg/db"
)

// candidateRow holds one row from the dbGroupBy-aggregated candidate query.
// Counts are already aggregated across variant combinations sharing the same
// dbGroupBy key. The VariantKey is a comma-separated string of "Key:Value" pairs.
type candidateRow struct {
	UniqueID      string         `gorm:"column:unique_id"`
	Component     string         `gorm:"column:component"`
	Capabilities  pq.StringArray `gorm:"column:capabilities;type:text[]"`
	VariantKey    string         `gorm:"column:variant_key"`
	SampleTotal   int            `gorm:"column:sample_total"`
	SampleSuccess int            `gorm:"column:sample_success"`
	SampleFlake   int            `gorm:"column:sample_flake"`
	BaseTotal     *int           `gorm:"column:base_total"`
	BaseSuccess   *int           `gorm:"column:base_success"`
	BaseFlake     *int           `gorm:"column:base_flake"`
}

// cellGridRow identifies a (component, variant_key) cell from the pre-computed grid.
type cellGridRow struct {
	Component  string `gorm:"column:component"`
	VariantKey string `gorm:"column:variant_key"`
}

// baseStatusRow holds one row from the GA matview base-only query.
type baseStatusRow struct {
	UniqueID     string         `gorm:"column:unique_id"`
	Component    string         `gorm:"column:component"`
	Capabilities pq.StringArray `gorm:"column:capabilities;type:text[]"`
	VariantKey   string         `gorm:"column:variant_key"`
	TotalCount   int            `gorm:"column:total_count"`
	SuccessCount int            `gorm:"column:success_count"`
	FlakeCount   int            `gorm:"column:flake_count"`
}

type lastFailureRow struct {
	UniqueID    string    `gorm:"column:unique_id"`
	LastFailure time.Time `gorm:"column:last_failure"`
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

// crDataSource describes where to read CR test status data (always a matview or GA table).
type crDataSource struct {
	Table   string
	Label   string
	Release string
}

// selectCRMatviewLabel picks the right time-windowed matview label for the
// given window, or returns empty string if no standard matview matches.
// The window [start, end) is a half-open interval where end is exclusive.
func selectCRMatviewLabel(start, end civil.Date) string {
	today := civil.DateOf(time.Now().UTC())
	tomorrow := today.AddDays(1)
	lookback := today.DaysSince(start)
	switch {
	case end == tomorrow && lookback == 7:
		return "7d"
	case end == tomorrow && lookback == 30:
		return "30d"
	case end == tomorrow && lookback == 90:
		return "90d"
	case today.DaysSince(end) == 30 && lookback == 60:
		return "60d_30d"
	default:
		return ""
	}
}

func matviewTableName(label string) string {
	return "cr_test_status_" + label + "_matview"
}

// isGABaseWindow returns true if the base release window matches the GA data
// window, ending before today.
func isGABaseWindow(base reqopts.Release) bool {
	today := civil.DateOf(time.Now().UTC())
	gaDate := base.End.AddDays(-1)
	return base.Start == utils.GAWindowStart(gaDate) && base.End.Before(today)
}

// QueryMatviewTestStatus queries the matview for failure
// candidates with dbGroupBy aggregation done in SQL, then applies Fisher
// Exact Test in Go.
func QueryMatviewTestStatus(ctx context.Context, dbc *db.DB, opts reqopts.RequestOptions) (*QueryResult, error) {
	before := time.Now()
	fLog := log.WithField("func", "QueryMatviewTestStatus")

	dbGroupByKeys := opts.VariantOption.DBGroupBy.List()

	includeVariants := opts.VariantOption.IncludeVariants

	sampleLabel := selectCRMatviewLabel(opts.SampleRelease.Start, opts.SampleRelease.End)
	if sampleLabel == "" {
		return nil, fmt.Errorf("no matview covers sample window %s to %s",
			opts.SampleRelease.Start, opts.SampleRelease.End)
	}
	sampleSource := crDataSource{
		Table:   matviewTableName(sampleLabel),
		Label:   sampleLabel,
		Release: opts.SampleRelease.Name,
	}

	baseSource := crDataSource{
		Release: opts.BaseRelease.Name,
	}
	if isGABaseWindow(opts.BaseRelease) {
		baseSource.Table = "prow_ga_test_statuses_matview"
		fLog.WithField("release", opts.BaseRelease.Name).Info("using GA test status table for base data")
	} else {
		baseLabel := selectCRMatviewLabel(opts.BaseRelease.Start, opts.BaseRelease.End)
		if baseLabel == "" {
			return nil, fmt.Errorf("no matview covers base window %s to %s",
				opts.BaseRelease.Start, opts.BaseRelease.End)
		}
		baseSource.Table = matviewTableName(baseLabel)
	}

	fLog.WithField("sample_source", sampleSource.Table).
		WithField("base_source", baseSource.Table).
		Info("selected data sources")

	compareVariants := opts.VariantOption.CompareVariants
	crossCompareKeys := opts.VariantOption.VariantCrossCompare

	var candidates []candidateRow
	var gridRows []cellGridRow

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		candidates, err = queryCandidates(gctx, dbc, sampleSource, baseSource, includeVariants, compareVariants, crossCompareKeys, dbGroupByKeys)
		return err
	})
	g.Go(func() error {
		var err error
		gridRows, err = queryCellGrid(gctx, dbc, sampleSource, baseSource, includeVariants, compareVariants, crossCompareKeys, dbGroupByKeys)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	result := analyzeAndBuild(candidates, gridRows, opts)

	// Fetch last_failure only for tests that passed the Fisher filter
	if len(result.SampleStatus) > 0 {
		lastFailures, err := queryLastFailures(ctx, dbc, result, opts.SampleRelease.Name, opts.SampleRelease.Start, opts.SampleRelease.End)
		if err != nil {
			fLog.WithError(err).Warn("failed to query last failures, continuing without")
		}
		for keyStr, ss := range result.SampleStatus {
			var key crtest.KeyWithVariants
			if err := json.Unmarshal([]byte(keyStr), &key); err != nil {
				continue
			}
			if lf, ok := lastFailures[key.TestID]; ok {
				ss.LastFailure = lf
				result.SampleStatus[keyStr] = ss
			}
		}
	}

	fLog.WithField("base_status", len(result.BaseStatus)).
		WithField("sample_status", len(result.SampleStatus)).
		WithField("grid_cells", len(result.CellGrid)).
		WithField("elapsed", time.Since(before)).
		Info("matview query complete")

	return result, nil
}

// QueryGATestStatus queries the GA matview for base test status for a release.
// This is used by the fallback middleware to get previous release data without
// hitting BigQuery.
func QueryGATestStatus(ctx context.Context, dbc *db.DB, release string, includeVariants map[string][]string, dbGroupByKeys []string) (map[string]crstatus.TestStatus, error) {

	vcidCTE, vcidParams := buildMatchingVCIDsCTE("matching_vcids", includeVariants)
	vcParsedCTE := buildVCParsedCTE("vc_parsed", "matching_vcids", dbGroupByKeys)

	dbCols := dbGroupByColumns("vp", dbGroupByKeys)
	variantKey := variantKeyExpr("agg", dbGroupByKeys)

	query := fmt.Sprintf(`WITH %s, %s
		SELECT tow.unique_id, tow.component, tow.capabilities,
			%s AS variant_key,
			SUM(mv.total_count)::int AS total_count,
			SUM(mv.success_count)::int AS success_count,
			SUM(mv.flake_count)::int AS flake_count
		FROM prow_ga_test_statuses_matview mv
		JOIN vc_parsed vp ON mv.variant_combination_id = vp.id
		JOIN test_ownerships tow ON tow.test_id = mv.test_id
			AND (tow.suite_id = mv.suite_id OR (tow.suite_id IS NULL AND mv.suite_id = 0))
			AND tow.staff_approved_obsolete = false
		WHERE mv.release = ?
		GROUP BY tow.unique_id, tow.component, tow.capabilities, %s`,
		vcidCTE, vcParsedCTE,
		variantKey,
		dbCols)

	var args []any
	args = append(args, vcidParams...)
	args = append(args, release)

	var rows []baseStatusRow
	if err := dbc.DB.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("querying GA test status: %w", err)
	}

	status := make(map[string]crstatus.TestStatus, len(rows))
	for _, r := range rows {
		variants := parseVariantKey(r.VariantKey)
		testKey := crtest.KeyWithVariants{
			TestID:   r.UniqueID,
			Variants: variants,
		}
		keyStr := testKey.KeyOrDie()
		status[keyStr] = crstatus.TestStatus{
			Component:    r.Component,
			Capabilities: r.Capabilities,
			Variants:     variantMapToSortedSlice(variants),
			Count: crtest.Count{
				TotalCount:   r.TotalCount,
				SuccessCount: r.SuccessCount,
				FlakeCount:   r.FlakeCount,
			},
		}
	}

	log.WithField("release", release).
		WithField("tests", len(status)).
		Info("queried GA test status from matview")

	return status, nil
}

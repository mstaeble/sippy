package matviewquery

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/civil"

	"github.com/openshift/sippy/pkg/apis/api/componentreport/crtest"
	"github.com/openshift/sippy/pkg/db"
	"github.com/openshift/sippy/pkg/util/sets"
)

func queryCandidates(ctx context.Context, dbc *db.DB, sample, base crDataSource, includeVariants, compareVariants map[string][]string, crossCompareKeys, dbGroupByKeys []string) ([]candidateRow, error) {
	isCrossCompare := len(crossCompareKeys) > 0 && len(compareVariants) > 0
	sampleVariants := includeVariants
	if isCrossCompare {
		sampleVariants = mergeVariants(includeVariants, compareVariants)
	}

	var ctes []string
	var variantParams []any
	if isCrossCompare {
		sampleCTE, sampleParams := buildMatchingVCIDsCTE("sample_matching_vcids", sampleVariants)
		baseCTE, baseParams := buildMatchingVCIDsCTE("base_matching_vcids", includeVariants)
		ctes = []string{
			sampleCTE,
			baseCTE,
			buildVCParsedCTE("sample_vc_parsed", "sample_matching_vcids", dbGroupByKeys),
			buildVCParsedCTE("base_vc_parsed", "base_matching_vcids", dbGroupByKeys),
		}
		variantParams = append(variantParams, sampleParams...)
		variantParams = append(variantParams, baseParams...)
	} else {
		sampleCTE, sampleParams := buildMatchingVCIDsCTE("sample_matching_vcids", sampleVariants)
		ctes = []string{
			sampleCTE,
			"base_matching_vcids AS (SELECT id FROM sample_matching_vcids)",
			buildVCParsedCTE("sample_vc_parsed", "sample_matching_vcids", dbGroupByKeys),
			"base_vc_parsed AS (SELECT * FROM sample_vc_parsed)",
		}
		variantParams = sampleParams
	}

	dbCols := dbGroupByColumns("vp", dbGroupByKeys)
	variantKey := variantKeyExpr("s", dbGroupByKeys)

	matchKeys := excludeKeys(dbGroupByKeys, crossCompareKeys)
	matchFragment := ""
	if len(matchKeys) > 0 {
		matchFragment = " AND " + dbGroupByMatchClauses("s", "vp2", matchKeys)
	}

	query := fmt.Sprintf(`WITH %s,
		sample_agg AS (
			SELECT mv.test_id, mv.suite_id, %s,
				SUM(mv.total_count) AS total_count,
				SUM(mv.success_count) AS success_count,
				SUM(mv.flake_count) AS flake_count
			FROM %s mv
			JOIN sample_vc_parsed vp ON mv.variant_combination_id = vp.id
			WHERE mv.release = ?
			GROUP BY mv.test_id, mv.suite_id, %s
			HAVING SUM(mv.total_count) > SUM(mv.success_count)
		)
		SELECT s.test_id, s.suite_id,
			tow.unique_id, tow.component, tow.capabilities,
			%s AS variant_key,
			s.total_count AS sample_total,
			s.success_count AS sample_success,
			s.flake_count AS sample_flake,
			b.base_total, b.base_success, b.base_flake
		FROM sample_agg s
		JOIN test_ownerships tow ON tow.test_id = s.test_id
			AND (tow.suite_id = s.suite_id OR (tow.suite_id IS NULL AND s.suite_id = 0))
			AND tow.staff_approved_obsolete = false
		LEFT JOIN LATERAL (
			SELECT SUM(mv.total_count)::int AS base_total,
			       SUM(mv.success_count)::int AS base_success,
			       SUM(mv.flake_count)::int AS base_flake
			FROM %s mv
			JOIN base_vc_parsed vp2 ON mv.variant_combination_id = vp2.id
			WHERE mv.test_id = s.test_id AND mv.suite_id = s.suite_id
			  AND mv.release = ?%s
		) b ON true`,
		strings.Join(ctes, ", "),
		dbCols,
		sample.Table, dbCols,
		variantKey,
		base.Table, matchFragment)

	var args []any
	args = append(args, variantParams...)
	args = append(args, sample.Release, base.Release)

	var rows []candidateRow
	if err := dbc.DB.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("querying candidates: %w", err)
	}
	return rows, nil
}

func queryCellGrid(ctx context.Context, dbc *db.DB, sample, base crDataSource, includeVariants, compareVariants map[string][]string, crossCompareKeys, dbGroupByKeys []string) ([]cellGridRow, error) {
	sampleVariants := includeVariants
	if len(crossCompareKeys) > 0 && len(compareVariants) > 0 {
		sampleVariants = mergeVariants(includeVariants, compareVariants)
	}

	vcidCTE, vcidParams := buildMatchingVCIDsCTE("matching_vcids", sampleVariants)
	vcParsedCTE := buildVCParsedCTE("vc_parsed", "matching_vcids", dbGroupByKeys)
	variantKey := variantKeyExpr("vp", dbGroupByKeys)

	cellGridMatview := "cr_cell_grid_" + sample.Label + "_matview"

	var args []any
	args = append(args, vcidParams...)
	args = append(args, base.Release, sample.Release)

	var rows []cellGridRow
	err := dbc.DB.WithContext(ctx).Raw(fmt.Sprintf(`
		WITH %s, %s
		SELECT DISTINCT cg.component, %s AS variant_key
		FROM %s cg
		JOIN vc_parsed vp ON cg.variant_combination_id = vp.id
		WHERE cg.release IN (?, ?)
	`, vcidCTE, vcParsedCTE, variantKey, cellGridMatview),
		args...,
	).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("querying cell grid: %w", err)
	}
	return rows, nil
}

func queryLastFailures(ctx context.Context, dbc *db.DB, qr *QueryResult, release string, start, end civil.Date) (map[string]time.Time, error) {
	uniqueIDSet := sets.NewString()
	for keyStr := range qr.SampleStatus {
		var key crtest.KeyWithVariants
		if err := json.Unmarshal([]byte(keyStr), &key); err == nil {
			uniqueIDSet.Insert(key.TestID)
		}
	}
	uniqueIDs := uniqueIDSet.List()

	var rows []lastFailureRow
	err := dbc.DB.WithContext(ctx).Raw(`
		SELECT tow.unique_id,
		       MAX(pjrt.prow_job_run_timestamp) AS last_failure
		FROM prow_job_run_tests pjrt
		JOIN test_ownerships tow ON tow.test_id = pjrt.test_id
		WHERE tow.unique_id IN ?
		  AND pjrt.status NOT IN (1, 13)
		  AND pjrt.prow_job_run_release = ?
		  AND pjrt.prow_job_run_timestamp >= ?
		  AND pjrt.prow_job_run_timestamp < ?
		  AND pjrt.deleted_at IS NULL
		GROUP BY tow.unique_id
	`, uniqueIDs, release, start, end).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("querying last failures: %w", err)
	}

	result := make(map[string]time.Time, len(rows))
	for _, r := range rows {
		result[r.UniqueID] = r.LastFailure
	}
	return result, nil
}

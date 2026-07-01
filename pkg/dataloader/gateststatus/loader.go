package gateststatus

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"google.golang.org/api/iterator"

	log "github.com/sirupsen/logrus"

	"cloud.google.com/go/civil"

	"github.com/openshift/sippy/pkg/api/componentreadiness/utils"
	bqcachedclient "github.com/openshift/sippy/pkg/bigquery"
	"github.com/openshift/sippy/pkg/db"
	"github.com/openshift/sippy/pkg/db/models"
)

// GATestStatusLoader populates prow_ga_test_statuses for releases that have reached GA.
//
// The loader has two phases:
//  1. Fetch: query BigQuery for the GA window and persist raw results in
//     prow_ga_raw_test_data (keyed by release + ga_date). This runs only when the
//     raw data is missing or the GA date changed, or when forced.
//  2. Aggregate: re-derive prow_ga_test_statuses from prow_ga_raw_test_data by joining
//     with current dimension tables (tests, suites, prow_jobs). This runs
//     every load cycle so that changes to dimension tables are reflected
//     without re-querying BigQuery.
type GATestStatusLoader struct {
	ctx      context.Context
	dbc      *db.DB
	bqClient *bqcachedclient.Client
	force    bool
	releases []string
	errs     []error
}

func New(ctx context.Context, dbc *db.DB, bqClient *bqcachedclient.Client, force bool, releases []string) *GATestStatusLoader {
	return &GATestStatusLoader{
		ctx:      ctx,
		dbc:      dbc,
		bqClient: bqClient,
		force:    force,
		releases: releases,
	}
}

func (l *GATestStatusLoader) Name() string    { return "ga-test-status" }
func (l *GATestStatusLoader) Errors() []error { return l.errs }

func (l *GATestStatusLoader) Load() {
	start := time.Now()

	releases, err := l.gaReleases()
	if err != nil {
		l.errs = append(l.errs, err)
		return
	}
	if len(releases) == 0 {
		log.Info("ga-test-status: no GA releases found, skipping")
		return
	}

	for _, rel := range releases {
		if err := l.loadRelease(rel); err != nil {
			l.errs = append(l.errs, fmt.Errorf("release %s: %w", rel.Release, err))
		}
	}

	log.WithField("elapsed", time.Since(start)).
		WithField("releases", len(releases)).
		Info("ga-test-status: load complete")
}

func (l *GATestStatusLoader) gaReleases() ([]models.ReleaseDefinition, error) {
	var defs []models.ReleaseDefinition
	q := l.dbc.DB.WithContext(l.ctx).Where("ga_date < NOW()")
	if len(l.releases) > 0 {
		q = q.Where("release IN ?", l.releases)
	}
	if err := q.Find(&defs).Error; err != nil {
		return nil, fmt.Errorf("querying release_definitions: %w", err)
	}
	return defs, nil
}

func (l *GATestStatusLoader) loadRelease(rel models.ReleaseDefinition) error {
	rLog := log.WithField("release", rel.Release)
	gaDate := civil.DateOf(rel.GADate.UTC())
	gaStart := utils.GAWindowStart(gaDate)
	gaEnd := utils.GAWindowEnd(gaDate)

	return l.ensureRawData(rLog, rel.Release, gaDate, gaStart, gaEnd)
}

// ensureRawData fetches from BigQuery and persists into prow_ga_raw_test_data if the
// raw data is missing or the GA date has changed.
func (l *GATestStatusLoader) ensureRawData(rLog *log.Entry, release string, gaDate, gaStart, gaEnd civil.Date) error {
	if !l.force {
		var rd models.ReleaseDefinition
		err := l.dbc.DB.WithContext(l.ctx).
			Where("release = ?", release).
			First(&rd).Error
		if err == nil && rd.LoadedGADate != nil && *rd.LoadedGADate == gaDate {
			rLog.Debug("ga-test-status: raw data exists with matching GA date, skipping fetch")
			return nil
		}
	}

	rLog.WithField("window", fmt.Sprintf("%s to %s", gaStart, gaEnd)).
		Info("ga-test-status: fetching from BigQuery")

	allRows, err := l.fetchFromBigQuery(release, gaStart, gaEnd)
	if err != nil {
		return fmt.Errorf("fetching from BigQuery: %w", err)
	}
	rLog.WithField("bq_rows", len(allRows)).Info("ga-test-status: fetched from BigQuery")

	if len(allRows) == 0 {
		rLog.Warn("ga-test-status: BigQuery returned zero rows")
		return nil
	}

	if err := l.dbc.DB.WithContext(l.ctx).Where("release = ?", release).Delete(&models.ProwGARawTestDatum{}).Error; err != nil {
		return fmt.Errorf("deleting existing raw rows: %w", err)
	}

	rLog.WithField("rows", len(allRows)).Info("ga-test-status: inserting into database")
	sqlDB, err := l.dbc.DB.DB()
	if err != nil {
		return fmt.Errorf("getting sql.DB: %w", err)
	}
	conn, err := stdlib.AcquireConn(sqlDB)
	if err != nil {
		return fmt.Errorf("acquiring pgx conn: %w", err)
	}
	defer stdlib.ReleaseConn(sqlDB, conn)

	copyRows := make([][]any, len(allRows))
	for i, r := range allRows {
		copyRows[i] = []any{release, r.TestName, r.JobName, r.Suite, r.Passes, r.Failures, r.Flakes, r.Runs}
	}

	n, err := conn.CopyFrom(l.ctx,
		pgx.Identifier{"prow_ga_raw_test_data"},
		[]string{"release", "test_name", "job_name", "suite", "passes", "failures", "flakes", "runs"},
		pgx.CopyFromRows(copyRows),
	)
	if err != nil {
		return fmt.Errorf("COPY into prow_ga_raw_test_data: %w", err)
	}

	if err := l.dbc.DB.WithContext(l.ctx).
		Model(&models.ReleaseDefinition{}).
		Where("release = ?", release).
		Update("ga_data_loaded_date", gaDate).Error; err != nil {
		return fmt.Errorf("recording load status: %w", err)
	}

	rLog.WithField("bq_rows", n).Info("ga-test-status: raw data persisted")
	return nil
}

// fetchFromBigQuery executes the BQ query and collects all result rows into a slice.
func (l *GATestStatusLoader) fetchFromBigQuery(release string, from, to civil.Date) ([]stagingRow, error) {
	q := l.bqClient.BQ.Query(fmt.Sprintf(`
		WITH deduped AS (
			SELECT
				junit.test_name,
				jobs.prowjob_job_name AS job_name,
				COALESCE(junit.testsuite, '') AS suite_name,
				CASE WHEN junit.flake_count > 0 THEN 0 ELSE junit.success_val END AS adjusted_success,
				CASE WHEN junit.flake_count > 0 THEN 1 ELSE 0 END AS is_flake,
				ROW_NUMBER() OVER(
					PARTITION BY junit.file_path, junit.test_name, junit.testsuite
					ORDER BY CASE WHEN junit.flake_count > 0 THEN 0 WHEN junit.success_val > 0 THEN 1 ELSE 2 END
				) AS row_num
			FROM %[1]s.junit
			JOIN %[1]s.jobs jobs ON junit.prowjob_build_id = jobs.prowjob_build_id
			JOIN %[1]s.job_variants jv ON jobs.prowjob_job_name = jv.job_name
				AND jv.variant_name = 'Release' AND jv.variant_value = @release
			WHERE junit.modified_time >= DATETIME(@from)
				AND junit.modified_time < DATETIME(@to)
				AND junit.release = @release
				AND jobs.prowjob_start >= DATETIME(@from)
				AND jobs.prowjob_start < DATETIME(@to)
				AND junit.skipped = FALSE
		)
		SELECT
			test_name,
			job_name,
			suite_name,
			SUM(adjusted_success) AS passes,
			SUM(CASE WHEN adjusted_success = 0 AND is_flake = 0 THEN 1 ELSE 0 END) AS failures,
			SUM(is_flake) AS flakes,
			COUNT(*) AS runs
		FROM deduped
		WHERE row_num = 1
		GROUP BY test_name, job_name, suite_name
	`, l.bqClient.Dataset))
	q.Parameters = []bigquery.QueryParameter{
		{Name: "release", Value: release},
		{Name: "from", Value: from},
		{Name: "to", Value: to},
	}

	it, err := q.Read(l.ctx)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}

	var result []stagingRow
	for {
		var r stagingRow
		err := it.Next(&r)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading row: %w", err)
		}
		result = append(result, r)
	}
	return result, nil
}

type stagingRow struct {
	TestName string `bigquery:"test_name"`
	JobName  string `bigquery:"job_name"`
	Suite    string `bigquery:"suite_name"`
	Passes   int64  `bigquery:"passes"`
	Failures int64  `bigquery:"failures"`
	Flakes   int64  `bigquery:"flakes"`
	Runs     int64  `bigquery:"runs"`
}

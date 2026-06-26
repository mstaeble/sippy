package gateststatus

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	bqcachedclient "github.com/openshift/sippy/pkg/bigquery"
	"github.com/openshift/sippy/pkg/db"
	"github.com/openshift/sippy/pkg/db/models"
)

const (
	gaWindowDays = 30
	batchSize    = 5000
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
	errs     []error
}

func New(ctx context.Context, dbc *db.DB, bqClient *bqcachedclient.Client, force bool) *GATestStatusLoader {
	return &GATestStatusLoader{
		ctx:      ctx,
		dbc:      dbc,
		bqClient: bqClient,
		force:    force,
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
	err := l.dbc.DB.WithContext(l.ctx).
		Where("ga_date < NOW()").
		Find(&defs).Error
	if err != nil {
		return nil, fmt.Errorf("querying release_definitions: %w", err)
	}
	return defs, nil
}

func (l *GATestStatusLoader) loadRelease(rel models.ReleaseDefinition) error {
	rLog := log.WithField("release", rel.Release)
	gaEnd := rel.GADate.UTC().Truncate(24 * time.Hour)
	gaStart := gaEnd.AddDate(0, 0, -gaWindowDays)

	if err := l.ensureRawData(rLog, rel.Release, gaStart, gaEnd); err != nil {
		return err
	}

	if err := l.aggregate(rel.Release, gaEnd); err != nil {
		return fmt.Errorf("aggregating: %w", err)
	}

	return nil
}

// ensureRawData fetches from BigQuery and persists into prow_ga_raw_test_data if the
// raw data is missing or the GA date has changed.
func (l *GATestStatusLoader) ensureRawData(rLog *log.Entry, release string, gaStart, gaEnd time.Time) error {
	if !l.force {
		var existing models.ProwGATestStatus
		err := l.dbc.DB.WithContext(l.ctx).
			Where("release = ? AND ga_date = ?", release, gaEnd).
			First(&existing).Error
		if err == nil {
			rLog.Debug("ga-test-status: data exists with matching GA date, skipping fetch")
			return nil
		}
	}

	rLog.WithField("window", fmt.Sprintf("%s to %s", gaStart.Format("2006-01-02"), gaEnd.Format("2006-01-02"))).
		Info("ga-test-status: fetching from BigQuery")

	rows, errCh := l.streamFromBigQuery(release, gaStart, gaEnd)

	tx := l.dbc.DB.WithContext(l.ctx).Begin()
	if tx.Error != nil {
		return tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	if err := tx.Where("release = ?", release).Delete(&models.ProwGARawTestDatum{}).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("deleting existing raw rows: %w", err)
	}

	rowCount, err := batchInsertRawRows(tx, rows, release)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("inserting raw rows: %w", err)
	}
	if bqErr := <-errCh; bqErr != nil {
		tx.Rollback()
		return fmt.Errorf("streaming from BigQuery: %w", bqErr)
	}
	if rowCount == 0 {
		rLog.Warn("ga-test-status: BigQuery returned zero rows")
	}

	if err := tx.Commit().Error; err != nil {
		return err
	}

	rLog.WithField("bq_rows", rowCount).Info("ga-test-status: raw data persisted")
	return nil
}

// aggregate re-derives prow_ga_test_statuses from prow_ga_raw_test_data by joining with
// current dimension tables. This is cheap (PG-only) and runs every load cycle.
func (l *GATestStatusLoader) aggregate(release string, gaDate time.Time) error {
	tx := l.dbc.DB.WithContext(l.ctx).Begin()
	if tx.Error != nil {
		return tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	if err := tx.Where("release = ?", release).Delete(&models.ProwGATestStatus{}).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("deleting existing aggregated rows: %w", err)
	}

	if err := tx.Exec(`
		INSERT INTO prow_ga_test_statuses (test_id, suite_id, variant_combination_id, release, total_count, success_count, flake_count, ga_date)
		SELECT
			t.id,
			COALESCE(s.id, 0),
			pj.variant_combination_id,
			?,
			SUM(raw.runs)::int,
			SUM(raw.passes + raw.flakes)::int,
			SUM(raw.flakes)::int,
			?
		FROM prow_ga_raw_test_data raw
		JOIN tests t ON t.name = raw.test_name
		JOIN prow_jobs pj ON pj.name = raw.job_name AND pj.deleted_at IS NULL AND pj.variant_combination_id IS NOT NULL
		LEFT JOIN suites s ON s.name = raw.suite_name
		WHERE raw.release = ?
		GROUP BY t.id, COALESCE(s.id, 0), pj.variant_combination_id
	`, release, gaDate, release).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("aggregating from raw data: %w", err)
	}

	return tx.Commit().Error
}

// streamFromBigQuery executes the BQ query and sends rows on a channel.
// A goroutine reads from BigQuery; any terminal error is sent on errCh
// after the rows channel is closed.
func (l *GATestStatusLoader) streamFromBigQuery(release string, from, to time.Time) (<-chan stagingRow, <-chan error) {
	rows := make(chan stagingRow, 2*batchSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(rows)
		defer close(errCh)

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
				WHERE jobs.branch = @release
					AND jobs.prowjob_start >= @from
					AND jobs.prowjob_start < @to
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
			errCh <- fmt.Errorf("executing query: %w", err)
			return
		}

		for {
			var r stagingRow
			err := it.Next(&r)
			if err == iterator.Done {
				return
			}
			if err != nil {
				errCh <- fmt.Errorf("reading row: %w", err)
				return
			}
			rows <- r
		}
	}()

	return rows, errCh
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

func batchInsertRawRows(tx *gorm.DB, rows <-chan stagingRow, release string) (int, error) {
	batch := make([]models.ProwGARawTestDatum, 0, batchSize)
	total := 0
	for r := range rows {
		batch = append(batch, models.ProwGARawTestDatum{
			Release:  release,
			TestName: r.TestName,
			JobName:  r.JobName,
			Suite:    r.Suite,
			Passes:   r.Passes,
			Failures: r.Failures,
			Flakes:   r.Flakes,
			Runs:     r.Runs,
		})
		if len(batch) >= batchSize {
			if err := tx.Create(batch).Error; err != nil {
				return total, fmt.Errorf("inserting batch at row %d: %w", total, err)
			}
			total += len(batch)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := tx.Create(batch).Error; err != nil {
			return total, fmt.Errorf("inserting final batch at row %d: %w", total, err)
		}
		total += len(batch)
	}
	return total, nil
}

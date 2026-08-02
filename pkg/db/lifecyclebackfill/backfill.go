package lifecyclebackfill

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"

	bqcachedclient "github.com/openshift/sippy/pkg/bigquery"
	"github.com/openshift/sippy/pkg/bigquery/bqlabel"
	"github.com/openshift/sippy/pkg/db"
)

type bqRow struct {
	ProwJobBuildID string         `bigquery:"prowjob_build_id"`
	TestName       string         `bigquery:"test_name"`
	Release        string         `bigquery:"release"`
	ProwJobStart   civil.DateTime `bigquery:"prowjob_start"`
}

type pgRow struct {
	ProwJobRunID        int64
	TestName            string
	ProwJobRunTimestamp time.Time
}

var tempCols = []db.TempColumn[pgRow]{
	{Name: "prow_job_run_id", Type: "bigint NOT NULL", Value: func(r *pgRow) any { return r.ProwJobRunID }},
	{Name: "test_name", Type: "text NOT NULL", Value: func(r *pgRow) any { return r.TestName }},
	{Name: "prow_job_run_timestamp", Type: "timestamptz NOT NULL", Value: func(r *pgRow) any { return r.ProwJobRunTimestamp }},
}

func Backfill(ctx context.Context, dbc *db.DB, bqClient *bqcachedclient.Client, startDate, endDate civil.Date, dryRun bool) error {
	totalUpdated := int64(0)
	overallStart := time.Now()

	for date := startDate; !date.After(endDate); date = date.AddDays(1) {
		updated, err := processDay(ctx, dbc, bqClient, date, dryRun)
		if err != nil {
			return fmt.Errorf("processing %s: %w", date, err)
		}
		totalUpdated += updated
	}

	log.WithFields(log.Fields{
		"total_updated": totalUpdated,
		"elapsed":       time.Since(overallStart),
		"dry_run":       dryRun,
	}).Info("lifecycle backfill complete")

	return nil
}

func processDay(ctx context.Context, dbc *db.DB, bqClient *bqcachedclient.Client, date civil.Date, dryRun bool) (int64, error) {
	dayLog := log.WithField("date", date)

	rowsByRelease, err := fetchInformingRuns(ctx, bqClient, date)
	if err != nil {
		return 0, fmt.Errorf("querying BigQuery: %w", err)
	}

	totalBQRows := 0
	for _, rows := range rowsByRelease {
		totalBQRows += len(rows)
	}

	if totalBQRows == 0 {
		return 0, nil
	}

	dayLog.WithFields(log.Fields{
		"bq_rows":  totalBQRows,
		"releases": len(rowsByRelease),
	}).Info("fetched informing runs from BigQuery")

	if dryRun {
		dayLog.Info("dry run: skipping update")
		return int64(totalBQRows), nil
	}

	totalUpdated := int64(0)
	for release, rows := range rowsByRelease {
		updated, err := applyUpdates(ctx, dbc, release, rows)
		if err != nil {
			return 0, fmt.Errorf("applying updates for release %s: %w", release, err)
		}
		dayLog.WithFields(log.Fields{
			"release": release,
			"updated": updated,
		}).Info("updated prow_job_run_tests")
		totalUpdated += updated
	}

	return totalUpdated, nil
}

func fetchInformingRuns(ctx context.Context, bqClient *bqcachedclient.Client, date civil.Date) (map[string][]pgRow, error) {
	nextDate := date.AddDays(1)

	query := bqClient.Query(ctx, bqlabel.LifecycleBackfill, fmt.Sprintf(`
		SELECT DISTINCT junit.prowjob_build_id, junit.test_name, junit.release, jobs.prowjob_start
		FROM %[1]s.junit
		JOIN %[1]s.jobs ON junit.prowjob_build_id = jobs.prowjob_build_id
		WHERE junit.modified_time >= DATETIME(@start_date)
		  AND junit.modified_time < DATETIME(@end_date)
		  AND junit.release IS NOT NULL
		  AND jobs.prowjob_start IS NOT NULL
		  AND COALESCE(NULLIF(junit.lifecycle, ''), 'blocking') = 'informing'
		  AND junit.skipped = false
	`, bqClient.Dataset))
	query.Parameters = []bigquery.QueryParameter{
		{Name: "start_date", Value: civil.DateTime{Date: date}},
		{Name: "end_date", Value: civil.DateTime{Date: nextDate}},
	}

	iter, err := query.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}

	result := make(map[string][]pgRow)
	for {
		var row bqRow
		err := iter.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading row: %w", err)
		}
		id, err := strconv.ParseInt(row.ProwJobBuildID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing prowjob_build_id %q: %w", row.ProwJobBuildID, err)
		}
		result[row.Release] = append(result[row.Release], pgRow{
			ProwJobRunID:        id,
			TestName:            row.TestName,
			ProwJobRunTimestamp: row.ProwJobStart.In(time.UTC),
		})
	}
	return result, nil
}

func timestampBounds(rows []pgRow) (min, max time.Time) {
	min = rows[0].ProwJobRunTimestamp
	max = rows[0].ProwJobRunTimestamp
	for _, r := range rows[1:] {
		if r.ProwJobRunTimestamp.Before(min) {
			min = r.ProwJobRunTimestamp
		}
		if r.ProwJobRunTimestamp.After(max) {
			max = r.ProwJobRunTimestamp
		}
	}
	return min, max
}

func applyUpdates(ctx context.Context, dbc *db.DB, release string, rows []pgRow) (int64, error) {
	sqlDB, err := dbc.DB.DB()
	if err != nil {
		return 0, fmt.Errorf("getting sql.DB: %w", err)
	}
	conn, err := stdlib.AcquireConn(sqlDB)
	if err != nil {
		return 0, fmt.Errorf("acquiring pgx conn: %w", err)
	}
	defer func() {
		if releaseErr := stdlib.ReleaseConn(sqlDB, conn); releaseErr != nil {
			log.WithError(releaseErr).Error("failed to release pgx conn")
		}
	}()

	cleanup, err := db.CopyToTempTable(ctx, conn, "tmp_lifecycle_backfill", rows, tempCols)
	if err != nil {
		return 0, fmt.Errorf("copying to temp table: %w", err)
	}
	defer cleanup()

	minTS, maxTS := timestampBounds(rows)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && rollbackErr != pgx.ErrTxClosed {
			log.WithError(rollbackErr).Error("failed to rollback transaction")
		}
	}()

	result, err := tx.Exec(ctx, `
		UPDATE prow_job_run_tests pjrt
		SET lifecycle = 'informing', updated_at = NOW()
		FROM tmp_lifecycle_backfill tmp
		JOIN tests t ON t.name = tmp.test_name AND t.deleted_at IS NULL
		WHERE pjrt.prow_job_run_id = tmp.prow_job_run_id
		  AND pjrt.test_id = t.id
		  AND pjrt.prow_job_run_release = $1
		  AND pjrt.prow_job_run_timestamp >= $2
		  AND pjrt.prow_job_run_timestamp <= $3
		  AND pjrt.prow_job_run_timestamp = tmp.prow_job_run_timestamp
		  AND pjrt.lifecycle != 'informing'
	`, release, minTS, maxTS)
	if err != nil {
		return 0, fmt.Errorf("updating prow_job_run_tests: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("committing transaction: %w", err)
	}

	log.WithFields(log.Fields{
		"release": release,
		"min_ts":  minTS,
		"max_ts":  maxTS,
	}).Debug("timestamp bounds for partition pruning")

	return result.RowsAffected(), nil
}

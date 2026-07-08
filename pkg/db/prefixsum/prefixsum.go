package prefixsum

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/openshift/sippy/pkg/db"
)

const retentionDays = 95

type summaryStore interface {
	MaxSummaryDate() (*time.Time, error)
	MinDailySummaryDate() (*time.Time, error)
	MaxDailySummaryDate() (*time.Time, error)
	UpdateDay(date time.Time) error
	DeleteOldRows(cutoff time.Time) (int64, error)
}

// Options configures the prefix sum refresh.
type Options struct {
	Rebuild bool
}

// Refresh computes prefix sums from test_daily_summaries. On first run
// (empty table), it backfills day-by-day from the earliest daily summary.
// On subsequent runs, it incrementally adds prefix sums for new dates.
// The same SQL is used for both paths.
func Refresh(dbc *db.DB, opts Options) error {
	return refreshPrefixSums(&pgStore{dbc: dbc}, opts)
}

func refreshPrefixSums(store summaryStore, opts Options) error {
	loadStart := time.Now()
	log.Info("refreshing prefix sums")

	if opts.Rebuild {
		log.Info("rebuild not supported for prefix sums, skipping")
		return nil
	}

	maxPrefixDate, err := store.MaxSummaryDate()
	if err != nil {
		return fmt.Errorf("checking max prefix sum date: %w", err)
	}

	var startDate time.Time
	if maxPrefixDate == nil {
		minDaily, err := store.MinDailySummaryDate()
		if err != nil {
			return fmt.Errorf("checking min daily summary date: %w", err)
		}
		if minDaily == nil {
			log.Info("no daily summaries found, nothing to backfill")
			return nil
		}
		startDate = *minDaily
		log.WithField("start", startDate.Format("2006-01-02")).Info("backfilling prefix sums from earliest daily summary")
	} else {
		startDate = maxPrefixDate.AddDate(0, 0, 1)
	}

	maxDaily, err := store.MaxDailySummaryDate()
	if err != nil {
		return fmt.Errorf("checking max daily summary date: %w", err)
	}
	if maxDaily == nil {
		log.Info("no daily summaries found, nothing to update")
		return nil
	}
	endDate := *maxDaily

	if startDate.After(endDate) {
		log.Info("prefix sums already up to date")
	} else {
		days := int(endDate.Sub(startDate).Hours()/24) + 1
		log.WithFields(log.Fields{
			"start": startDate.Format("2006-01-02"),
			"end":   endDate.Format("2006-01-02"),
			"days":  days,
		}).Info("updating prefix sums")

		for date := startDate; !date.After(endDate); date = date.AddDate(0, 0, 1) {
			if err := store.UpdateDay(date); err != nil {
				return fmt.Errorf("updating prefix sums for %s: %w", date.Format("2006-01-02"), err)
			}
			log.WithField("date", date.Format("2006-01-02")).Debug("updated prefix sums for date")
		}
	}

	now := time.Now()
	cutoff := now.AddDate(0, 0, -retentionDays)
	deleted, err := store.DeleteOldRows(cutoff)
	if err != nil {
		return fmt.Errorf("cleaning up old rows: %w", err)
	}
	if deleted > 0 {
		log.WithField("deleted", deleted).Info("cleaned up old prefix sum rows")
	}

	log.WithField("elapsed", time.Since(loadStart)).Info("prefix sum refresh complete")
	return nil
}

// pgStore implements summaryStore against PostgreSQL.
type pgStore struct {
	dbc *db.DB
}

func (s *pgStore) MaxSummaryDate() (*time.Time, error) {
	var maxDate *time.Time
	err := s.dbc.DB.Table("prefix_sums").
		Select("MAX(summary_date)").Row().Scan(&maxDate)
	return maxDate, err
}

func (s *pgStore) MinDailySummaryDate() (*time.Time, error) {
	var minDate *time.Time
	err := s.dbc.DB.Table("test_daily_summaries").
		Select("MIN(summary_date)").Row().Scan(&minDate)
	return minDate, err
}

func (s *pgStore) MaxDailySummaryDate() (*time.Time, error) {
	var maxDate *time.Time
	err := s.dbc.DB.Table("test_daily_summaries").
		Select("MAX(summary_date)").Row().Scan(&maxDate)
	return maxDate, err
}

func (s *pgStore) UpdateDay(date time.Time) error {
	return s.dbc.DB.Exec(`
		INSERT INTO prefix_sums (summary_date, test_id, prow_job_id, suite_id, release,
		                         cum_successes, cum_failures, cum_flakes, cum_runs)
		SELECT
			tds.summary_date,
			tds.test_id,
			tds.prow_job_id,
			tds.suite_id,
			tds.release,
			COALESCE(prev.cum_successes, 0) + tds.successes,
			COALESCE(prev.cum_failures, 0) + tds.failures,
			COALESCE(prev.cum_flakes, 0) + tds.flakes,
			COALESCE(prev.cum_runs, 0) + tds.runs
		FROM test_daily_summaries tds
		LEFT JOIN prefix_sums prev
			ON tds.test_id = prev.test_id
			AND tds.prow_job_id = prev.prow_job_id
			AND tds.suite_id = prev.suite_id
			AND tds.release = prev.release
			AND prev.summary_date = tds.summary_date - 1
		WHERE tds.summary_date = ?
		ON CONFLICT (release, summary_date, test_id, prow_job_id, suite_id)
		DO UPDATE SET
			cum_successes = EXCLUDED.cum_successes,
			cum_failures = EXCLUDED.cum_failures,
			cum_flakes = EXCLUDED.cum_flakes,
			cum_runs = EXCLUDED.cum_runs
		WHERE (prefix_sums.cum_successes, prefix_sums.cum_failures,
		       prefix_sums.cum_flakes, prefix_sums.cum_runs)
		   IS DISTINCT FROM
		      (EXCLUDED.cum_successes, EXCLUDED.cum_failures,
		       EXCLUDED.cum_flakes, EXCLUDED.cum_runs)
	`, date).Error
}

func (s *pgStore) DeleteOldRows(cutoff time.Time) (int64, error) {
	result := s.dbc.DB.Exec("DELETE FROM prefix_sums WHERE summary_date < ?", cutoff)
	return result.RowsAffected, result.Error
}

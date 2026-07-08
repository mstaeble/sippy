package crdailysummary

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/openshift/sippy/pkg/db"
)

const (
	defaultLookbackDays    = 95
	retentionDays          = 95
	parallelWorkers        = 4
	scopedRebuildThreshold = 0.20
)

var valueColumns = []string{"successes", "failures", "flakes", "runs"}

var (
	insertSQL        = buildInsertSQL()
	onConflictClause = buildOnConflictClause()
)

func buildInsertSQL() string {
	return fmt.Sprintf(`
		INSERT INTO cr_daily_summaries (test_id, suite_id, variant_combination_id, release, summary_date, %s)
		SELECT
			tds.test_id,
			tds.suite_id,
			pj.variant_combination_id,
			tds.release,
			tds.summary_date,
			SUM(tds.successes),
			SUM(tds.failures),
			SUM(tds.flakes),
			SUM(tds.runs)
		FROM test_daily_summaries tds
		JOIN prow_jobs pj ON tds.prow_job_id = pj.id
		WHERE tds.summary_date >= ?::date
		  AND tds.summary_date < (?::date + INTERVAL '1 day')
		  AND tds.release = ?
		  AND pj.variant_combination_id IS NOT NULL
		GROUP BY tds.test_id, tds.suite_id, pj.variant_combination_id, tds.release, tds.summary_date`,
		strings.Join(valueColumns, ", "))
}

func buildOnConflictClause() string {
	var setClauses, oldCols, newCols []string
	for _, col := range valueColumns {
		setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		oldCols = append(oldCols, "cr_daily_summaries."+col)
		newCols = append(newCols, "EXCLUDED."+col)
	}

	return fmt.Sprintf(`
		ON CONFLICT (release, summary_date, test_id, suite_id, variant_combination_id)
		DO UPDATE SET %s
		WHERE (%s) IS DISTINCT FROM (%s)`,
		strings.Join(setClauses, ", "),
		strings.Join(oldCols, ", "),
		strings.Join(newCols, ", "))
}

type summaryStore interface {
	MaxSummaryDate() (*time.Time, error)
	Truncate() error
	Releases() ([]string, error)
	AggregateRangeForRelease(start, end time.Time, release string, skipConflictDetection bool) error
	DetectVariantChanges() ([]uint, error)
	ScopedRebuild(changedProwJobIDs []uint) error
	TotalProwJobCount() (int64, error)
	UpdateVCIDMapping() error
	VCIDMappingPopulated() (bool, error)
	DeleteOldRows(cutoff time.Time) (int64, error)
}

// Options configures the CR daily summary refresh.
type Options struct {
	Rebuild       bool
	StartOverride *time.Time
	EndOverride   *time.Time
}

// Refresh aggregates test_daily_summaries by variant_combination_id into
// the cr_daily_summaries table. It runs after daily summary refresh and
// before matview refresh so the CR matviews read from pre-aggregated data.
func Refresh(dbc *db.DB, opts Options) error {
	return refreshSummaries(&pgStore{dbc: dbc}, opts)
}

func refreshSummaries(store summaryStore, opts Options) error {
	loadStart := time.Now()
	log.Info("refreshing CR daily summaries")

	now := time.Now()

	startDate, endDate, err := dateRange(store, opts, now)
	if err != nil {
		return err
	}

	if opts.Rebuild {
		log.Info("rebuild requested, truncating cr_daily_summaries")
		if err := store.Truncate(); err != nil {
			return fmt.Errorf("truncating table: %w", err)
		}
	} else {
		populated, err := store.VCIDMappingPopulated()
		if err != nil {
			return fmt.Errorf("checking VCID mapping: %w", err)
		}
		if populated {
			changes, err := store.DetectVariantChanges()
			if err != nil {
				return fmt.Errorf("detecting variant changes: %w", err)
			}
			if len(changes) > 0 {
				total, err := store.TotalProwJobCount()
				if err != nil {
					return fmt.Errorf("counting prow jobs: %w", err)
				}
				if total > 0 && float64(len(changes))/float64(total) > scopedRebuildThreshold {
					log.WithFields(log.Fields{
						"changed": len(changes),
						"total":   total,
					}).Info("variant changes exceed threshold, truncating for full rebuild")
					if err := store.Truncate(); err != nil {
						return fmt.Errorf("truncating table: %w", err)
					}
				} else {
					log.WithField("count", len(changes)).Info("variant changes detected, doing scoped rebuild")
					if err := store.ScopedRebuild(changes); err != nil {
						return fmt.Errorf("scoped rebuild for variant changes: %w", err)
					}
				}
			}
		} else {
			log.Info("VCID mapping empty (first run), skipping variant detection")
		}
	}

	releases, err := store.Releases()
	if err != nil {
		return fmt.Errorf("querying releases: %w", err)
	}

	skipConflictDetection := opts.Rebuild
	if !skipConflictDetection {
		maxDate, err := store.MaxSummaryDate()
		if err != nil {
			return fmt.Errorf("checking if table is empty: %w", err)
		}
		skipConflictDetection = maxDate == nil
	}

	log.WithFields(log.Fields{
		"start": startDate.Format("2006-01-02"),
		"end":   endDate.Format("2006-01-02"),
	}).Info("aggregating CR daily summaries")

	if err := aggregateReleases(store, releases, startDate, endDate, skipConflictDetection); err != nil {
		return err
	}

	if err := store.UpdateVCIDMapping(); err != nil {
		return fmt.Errorf("updating VCID mapping: %w", err)
	}

	cutoff := now.AddDate(0, 0, -retentionDays)
	deleted, err := store.DeleteOldRows(cutoff)
	if err != nil {
		return fmt.Errorf("cleaning up old rows: %w", err)
	}
	if deleted > 0 {
		log.WithField("deleted", deleted).Info("cleaned up old CR daily summary rows")
	}

	log.WithField("elapsed", time.Since(loadStart)).Info("CR daily summary refresh complete")
	return nil
}

func aggregateReleases(store summaryStore, releases []string, startDate, endDate time.Time, skipConflictDetection bool) error {
	errs := make(chan error, len(releases))
	work := make(chan string, len(releases))

	var wg sync.WaitGroup
	for range parallelWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for release := range work {
				if err := store.AggregateRangeForRelease(startDate, endDate, release, skipConflictDetection); err != nil {
					errs <- fmt.Errorf("aggregating release %s: %w", release, err)
					continue
				}
				log.WithField("release", release).Debug("aggregated CR daily summary for release")
			}
		}()
	}

	for _, release := range releases {
		work <- release
	}
	close(work)
	wg.Wait()
	close(errs)

	var combined []error
	for err := range errs {
		combined = append(combined, err)
	}
	return errors.Join(combined...)
}

func dateRange(store summaryStore, opts Options, now time.Time) (time.Time, time.Time, error) {
	if opts.StartOverride != nil && opts.EndOverride != nil {
		return *opts.StartOverride, *opts.EndOverride, nil
	}

	endDate := now
	if opts.EndOverride != nil {
		endDate = *opts.EndOverride
	}

	if opts.StartOverride != nil {
		return *opts.StartOverride, endDate, nil
	}

	if opts.Rebuild {
		return now.AddDate(0, 0, -defaultLookbackDays), endDate, nil
	}

	startDate, err := resolveStartDate(store, now)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("querying max summary date: %w", err)
	}

	return startDate, endDate, nil
}

func resolveStartDate(store summaryStore, now time.Time) (time.Time, error) {
	yesterday := now.AddDate(0, 0, -1)

	maxSummary, err := store.MaxSummaryDate()
	if err != nil {
		return time.Time{}, err
	}
	if maxSummary != nil {
		if maxSummary.Before(yesterday) {
			return *maxSummary, nil
		}
		return yesterday, nil
	}

	return now.AddDate(0, 0, -defaultLookbackDays), nil
}

// pgStore implements summaryStore against PostgreSQL.
type pgStore struct {
	dbc *db.DB
}

func (s *pgStore) MaxSummaryDate() (*time.Time, error) {
	var maxDate *time.Time
	err := s.dbc.DB.Table("cr_daily_summaries").
		Select("MAX(summary_date)").Row().Scan(&maxDate)
	return maxDate, err
}

func (s *pgStore) Truncate() error {
	if err := s.dbc.DB.Exec("TRUNCATE cr_daily_summaries").Error; err != nil {
		return err
	}
	return s.dbc.DB.Exec("TRUNCATE cr_vcid_mappings").Error
}

func (s *pgStore) Releases() ([]string, error) {
	var releases []string
	err := s.dbc.DB.Table("prow_jobs").Distinct("release").Pluck("release", &releases).Error
	return releases, err
}

func (s *pgStore) AggregateRangeForRelease(startDate, endDate time.Time, release string, skipConflictDetection bool) error {
	sql := insertSQL
	if !skipConflictDetection {
		sql += onConflictClause
	}
	return s.dbc.DB.Exec(sql, startDate, endDate, release).Error
}

func (s *pgStore) VCIDMappingPopulated() (bool, error) {
	var count int64
	err := s.dbc.DB.Table("cr_vcid_mappings").Count(&count).Error
	return count > 0, err
}

func (s *pgStore) TotalProwJobCount() (int64, error) {
	var count int64
	err := s.dbc.DB.Table("prow_jobs").Where("variant_combination_id IS NOT NULL").Count(&count).Error
	return count, err
}

func (s *pgStore) DetectVariantChanges() ([]uint, error) {
	var changedIDs []uint
	err := s.dbc.DB.Raw(`
		SELECT pj.id
		FROM prow_jobs pj
		LEFT JOIN cr_vcid_mappings m ON pj.id = m.prow_job_id
		WHERE pj.variant_combination_id IS NOT NULL
		  AND (m.prow_job_id IS NULL
		       OR m.variant_combination_id IS DISTINCT FROM pj.variant_combination_id)
	`).Pluck("id", &changedIDs).Error
	return changedIDs, err
}

func (s *pgStore) ScopedRebuild(changedProwJobIDs []uint) error {
	return s.dbc.DB.Exec(`
		WITH affected_tuples AS (
			SELECT DISTINCT tds.test_id, tds.suite_id, tds.release, tds.summary_date
			FROM test_daily_summaries tds
			WHERE tds.prow_job_id IN (?)
		),
		deleted AS (
			DELETE FROM cr_daily_summaries cds
			USING affected_tuples at
			WHERE cds.test_id = at.test_id
			  AND cds.suite_id = at.suite_id
			  AND cds.release = at.release
			  AND cds.summary_date = at.summary_date
		)
		INSERT INTO cr_daily_summaries (test_id, suite_id, variant_combination_id, release, summary_date,
		                                successes, failures, flakes, runs)
		SELECT
			tds.test_id, tds.suite_id, pj.variant_combination_id, tds.release, tds.summary_date,
			SUM(tds.successes), SUM(tds.failures), SUM(tds.flakes), SUM(tds.runs)
		FROM test_daily_summaries tds
		JOIN prow_jobs pj ON tds.prow_job_id = pj.id
		JOIN affected_tuples at ON tds.test_id = at.test_id
			AND tds.suite_id = at.suite_id
			AND tds.release = at.release
			AND tds.summary_date = at.summary_date
		WHERE pj.variant_combination_id IS NOT NULL
		GROUP BY tds.test_id, tds.suite_id, pj.variant_combination_id, tds.release, tds.summary_date
		ON CONFLICT (release, summary_date, test_id, suite_id, variant_combination_id)
		DO UPDATE SET
			successes = EXCLUDED.successes,
			failures  = EXCLUDED.failures,
			flakes    = EXCLUDED.flakes,
			runs      = EXCLUDED.runs
	`, changedProwJobIDs).Error
}

func (s *pgStore) UpdateVCIDMapping() error {
	if err := s.dbc.DB.Exec("TRUNCATE cr_vcid_mappings").Error; err != nil {
		return err
	}
	return s.dbc.DB.Exec(`
		INSERT INTO cr_vcid_mappings (prow_job_id, variant_combination_id)
		SELECT id, variant_combination_id FROM prow_jobs
		WHERE variant_combination_id IS NOT NULL
	`).Error
}

func (s *pgStore) DeleteOldRows(cutoff time.Time) (int64, error) {
	result := s.dbc.DB.Exec("DELETE FROM cr_daily_summaries WHERE summary_date < ?", cutoff)
	return result.RowsAffected, result.Error
}

package prefixsum

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/openshift/sippy/pkg/db"
)

type variantStore interface {
	MaxVariantPrefixDate() (*time.Time, error)
	MaxPrefixSumDate() (*time.Time, error)
	UpdateVariantDay(date time.Time) error
	DetectVariantChanges() ([]uint, error)
	ScopedRebuildVariants(changedProwJobIDs []uint) error
	TotalProwJobCount() (int64, error)
	UpdateVCIDMapping() error
	VCIDMappingPopulated() (bool, error)
	DeleteOldVariantRows(cutoff time.Time) (int64, error)
}

// RefreshVariantPrefixSums groups prefix_sums by variant_combination_id
// into the cr_variant_prefix_sums table. Each day's update groups one
// day's prefix_sums and inserts. Variant changes are detected via
// cr_vcid_mappings and trigger scoped rebuilds.
func RefreshVariantPrefixSums(dbc *db.DB, opts Options) error {
	return refreshVariantPrefixSums(&pgVariantStore{dbc: dbc}, opts)
}

func refreshVariantPrefixSums(store variantStore, opts Options) error {
	loadStart := time.Now()
	log.Info("refreshing variant prefix sums")

	if !opts.Rebuild {
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
				if total > 0 && float64(len(changes))/float64(total) > 0.20 {
					log.WithFields(log.Fields{
						"changed": len(changes),
						"total":   total,
					}).Warn("variant changes exceed 20% threshold, full variant prefix sum rebuild needed")
				} else {
					log.WithField("count", len(changes)).Info("variant changes detected, doing scoped rebuild")
					if err := store.ScopedRebuildVariants(changes); err != nil {
						return fmt.Errorf("scoped rebuild for variant changes: %w", err)
					}
				}
			}
		} else {
			log.Info("VCID mapping empty (first run), skipping variant detection")
		}
	}

	maxVariant, err := store.MaxVariantPrefixDate()
	if err != nil {
		return fmt.Errorf("checking max variant prefix sum date: %w", err)
	}

	maxPrefix, err := store.MaxPrefixSumDate()
	if err != nil {
		return fmt.Errorf("checking max prefix sum date: %w", err)
	}
	if maxPrefix == nil {
		log.Info("no prefix sums found, nothing to group")
		return nil
	}

	var startDate time.Time
	if maxVariant == nil {
		startDate = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		log.Info("backfilling variant prefix sums from prefix_sums")
	} else {
		startDate = maxVariant.AddDate(0, 0, 1)
	}

	endDate := *maxPrefix

	if startDate.After(endDate) {
		log.Info("variant prefix sums already up to date")
	} else {
		days := int(endDate.Sub(startDate).Hours()/24) + 1
		log.WithFields(log.Fields{
			"start": startDate.Format("2006-01-02"),
			"end":   endDate.Format("2006-01-02"),
			"days":  days,
		}).Info("updating variant prefix sums")

		for date := startDate; !date.After(endDate); date = date.AddDate(0, 0, 1) {
			dayStart := time.Now()
			if err := store.UpdateVariantDay(date); err != nil {
				return fmt.Errorf("updating variant prefix sums for %s: %w", date.Format("2006-01-02"), err)
			}
			log.WithFields(log.Fields{
				"date":    date.Format("2006-01-02"),
				"elapsed": time.Since(dayStart),
			}).Debug("updated variant prefix sums for date")
		}
	}

	if err := store.UpdateVCIDMapping(); err != nil {
		return fmt.Errorf("updating VCID mapping: %w", err)
	}

	now := time.Now()
	cutoff := now.AddDate(0, 0, -retentionDays)
	deleted, err := store.DeleteOldVariantRows(cutoff)
	if err != nil {
		return fmt.Errorf("cleaning up old variant prefix sum rows: %w", err)
	}
	if deleted > 0 {
		log.WithField("deleted", deleted).Info("cleaned up old variant prefix sum rows")
	}

	log.WithField("elapsed", time.Since(loadStart)).Info("variant prefix sum refresh complete")
	return nil
}

// pgVariantStore implements variantStore against PostgreSQL.
type pgVariantStore struct {
	dbc *db.DB
}

func (s *pgVariantStore) MaxVariantPrefixDate() (*time.Time, error) {
	var maxDate *time.Time
	err := s.dbc.DB.Table("cr_variant_prefix_sums").
		Select("MAX(summary_date)").Row().Scan(&maxDate)
	return maxDate, err
}

func (s *pgVariantStore) MaxPrefixSumDate() (*time.Time, error) {
	var maxDate *time.Time
	err := s.dbc.DB.Table("prefix_sums").
		Select("MAX(summary_date)").Row().Scan(&maxDate)
	return maxDate, err
}

func (s *pgVariantStore) UpdateVariantDay(date time.Time) error {
	return s.dbc.DB.Exec(`
		INSERT INTO cr_variant_prefix_sums (summary_date, test_id, suite_id, variant_combination_id, release,
		                                    cum_successes, cum_failures, cum_flakes, cum_runs)
		SELECT ps.summary_date, ps.test_id, ps.suite_id, pj.variant_combination_id, ps.release,
			SUM(ps.cum_successes)::bigint,
			SUM(ps.cum_failures)::bigint,
			SUM(ps.cum_flakes)::bigint,
			SUM(ps.cum_runs)::bigint
		FROM prefix_sums ps
		JOIN prow_jobs pj ON ps.prow_job_id = pj.id
		WHERE ps.summary_date = ?
		  AND pj.variant_combination_id IS NOT NULL
		GROUP BY ps.summary_date, ps.test_id, ps.suite_id, pj.variant_combination_id, ps.release
		ON CONFLICT (release, summary_date, test_id, suite_id, variant_combination_id)
		DO UPDATE SET
			cum_successes = EXCLUDED.cum_successes,
			cum_failures = EXCLUDED.cum_failures,
			cum_flakes = EXCLUDED.cum_flakes,
			cum_runs = EXCLUDED.cum_runs
		WHERE (cr_variant_prefix_sums.cum_successes, cr_variant_prefix_sums.cum_failures,
		       cr_variant_prefix_sums.cum_flakes, cr_variant_prefix_sums.cum_runs)
		   IS DISTINCT FROM
		      (EXCLUDED.cum_successes, EXCLUDED.cum_failures,
		       EXCLUDED.cum_flakes, EXCLUDED.cum_runs)
	`, date).Error
}

func (s *pgVariantStore) VCIDMappingPopulated() (bool, error) {
	var count int64
	err := s.dbc.DB.Table("cr_vcid_mappings").Count(&count).Error
	return count > 0, err
}

func (s *pgVariantStore) DetectVariantChanges() ([]uint, error) {
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

func (s *pgVariantStore) ScopedRebuildVariants(changedProwJobIDs []uint) error {
	return s.dbc.DB.Exec(`
		WITH affected_entities AS (
			SELECT DISTINCT ps.test_id, ps.suite_id, ps.release
			FROM prefix_sums ps
			WHERE ps.prow_job_id IN (?)
		),
		deleted AS (
			DELETE FROM cr_variant_prefix_sums cvps
			USING affected_entities ae
			WHERE cvps.test_id = ae.test_id
			  AND cvps.suite_id = ae.suite_id
			  AND cvps.release = ae.release
		)
		INSERT INTO cr_variant_prefix_sums (summary_date, test_id, suite_id, variant_combination_id, release,
		                                    cum_successes, cum_failures, cum_flakes, cum_runs)
		SELECT ps.summary_date, ps.test_id, ps.suite_id, pj.variant_combination_id, ps.release,
			SUM(ps.cum_successes)::bigint, SUM(ps.cum_failures)::bigint,
			SUM(ps.cum_flakes)::bigint, SUM(ps.cum_runs)::bigint
		FROM prefix_sums ps
		JOIN prow_jobs pj ON ps.prow_job_id = pj.id
		JOIN affected_entities ae ON ps.test_id = ae.test_id
			AND ps.suite_id = ae.suite_id AND ps.release = ae.release
		WHERE pj.variant_combination_id IS NOT NULL
		GROUP BY ps.summary_date, ps.test_id, ps.suite_id, pj.variant_combination_id, ps.release
		ON CONFLICT (release, summary_date, test_id, suite_id, variant_combination_id)
		DO UPDATE SET
			cum_successes = EXCLUDED.cum_successes,
			cum_failures = EXCLUDED.cum_failures,
			cum_flakes = EXCLUDED.cum_flakes,
			cum_runs = EXCLUDED.cum_runs
	`, changedProwJobIDs).Error
}

func (s *pgVariantStore) TotalProwJobCount() (int64, error) {
	var count int64
	err := s.dbc.DB.Table("prow_jobs").Where("variant_combination_id IS NOT NULL").Count(&count).Error
	return count, err
}

func (s *pgVariantStore) UpdateVCIDMapping() error {
	if err := s.dbc.DB.Exec("TRUNCATE cr_vcid_mappings").Error; err != nil {
		return err
	}
	return s.dbc.DB.Exec(`
		INSERT INTO cr_vcid_mappings (prow_job_id, variant_combination_id)
		SELECT id, variant_combination_id FROM prow_jobs
		WHERE variant_combination_id IS NOT NULL
	`).Error
}

func (s *pgVariantStore) DeleteOldVariantRows(cutoff time.Time) (int64, error) {
	result := s.dbc.DB.Exec("DELETE FROM cr_variant_prefix_sums WHERE summary_date < ?", cutoff)
	return result.RowsAffected, result.Error
}

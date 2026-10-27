package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/openshift/sippy/pkg/apis/api/componentreport/reqopts"
	"gorm.io/gorm"
)

// buildSamplePullRequestAndPayloadRunFilter builds the pull request and payload
// tag predicates shared by sample reports and test details. The time bounds
// constrain the partitioned pull request run lookup. EXISTS avoids multiplying
// test results when a run has multiple associations. The caller must provide
// the prow_job_runs alias pjr.
func buildSamplePullRequestAndPayloadRunFilter(release reqopts.Release, start, end time.Time) (string, []any, error) {
	var clauses []string
	var args []any

	if pr := release.PullRequestOptions; pr != nil {
		number, err := strconv.Atoi(pr.PRNumber)
		if err != nil {
			return "", nil, fmt.Errorf("invalid sample pull request number %q: %w", pr.PRNumber, err)
		}
		clauses = append(clauses, `EXISTS (
			SELECT 1
			FROM prow_job_run_prow_pull_requests pr_run
			JOIN prow_pull_requests pr ON pr.id = pr_run.prow_pull_request_id
			WHERE pr_run.prow_job_run_id = pjr.id
				AND pr_run.prow_job_run_release = pjr.prow_job_release
				AND pr_run.prow_job_run_timestamp = pjr.timestamp
				AND pr.org = ? AND pr.repo = ? AND pr.number = ?
				AND pr_run.prow_job_run_timestamp >= ? AND pr_run.prow_job_run_timestamp < ?
				AND pr.deleted_at IS NULL
		)`)
		args = append(args, pr.Org, pr.Repo, number, start, end)
	}

	if payload := release.PayloadOptions; payload != nil {
		clauses = append(clauses, `EXISTS (
			SELECT 1
			FROM release_job_runs payload_run
			JOIN release_tags rt ON rt.id = payload_run.release_tag_id
			WHERE payload_run.prow_job_run_id = pjr.id
				AND rt.release_tag = ANY(?)
				AND payload_run.deleted_at IS NULL AND rt.deleted_at IS NULL
		)`)
		args = append(args, pq.Array(payload.Tags))
	}

	return strings.Join(clauses, " AND "), args, nil
}

// beginSamplePullRequestReleaseSnapshot opens a repeatable-read transaction and
// finds every release with a run associated with the selected pull request in
// the requested sample window. The caller uses the same transaction for the
// main sample query so the release set and matching rows share a snapshot.
func beginSamplePullRequestReleaseSnapshot(
	ctx context.Context,
	db *gorm.DB,
	pr *reqopts.PullRequest,
	start, end time.Time,
) (*gorm.DB, []string, error) {
	number, err := strconv.Atoi(pr.PRNumber)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid sample pull request number %q: %w", pr.PRNumber, err)
	}

	tx := db.WithContext(ctx).Begin(&sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if tx.Error != nil {
		return nil, nil, fmt.Errorf("starting pull request release lookup transaction: %w", tx.Error)
	}

	type pullRequestReleaseRow struct {
		Release string `gorm:"column:prow_job_run_release"`
	}
	var releaseRows []pullRequestReleaseRow
	err = tx.Raw(`
		SELECT DISTINCT pr_run.prow_job_run_release
		FROM prow_job_run_prow_pull_requests pr_run
		JOIN prow_pull_requests pr ON pr.id = pr_run.prow_pull_request_id
		WHERE pr.org = ? AND pr.repo = ? AND pr.number = ?
			AND pr_run.prow_job_run_timestamp >= ? AND pr_run.prow_job_run_timestamp < ?
			AND pr.deleted_at IS NULL`,
		pr.Org, pr.Repo, number, start, end).Scan(&releaseRows).Error
	if err != nil {
		if rollbackErr := tx.Rollback().Error; rollbackErr != nil {
			err = fmt.Errorf("%w (rolling back release lookup: %v)", err, rollbackErr)
		}
		return nil, nil, fmt.Errorf("finding releases for sample pull request: %w", err)
	}
	releases := make([]string, 0, len(releaseRows))
	for _, row := range releaseRows {
		releases = append(releases, row.Release)
	}

	return tx, releases, nil
}

// buildSamplePRReleasePartitionFilter constrains the run and test relations to
// releases discovered from the selected pull request. testReleaseColumn is an
// internal SQL column reference for the partitioned test relation. A non-nil
// empty slice means the lookup found no matching runs and must return no rows.
func buildSamplePRReleasePartitionFilter(releases []string, testReleaseColumn string) (string, []any) {
	if releases == nil {
		return "", nil
	}
	if len(releases) == 0 {
		return "FALSE", nil
	}
	return testReleaseColumn + ".prow_job_run_release = ANY(?) AND pjr.prow_job_release = ANY(?)",
		[]any{pq.Array(releases), pq.Array(releases)}
}

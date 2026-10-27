package postgres

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/openshift/sippy/pkg/apis/api/componentreport/reqopts"
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

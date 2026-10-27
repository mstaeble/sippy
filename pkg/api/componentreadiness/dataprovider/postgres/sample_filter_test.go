package postgres

import (
	"reflect"
	"strings"
	"testing"

	"github.com/lib/pq"
	"github.com/openshift/sippy/pkg/apis/api/componentreport/reqopts"
)

func TestBuildSamplePullRequestAndPayloadRunFilter(t *testing.T) {
	tests := []struct {
		name          string
		release       reqopts.Release
		wantFragments []string
		wantArgs      []any
	}{
		{name: "ordinary release has no run selector"},
		{
			name: "pull request matches its complete identity and run key",
			release: reqopts.Release{PullRequestOptions: &reqopts.PullRequest{
				Org: "openshift", Repo: "origin", PRNumber: "12345",
			}},
			wantFragments: []string{
				"EXISTS (", "prow_job_run_prow_pull_requests", "prow_pull_requests",
				"pr_run.prow_job_run_id = pjr.id",
				"pr_run.prow_job_run_release = pjr.prow_job_release",
				"pr_run.prow_job_run_timestamp = pjr.timestamp",
				"pr.org = ?", "pr.repo = ?", "pr.number = ?",
			},
			wantArgs: []any{"openshift", "origin", 12345},
		},
		{
			name: "payload matches all requested tags",
			release: reqopts.Release{PayloadOptions: &reqopts.Payload{
				Tags: []string{"4.20.0-0.ci-1", "4.20.0-0.ci-2"},
			}},
			wantFragments: []string{
				"EXISTS (", "release_job_runs", "release_tags",
				"payload_run.prow_job_run_id = pjr.id", "rt.release_tag = ANY(?)",
			},
			wantArgs: []any{pq.Array([]string{"4.20.0-0.ci-1", "4.20.0-0.ci-2"})},
		},
		{
			name: "pull request and payload both restrict the same run",
			release: reqopts.Release{
				PullRequestOptions: &reqopts.PullRequest{Org: "openshift", Repo: "origin", PRNumber: "42"},
				PayloadOptions:     &reqopts.Payload{Tags: []string{"4.20.0-0.ci-1"}},
			},
			wantFragments: []string{"pr.org = ?", "pr.repo = ?", "pr.number = ?", "rt.release_tag = ANY(?)"},
			wantArgs:      []any{"openshift", "origin", 42, pq.Array([]string{"4.20.0-0.ci-1"})},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause, args, err := buildSamplePullRequestAndPayloadRunFilter(tt.release)
			if err != nil {
				t.Fatalf("buildSamplePullRequestAndPayloadRunFilter() error = %v", err)
			}
			if len(tt.wantFragments) == 0 && clause != "" {
				t.Fatalf("unexpected run selector: %s", clause)
			}
			for _, fragment := range tt.wantFragments {
				if !strings.Contains(clause, fragment) {
					t.Errorf("run selector missing %q: %s", fragment, clause)
				}
			}
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("arguments = %#v, want %#v", args, tt.wantArgs)
			}
		})
	}
}

func TestBuildSamplePullRequestAndPayloadRunFilterRejectsInvalidPRNumber(t *testing.T) {
	_, _, err := buildSamplePullRequestAndPayloadRunFilter(reqopts.Release{PullRequestOptions: &reqopts.PullRequest{
		Org: "openshift", Repo: "origin", PRNumber: "invalid",
	}})
	if err == nil {
		t.Fatal("expected invalid pull request number to be rejected")
	}
}

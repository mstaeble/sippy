package crstatus

import "math/big"

// SummarizeTestJobRuns groups per-run TestJobRunRows by (job, test key) and
// produces pre-computed TestDetailsSummary entries. The result map is keyed
// by normalized prow job name, matching the input map's keys.
func SummarizeTestJobRuns(rows map[string][]TestJobRunRows) map[string][]TestDetailsSummary {
	result := map[string][]TestDetailsSummary{}

	for jobName, jobRows := range rows {
		byTestKey := map[string]*TestDetailsSummary{}
		var orderedKeys []string

		for _, row := range jobRows {
			summary, ok := byTestKey[row.TestKeyStr]
			if !ok {
				summary = &TestDetailsSummary{
					TestKey:    row.TestKey,
					TestKeyStr: row.TestKeyStr,
					ProwJob:    row.ProwJob,
				}
				byTestKey[row.TestKeyStr] = summary
				orderedKeys = append(orderedKeys, row.TestKeyStr)
			}

			summary.Stats = summary.Stats.AddTestCount(row.Count, false)

			if row.ProwJobRunID != "" {
				summary.JobRuns = append(summary.JobRuns, JobRunDetail{
					ProwJobRunID: row.ProwJobRunID,
					ProwJobURL:   row.ProwJobURL,
					StartTime:    row.StartTime,
					Count:        row.Count,
					JobLabels:    row.JobLabels,
					JobSymptoms:  row.JobSymptoms,
					TestFailures: row.TestFailures,
				})
			}

			if summary.JiraComponent == "" && row.JiraComponent != "" {
				summary.JiraComponent = row.JiraComponent
			}
			if summary.JiraComponentID == nil && row.JiraComponentID != nil {
				summary.JiraComponentID = new(big.Rat).Set(row.JiraComponentID)
			}
			if summary.TestName == "" && row.TestName != "" {
				summary.TestName = row.TestName
			}
			if summary.Lifecycle != "informing" {
				if row.Lifecycle == "informing" {
					summary.Lifecycle = "informing"
				} else if summary.Lifecycle == "" && row.Lifecycle != "" {
					summary.Lifecycle = row.Lifecycle
				}
			}
		}

		for _, key := range orderedKeys {
			result[jobName] = append(result[jobName], *byTestKey[key])
		}
	}

	return result
}

package matviewquery

import (
	fischer "github.com/glycerine/golang-fisher-exact"

	"github.com/openshift/sippy/pkg/apis/api/componentreport/crstatus"
	"github.com/openshift/sippy/pkg/apis/api/componentreport/crtest"
	"github.com/openshift/sippy/pkg/apis/api/componentreport/reqopts"
)

// analyzeAndBuild applies Fisher Exact Test to the dbGroupBy-aggregated
// candidates and builds the report status maps. No Go-side dbGroupBy
// aggregation is needed since SQL already did it.
func analyzeAndBuild(
	candidates []candidateRow,
	gridRows []cellGridRow,
	opts reqopts.RequestOptions,
) *QueryResult {
	minimumFailure := opts.AdvancedOption.MinimumFailure
	pityFactor := float64(opts.AdvancedOption.PityFactor) / 100.0
	confidence := opts.AdvancedOption.Confidence

	baseStatus := make(map[string]crstatus.TestStatus)
	sampleStatus := make(map[string]crstatus.TestStatus)

	for _, c := range candidates {
		variants := parseVariantKey(c.VariantKey)

		sampleFailures := c.SampleTotal - c.SampleSuccess
		if sampleFailures < minimumFailure {
			continue
		}
		baseTotal := 0
		baseSuccess := 0
		baseFlake := 0
		if c.BaseTotal != nil {
			baseTotal = *c.BaseTotal
			baseSuccess = *c.BaseSuccess
			baseFlake = *c.BaseFlake
		}
		if baseTotal == 0 {
			continue
		}

		basePassRate := float64(baseSuccess) / float64(baseTotal)
		samplePassRate := float64(c.SampleSuccess) / float64(c.SampleTotal)
		if basePassRate-samplePassRate <= pityFactor {
			continue
		}

		_, _, fisherP, _ := fischer.FisherExactTest(
			c.SampleTotal-c.SampleSuccess, c.SampleSuccess,
			baseTotal-baseSuccess, baseSuccess,
		)
		if fisherP >= 1.0-float64(confidence)/100.0 {
			continue
		}

		testKey := crtest.KeyWithVariants{
			TestID:   c.UniqueID,
			Variants: variants,
		}
		keyStr := testKey.KeyOrDie()
		variantSlice := variantMapToSortedSlice(variants)

		sampleStatus[keyStr] = crstatus.TestStatus{
			Component:    c.Component,
			Capabilities: c.Capabilities,
			Variants:     variantSlice,
			Count: crtest.Count{
				TotalCount:   c.SampleTotal,
				SuccessCount: c.SampleSuccess,
				FlakeCount:   c.SampleFlake,
			},
		}
		baseStatus[keyStr] = crstatus.TestStatus{
			Component:    c.Component,
			Capabilities: c.Capabilities,
			Variants:     variantSlice,
			Count: crtest.Count{
				TotalCount:   baseTotal,
				SuccessCount: baseSuccess,
				FlakeCount:   baseFlake,
			},
		}
	}

	cellGrid := buildCellGrid(gridRows)

	return &QueryResult{
		BaseStatus:   baseStatus,
		SampleStatus: sampleStatus,
		CellGrid:     cellGrid,
	}
}

func buildCellGrid(gridRows []cellGridRow) []CellGridEntry {
	seen := make(map[string]bool)
	var cellGrid []CellGridEntry
	for _, g := range gridRows {
		if seen[g.Component+"|"+g.VariantKey] {
			continue
		}
		seen[g.Component+"|"+g.VariantKey] = true
		cellGrid = append(cellGrid, CellGridEntry{
			Component: g.Component,
			Variants:  parseVariantKey(g.VariantKey),
		})
	}
	return cellGrid
}

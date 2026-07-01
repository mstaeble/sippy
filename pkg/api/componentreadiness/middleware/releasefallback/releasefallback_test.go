package releasefallback

import (
	"encoding/json"
	"testing"

	"cloud.google.com/go/civil"

	"github.com/openshift/sippy/pkg/apis/api/componentreport/crstatus"
	"github.com/openshift/sippy/pkg/apis/api/componentreport/crtest"
	"github.com/openshift/sippy/pkg/apis/api/componentreport/reqopts"
	"github.com/openshift/sippy/pkg/apis/api/componentreport/testdetails"
	v1 "github.com/openshift/sippy/pkg/apis/sippy/v1"
	"github.com/stretchr/testify/assert"
)

func Test_PreAnalysis(t *testing.T) {
	reqOpts419 := reqopts.RequestOptions{
		BaseRelease: reqopts.Release{
			Name: "4.19",
		},
		AdvancedOption: reqopts.Advanced{IncludeMultiReleaseAnalysis: true},
	}
	test1ID := "test1ID"
	test1Variants := map[string]string{
		"Arch":     "amd64",
		"Platform": "aws",
	}
	test1VariantsFlattened := []string{"Arch:amd64", "Platform:aws"}
	test1MapKey := crtest.KeyWithVariants{
		TestID:   test1ID,
		Variants: test1Variants,
	}
	test1KeyBytes, err := json.Marshal(test1MapKey)
	test1KeyStr := string(test1KeyBytes)
	assert.NoError(t, err)
	test1RTI := crtest.Identification{
		RowIdentification: crtest.RowIdentification{
			Component:  "",
			Capability: "",
			TestName:   "test 1",
			TestSuite:  "",
			TestID:     test1ID,
		},
		ColumnIdentification: crtest.ColumnIdentification{
			Variants: test1Variants,
		},
	}

	// 4.19 will be our assumed requested base release, which may trigger fallback to 4.18 or 4.17 in these tests
	start419 := civil.Date{Year: 2025, Month: 3, Day: 2}
	end419 := civil.Date{Year: 2025, Month: 4, Day: 30}

	start418 := civil.Date{Year: 2025, Month: 2, Day: 1}
	end418 := civil.Date{Year: 2025, Month: 3, Day: 1}
	fallbackMap418 := ReleaseTestMap{
		Release: "4.18",
		Start:   &start418,
		End:     &end418,
		Tests: map[string]crstatus.TestStatus{
			test1KeyStr: buildTestStatus("test1", test1VariantsFlattened, 100, 95, 0),
		},
	}

	start417 := civil.Date{Year: 2024, Month: 12, Day: 1}
	end417 := civil.Date{Year: 2024, Month: 12, Day: 31}
	fallbackMap417 := ReleaseTestMap{
		Release: "4.17",
		Start:   &start417,
		End:     &end417,
		Tests: map[string]crstatus.TestStatus{
			test1KeyStr: buildTestStatus("test1", test1VariantsFlattened, 100, 98, 0),
		},
	}

	releaseConfigs := []v1.Release{
		{Release: "4.19", PreviousRelease: "4.18"},
		{Release: "4.18", PreviousRelease: "4.17"},
		{Release: "4.17", PreviousRelease: "4.16"},
	}

	tests := []struct {
		name             string
		reqOpts          reqopts.RequestOptions
		testKey          crtest.Identification
		fallbackReleases FallbackReleases
		testStats        *testdetails.TestComparison
		expectedStatus   *testdetails.TestComparison
	}{
		{
			name:    "fallback to prior release",
			reqOpts: reqOpts419,
			testKey: test1RTI,
			fallbackReleases: FallbackReleases{
				Releases: map[string]ReleaseTestMap{
					fallbackMap418.Release: fallbackMap418,
				},
			},
			testStats:      buildTestStats(100, 93, "4.19", &start419, &end419, nil),
			expectedStatus: buildTestStats(100, 95, "4.18", &start418, &end418, []string{"Overrode base stats (0.9300) using release 4.18 (0.9500)"}),
		},
		{
			name:    "fallback twice to prior release",
			reqOpts: reqOpts419,
			testKey: test1RTI,
			fallbackReleases: FallbackReleases{
				Releases: map[string]ReleaseTestMap{
					fallbackMap418.Release: fallbackMap418,
					fallbackMap417.Release: fallbackMap417, // 4.17 improves even further
				},
			},
			testStats:      buildTestStats(100, 93, "4.19", &start419, &end419, nil),
			expectedStatus: buildTestStats(100, 98, "4.17", &start417, &end417, []string{"Overrode base stats (0.9500) using release 4.17 (0.9800)"}),
		},
		{
			name:    "fallback once to two releases ago",
			reqOpts: reqOpts419,
			testKey: test1RTI,
			fallbackReleases: FallbackReleases{
				Releases: map[string]ReleaseTestMap{
					fallbackMap418.Release: fallbackMap418,
					fallbackMap417.Release: fallbackMap417, // 4.17 improves even further
				},
			},
			testStats:      buildTestStats(100, 97, "4.19", &start419, &end419, nil),
			expectedStatus: buildTestStats(100, 98, "4.17", &start417, &end417, []string{"Overrode base stats (0.9700) using release 4.17 (0.9800)"}),
		},
		{
			name:    "don't fallback to prior release",
			reqOpts: reqOpts419,
			testKey: test1RTI,
			fallbackReleases: FallbackReleases{
				Releases: map[string]ReleaseTestMap{
					fallbackMap418.Release: fallbackMap418,
				},
			},
			testStats:      buildTestStats(100, 100, "4.19", &start419, &end419, nil),
			expectedStatus: buildTestStats(100, 100, "4.19", &start419, &end419, nil),
		},
		{
			name:    "don't fallback to prior release with insufficient runs",
			reqOpts: reqOpts419,
			testKey: test1RTI,
			fallbackReleases: FallbackReleases{
				Releases: map[string]ReleaseTestMap{
					fallbackMap418.Release: fallbackMap418,
					fallbackMap417.Release: fallbackMap417,
				},
			},
			testStats: buildTestStats(10000, 9700, "4.19", &start419, &end419, nil),
			// No fallback release had at least 60% of our run count
			expectedStatus: buildTestStats(10000, 9700, "4.19", &start419, &end419, nil),
		},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rfb := NewReleaseFallbackMiddleware(nil, nil, test.reqOpts, releaseConfigs)
			rfb.cachedFallbackTestStatuses = &tests[i].fallbackReleases
			err := rfb.PreAnalysis(test.testKey, test.testStats)
			assert.NoError(t, err)
			assert.Equal(t, *test.expectedStatus, *test.testStats)
		})
	}
}
func TestCalculateFallbackReleases(t *testing.T) {
	ga419 := civil.Date{Year: 2025, Month: 4, Day: 30}
	ga418 := civil.Date{Year: 2025, Month: 3, Day: 1}
	ga417 := civil.Date{Year: 2024, Month: 12, Day: 31}
	ga416 := civil.Date{Year: 2024, Month: 6, Day: 30}

	releaseConfigs := []v1.Release{
		{Release: "4.20", PreviousRelease: "4.19"},
		{Release: "4.19", PreviousRelease: "4.18", GADate: &ga419},
		{Release: "4.18", PreviousRelease: "4.17", GADate: &ga418},
		{Release: "4.17", PreviousRelease: "4.16", GADate: &ga417},
		{Release: "4.16", PreviousRelease: "", GADate: &ga416},
	}

	result := calculateFallbackReleases("4.20", releaseConfigs, 3)
	expectedReleases := []string{"4.19", "4.18", "4.17"}
	expectedGADates := []*civil.Date{&ga419, &ga418, &ga417}

	assert.Equal(t, len(expectedReleases), len(result))
	for i := range expectedReleases {
		assert.Equal(t, expectedReleases[i], result[i].release)
		assert.Equal(t, expectedGADates[i], result[i].gaDate)
	}
}

//nolint:unparam
func buildTestStatus(testName string, variants []string, total, success, flake int) crstatus.TestStatus {
	return crstatus.TestStatus{
		TestName:     testName,
		TestSuite:    "conformance",
		Component:    "foo",
		Capabilities: nil,
		Variants:     variants,
		Count: crtest.Count{
			TotalCount:   total,
			SuccessCount: success,
			FlakeCount:   flake,
		},
	}
}

func buildTestStats(total, success int, release string, start, end *civil.Date, explanations []string) *testdetails.TestComparison {
	fails := total - success

	ts := &testdetails.TestComparison{
		BaseStats: &testdetails.ReleaseStats{
			Release: release,
			Start:   start,
			End:     end,
			Stats: crtest.Stats{
				FailureCount: fails,
				SuccessCount: success,
				FlakeCount:   0,
				SuccessRate:  crtest.CalculatePassRate(success, fails, 0, false),
			},
		},
	}
	if explanations != nil {
		ts.Explanations = explanations
	}
	return ts
}

package crdailysummary

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeStore struct {
	maxSummary    *time.Time
	maxSummaryErr error
	truncated     bool
	truncateErr   error
	releases      []string
	releasesErr   error
	aggregateErr  error

	vcidMappingPopulated    bool
	vcidMappingPopulatedErr error
	variantChanges          []uint
	variantChangesErr       error
	scopedRebuilt           []uint
	scopedRebuildErr        error
	totalProwJobs           int64
	totalProwJobsErr        error
	vcidMappingUpdated      bool
	vcidMappingErr          error
	deletedRows             int64
	deleteErr               error
	deleteCutoff            time.Time

	calls []aggregateCall
}

type aggregateCall struct {
	start, end            time.Time
	release               string
	skipConflictDetection bool
}

func (f *fakeStore) MaxSummaryDate() (*time.Time, error) {
	return f.maxSummary, f.maxSummaryErr
}

func (f *fakeStore) Truncate() error {
	f.truncated = true
	return f.truncateErr
}

func (f *fakeStore) Releases() ([]string, error) {
	if f.releases != nil {
		return f.releases, f.releasesErr
	}
	return []string{"4.22", "5.0"}, f.releasesErr
}

func (f *fakeStore) AggregateRangeForRelease(start, end time.Time, release string, skipConflictDetection bool) error {
	f.calls = append(f.calls, aggregateCall{start: start, end: end, release: release, skipConflictDetection: skipConflictDetection})
	return f.aggregateErr
}

func (f *fakeStore) VCIDMappingPopulated() (bool, error) {
	return f.vcidMappingPopulated, f.vcidMappingPopulatedErr
}

func (f *fakeStore) DetectVariantChanges() ([]uint, error) {
	return f.variantChanges, f.variantChangesErr
}

func (f *fakeStore) TotalProwJobCount() (int64, error) {
	if f.totalProwJobs == 0 {
		return 10000, f.totalProwJobsErr
	}
	return f.totalProwJobs, f.totalProwJobsErr
}

func (f *fakeStore) ScopedRebuild(changedProwJobIDs []uint) error {
	f.scopedRebuilt = changedProwJobIDs
	return f.scopedRebuildErr
}

func (f *fakeStore) UpdateVCIDMapping() error {
	f.vcidMappingUpdated = true
	return f.vcidMappingErr
}

func (f *fakeStore) DeleteOldRows(cutoff time.Time) (int64, error) {
	f.deleteCutoff = cutoff
	return f.deletedRows, f.deleteErr
}

func TestRefresh_Incremental(t *testing.T) {
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{maxSummary: &maxDate, vcidMappingPopulated: true}

	err := refreshSummaries(store, Options{})

	require.NoError(t, err)
	assert.Len(t, store.calls, 2)
	assert.False(t, store.truncated)
	assert.True(t, store.vcidMappingUpdated)
	assert.Nil(t, store.scopedRebuilt)
}

func TestRefresh_IncrementalCapsAtYesterday(t *testing.T) {
	future := time.Now().AddDate(0, 0, 1)
	store := &fakeStore{maxSummary: &future, vcidMappingPopulated: true}

	err := refreshSummaries(store, Options{})

	require.NoError(t, err)
	require.NotEmpty(t, store.calls)
	assert.True(t, store.calls[0].start.Before(future))
}

func TestRefresh_EmptyTableUsesDefaultLookbackAndSkipsConflictDetection(t *testing.T) {
	store := &fakeStore{}

	err := refreshSummaries(store, Options{})

	require.NoError(t, err)
	require.NotEmpty(t, store.calls)
	daysBefore := time.Since(store.calls[0].start).Hours() / 24
	assert.InDelta(t, defaultLookbackDays, daysBefore, 1)
	for _, call := range store.calls {
		assert.True(t, call.skipConflictDetection)
	}
}

func TestRefresh_RebuildTruncatesAndSkipsVariantDetection(t *testing.T) {
	store := &fakeStore{}

	err := refreshSummaries(store, Options{Rebuild: true})

	require.NoError(t, err)
	assert.True(t, store.truncated)
	assert.Len(t, store.calls, 2)
	assert.True(t, store.vcidMappingUpdated)
	assert.Nil(t, store.scopedRebuilt)
	for _, call := range store.calls {
		assert.True(t, call.skipConflictDetection)
	}
}

func TestRefresh_IncrementalUsesUpsert(t *testing.T) {
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{maxSummary: &maxDate, vcidMappingPopulated: true}

	err := refreshSummaries(store, Options{})

	require.NoError(t, err)
	assert.Len(t, store.calls, 2)
	for _, call := range store.calls {
		assert.False(t, call.skipConflictDetection)
	}
}

func TestRefresh_DateOverrides(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{maxSummary: &maxDate, vcidMappingPopulated: true}

	err := refreshSummaries(store, Options{StartOverride: &start, EndOverride: &end})

	require.NoError(t, err)
	require.Len(t, store.calls, 2)
	assert.Equal(t, start, store.calls[0].start)
	assert.Equal(t, end, store.calls[0].end)
}

func TestRefresh_VariantChangesDetected(t *testing.T) {
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{
		maxSummary:           &maxDate,
		vcidMappingPopulated: true,
		variantChanges:       []uint{101, 202, 303},
	}

	err := refreshSummaries(store, Options{})

	require.NoError(t, err)
	assert.Equal(t, []uint{101, 202, 303}, store.scopedRebuilt)
	assert.True(t, store.vcidMappingUpdated)
}

func TestRefresh_NoVariantChanges(t *testing.T) {
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{
		maxSummary:           &maxDate,
		vcidMappingPopulated: true,
		variantChanges:       []uint{},
	}

	err := refreshSummaries(store, Options{})

	require.NoError(t, err)
	assert.Nil(t, store.scopedRebuilt)
}

func TestRefresh_OldRowsCleanedUp(t *testing.T) {
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{maxSummary: &maxDate, vcidMappingPopulated: true, deletedRows: 42}

	err := refreshSummaries(store, Options{})

	require.NoError(t, err)
	daysBefore := time.Since(store.deleteCutoff).Hours() / 24
	assert.InDelta(t, retentionDays, daysBefore, 1)
}

func TestRefresh_TruncateError(t *testing.T) {
	store := &fakeStore{truncateErr: fmt.Errorf("permission denied")}

	err := refreshSummaries(store, Options{Rebuild: true})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncating table")
	assert.Empty(t, store.calls)
}

func TestRefresh_VariantDetectionError(t *testing.T) {
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{maxSummary: &maxDate, vcidMappingPopulated: true, variantChangesErr: fmt.Errorf("connection refused")}

	err := refreshSummaries(store, Options{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "detecting variant changes")
}

func TestRefresh_ScopedRebuildError(t *testing.T) {
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{
		maxSummary:           &maxDate,
		vcidMappingPopulated: true,
		variantChanges:       []uint{1},
		scopedRebuildErr:     fmt.Errorf("disk full"),
	}

	err := refreshSummaries(store, Options{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "scoped rebuild")
}

func TestRefresh_VCIDMappingError(t *testing.T) {
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{maxSummary: &maxDate, vcidMappingPopulated: true, vcidMappingErr: fmt.Errorf("connection refused")}

	err := refreshSummaries(store, Options{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating VCID mapping")
}

func TestRefresh_ReleasesError(t *testing.T) {
	store := &fakeStore{releasesErr: fmt.Errorf("connection refused")}

	err := refreshSummaries(store, Options{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "querying releases")
}

func TestRefresh_AggregateError(t *testing.T) {
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{maxSummary: &maxDate, vcidMappingPopulated: true, aggregateErr: fmt.Errorf("disk full")}

	err := refreshSummaries(store, Options{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "aggregating release")
}

func TestRefresh_VariantChangesExceedThresholdTruncates(t *testing.T) {
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	changes := make([]uint, 2500)
	for i := range changes {
		changes[i] = uint(i + 1)
	}
	store := &fakeStore{
		maxSummary:           &maxDate,
		vcidMappingPopulated: true,
		variantChanges:       changes,
		totalProwJobs:        10000,
	}

	err := refreshSummaries(store, Options{})

	require.NoError(t, err)
	assert.True(t, store.truncated)
	assert.Nil(t, store.scopedRebuilt)
}

func TestRefresh_VariantChangesBelowThresholdUsesScoped(t *testing.T) {
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{
		maxSummary:           &maxDate,
		vcidMappingPopulated: true,
		variantChanges:       []uint{1, 2, 3},
		totalProwJobs:        10000,
	}

	err := refreshSummaries(store, Options{})

	require.NoError(t, err)
	assert.False(t, store.truncated)
	assert.Equal(t, []uint{1, 2, 3}, store.scopedRebuilt)
}

func TestRefresh_NoReleases(t *testing.T) {
	store := &fakeStore{releases: []string{}}

	err := refreshSummaries(store, Options{})

	require.NoError(t, err)
	assert.Empty(t, store.calls)
}

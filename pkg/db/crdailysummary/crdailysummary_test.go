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
	aggregateErr  error
	aggregated    bool

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

	aggregateStart        time.Time
	aggregateEnd          time.Time
	aggregateSkipConflict bool
}

func (f *fakeStore) MaxSummaryDate() (*time.Time, error) {
	return f.maxSummary, f.maxSummaryErr
}

func (f *fakeStore) Truncate() error {
	f.truncated = true
	return f.truncateErr
}

func (f *fakeStore) AggregateRange(start, end time.Time, skipConflictDetection bool) error {
	f.aggregated = true
	f.aggregateStart = start
	f.aggregateEnd = end
	f.aggregateSkipConflict = skipConflictDetection
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
	assert.True(t, store.aggregated)
	assert.False(t, store.truncated)
	assert.True(t, store.vcidMappingUpdated)
	assert.Nil(t, store.scopedRebuilt)
}

func TestRefresh_IncrementalCapsAtYesterday(t *testing.T) {
	future := time.Now().AddDate(0, 0, 1)
	store := &fakeStore{maxSummary: &future, vcidMappingPopulated: true}

	err := refreshSummaries(store, Options{})

	require.NoError(t, err)
	assert.True(t, store.aggregated)
	assert.True(t, store.aggregateStart.Before(future))
}

func TestRefresh_EmptyTableUsesDefaultLookbackAndSkipsConflictDetection(t *testing.T) {
	store := &fakeStore{}

	err := refreshSummaries(store, Options{})

	require.NoError(t, err)
	assert.True(t, store.aggregated)
	daysBefore := time.Since(store.aggregateStart).Hours() / 24
	assert.InDelta(t, defaultLookbackDays, daysBefore, 1)
	assert.True(t, store.aggregateSkipConflict)
}

func TestRefresh_RebuildTruncatesAndSkipsVariantDetection(t *testing.T) {
	store := &fakeStore{}

	err := refreshSummaries(store, Options{Rebuild: true})

	require.NoError(t, err)
	assert.True(t, store.truncated)
	assert.True(t, store.aggregated)
	assert.True(t, store.vcidMappingUpdated)
	assert.Nil(t, store.scopedRebuilt)
	assert.True(t, store.aggregateSkipConflict)
}

func TestRefresh_IncrementalUsesUpsert(t *testing.T) {
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{maxSummary: &maxDate, vcidMappingPopulated: true}

	err := refreshSummaries(store, Options{})

	require.NoError(t, err)
	assert.True(t, store.aggregated)
	assert.False(t, store.aggregateSkipConflict)
}

func TestRefresh_DateOverrides(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{maxSummary: &maxDate, vcidMappingPopulated: true}

	err := refreshSummaries(store, Options{StartOverride: &start, EndOverride: &end})

	require.NoError(t, err)
	assert.True(t, store.aggregated)
	assert.Equal(t, start, store.aggregateStart)
	assert.Equal(t, end, store.aggregateEnd)
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
	assert.False(t, store.aggregated)
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

func TestRefresh_AggregateError(t *testing.T) {
	maxDate := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{maxSummary: &maxDate, vcidMappingPopulated: true, aggregateErr: fmt.Errorf("disk full")}

	err := refreshSummaries(store, Options{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "aggregating CR daily summaries")
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

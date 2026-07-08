package prefixsum

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
	minDaily      *time.Time
	minDailyErr   error
	maxDaily      *time.Time
	maxDailyErr   error
	updatedDays   []time.Time
	updateDayErr  error
	deletedRows   int64
	deleteErr     error
	deleteCutoff  time.Time
}

func (f *fakeStore) MaxSummaryDate() (*time.Time, error) {
	return f.maxSummary, f.maxSummaryErr
}

func (f *fakeStore) MinDailySummaryDate() (*time.Time, error) {
	return f.minDaily, f.minDailyErr
}

func (f *fakeStore) MaxDailySummaryDate() (*time.Time, error) {
	return f.maxDaily, f.maxDailyErr
}

func (f *fakeStore) UpdateDay(date time.Time) error {
	f.updatedDays = append(f.updatedDays, date)
	return f.updateDayErr
}

func (f *fakeStore) DeleteOldRows(cutoff time.Time) (int64, error) {
	f.deleteCutoff = cutoff
	return f.deletedRows, f.deleteErr
}

func TestRefresh_EmptyTableBackfillsFromMinDaily(t *testing.T) {
	minDaily := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	maxDaily := time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{minDaily: &minDaily, maxDaily: &maxDaily}

	err := refreshPrefixSums(store, Options{})

	require.NoError(t, err)
	assert.Len(t, store.updatedDays, 3)
	assert.Equal(t, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), store.updatedDays[0])
	assert.Equal(t, time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC), store.updatedDays[1])
	assert.Equal(t, time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC), store.updatedDays[2])
}

func TestRefresh_IncrementalUpdatesNewDays(t *testing.T) {
	maxPrefix := time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC)
	maxDaily := time.Date(2026, 7, 7, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{maxSummary: &maxPrefix, maxDaily: &maxDaily}

	err := refreshPrefixSums(store, Options{})

	require.NoError(t, err)
	assert.Len(t, store.updatedDays, 2)
	assert.Equal(t, time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC), store.updatedDays[0])
	assert.Equal(t, time.Date(2026, 7, 7, 0, 0, 0, 0, time.UTC), store.updatedDays[1])
}

func TestRefresh_AlreadyUpToDate(t *testing.T) {
	maxPrefix := time.Date(2026, 7, 7, 0, 0, 0, 0, time.UTC)
	maxDaily := time.Date(2026, 7, 7, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{maxSummary: &maxPrefix, maxDaily: &maxDaily}

	err := refreshPrefixSums(store, Options{})

	require.NoError(t, err)
	assert.Empty(t, store.updatedDays)
}

func TestRefresh_NoDailySummaries(t *testing.T) {
	store := &fakeStore{}

	err := refreshPrefixSums(store, Options{})

	require.NoError(t, err)
	assert.Empty(t, store.updatedDays)
}

func TestRefresh_CleansUpOldRows(t *testing.T) {
	maxPrefix := time.Date(2026, 7, 7, 0, 0, 0, 0, time.UTC)
	maxDaily := time.Date(2026, 7, 7, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{maxSummary: &maxPrefix, maxDaily: &maxDaily, deletedRows: 100}

	err := refreshPrefixSums(store, Options{})

	require.NoError(t, err)
	daysBefore := time.Since(store.deleteCutoff).Hours() / 24
	assert.InDelta(t, retentionDays, daysBefore, 1)
}

func TestRefresh_MaxSummaryDateError(t *testing.T) {
	store := &fakeStore{maxSummaryErr: fmt.Errorf("connection refused")}

	err := refreshPrefixSums(store, Options{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "max prefix sum date")
}

func TestRefresh_MinDailyError(t *testing.T) {
	store := &fakeStore{minDailyErr: fmt.Errorf("connection refused")}

	err := refreshPrefixSums(store, Options{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "min daily summary date")
}

func TestRefresh_UpdateDayError(t *testing.T) {
	minDaily := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	maxDaily := time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{minDaily: &minDaily, maxDaily: &maxDaily, updateDayErr: fmt.Errorf("disk full")}

	err := refreshPrefixSums(store, Options{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating prefix sums")
}

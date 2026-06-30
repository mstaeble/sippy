CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_test_daily_summaries_release_date
    ON test_daily_summaries (release, summary_date);

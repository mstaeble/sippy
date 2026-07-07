-- TRT-2741: Create cr_daily_summaries and cr_vcid_mappings tables.
--
-- cr_daily_summaries pre-aggregates test_daily_summaries by
-- variant_combination_id (collapsing many prow_job_ids into fewer
-- variant combinations). This lets the CR matviews skip the expensive
-- JOIN to prow_jobs during refresh.
--
-- cr_vcid_mappings tracks the prow_job_id to variant_combination_id
-- mapping so the refresh can detect when variants change and do a
-- scoped rebuild of only the affected rows.

CREATE TABLE IF NOT EXISTS cr_daily_summaries (
    test_id BIGINT NOT NULL,
    suite_id BIGINT NOT NULL DEFAULT 0,
    variant_combination_id BIGINT NOT NULL,
    release TEXT NOT NULL,
    summary_date DATE NOT NULL,
    successes INT NOT NULL DEFAULT 0,
    failures INT NOT NULL DEFAULT 0,
    flakes INT NOT NULL DEFAULT 0,
    runs INT NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_cr_daily_summaries_unique
    ON cr_daily_summaries (test_id, suite_id, variant_combination_id, release, summary_date);

CREATE TABLE IF NOT EXISTS cr_vcid_mappings (
    prow_job_id BIGINT PRIMARY KEY,
    variant_combination_id BIGINT NOT NULL
);

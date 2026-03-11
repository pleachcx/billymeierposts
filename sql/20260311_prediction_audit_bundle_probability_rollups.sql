BEGIN;

ALTER TABLE public.prediction_audit_runs
    DROP CONSTRAINT IF EXISTS prediction_audit_runs_stage_check;

ALTER TABLE public.prediction_audit_runs
    ADD CONSTRAINT prediction_audit_runs_stage_check
    CHECK (
        stage IN (
            'stage0_corpus_freeze',
            'stage1_candidate_extraction',
            'stage2_eligibility',
            'stage3_event_ledger',
            'stage4_match_scoring',
            'stage5_probability_model',
            'stage6_bundle_probability_rollup'
        )
    );

CREATE TABLE IF NOT EXISTS public.prediction_audit_bundle_rollups (
    id BIGSERIAL PRIMARY KEY,
    bundle_id BIGINT NOT NULL REFERENCES public.prediction_audit_bundles(id) ON DELETE CASCADE,
    rollup_run_id BIGINT REFERENCES public.prediction_audit_runs(id) ON DELETE SET NULL,
    stage5_run_id BIGINT REFERENCES public.prediction_audit_runs(id) ON DELETE SET NULL,
    event_family TEXT NOT NULL,
    scoped_prediction_count INTEGER NOT NULL CHECK (scoped_prediction_count > 0),
    probability_ready_count INTEGER NOT NULL CHECK (probability_ready_count >= 0),
    scoped_match_status TEXT NOT NULL CHECK (
        scoped_match_status IN ('unreviewed', 'exact_hit', 'near_hit', 'similar_only', 'miss', 'partial_hit', 'unresolved')
    ),
    scoped_status_counts JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(scoped_status_counts) = 'object'),
    p_observed_under_null DOUBLE PRECISION,
    observed_log10_under_null DOUBLE PRECISION,
    p_all_exact_under_null DOUBLE PRECISION,
    all_exact_log10_under_null DOUBLE PRECISION,
    p_all_near_or_better_under_null DOUBLE PRECISION,
    all_near_or_better_log10_under_null DOUBLE PRECISION,
    p_all_similar_or_better_under_null DOUBLE PRECISION,
    all_similar_or_better_log10_under_null DOUBLE PRECISION,
    p_all_miss_under_null DOUBLE PRECISION,
    all_miss_log10_under_null DOUBLE PRECISION,
    rollup_model_version TEXT,
    rollup_notes TEXT,
    rollup_meta JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(rollup_meta) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (bundle_id, rollup_run_id, event_family)
);

CREATE INDEX IF NOT EXISTS prediction_audit_bundle_rollups_stage5_family_idx
    ON public.prediction_audit_bundle_rollups(stage5_run_id, event_family, scoped_match_status);

CREATE INDEX IF NOT EXISTS prediction_audit_bundle_rollups_bundle_idx
    ON public.prediction_audit_bundle_rollups(bundle_id, event_family, created_at DESC);

COMMENT ON TABLE public.prediction_audit_bundle_rollups IS
    'Family-scoped probability rollups for multi-event bundles, derived from child prediction outcomes and null-model vectors.';

COMMENT ON COLUMN public.prediction_audit_bundle_rollups.p_observed_under_null IS
    'Product of each child prediction''s observed-outcome null probability for the scoped event family.';

COMMENT ON COLUMN public.prediction_audit_bundle_rollups.p_all_near_or_better_under_null IS
    'Probability that every scoped child prediction is at least a near hit under the family null model.';

COMMIT;

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
            'stage6_bundle_probability_rollup',
            'stage7_final_adjudication'
        )
    );

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS final_status TEXT NOT NULL DEFAULT 'pending';

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS final_reason TEXT;

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS last_final_review_run_id BIGINT REFERENCES public.prediction_audit_runs(id) ON DELETE SET NULL;

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS final_meta JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'prediction_audit_predictions_final_status_check'
          AND conrelid = 'public.prediction_audit_predictions'::regclass
    ) THEN
        ALTER TABLE public.prediction_audit_predictions
            ADD CONSTRAINT prediction_audit_predictions_final_status_check
            CHECK (final_status IN ('pending', 'included_in_statistics', 'excluded_from_statistics', 'permanently_unresolved'));
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'prediction_audit_predictions_final_meta_check'
          AND conrelid = 'public.prediction_audit_predictions'::regclass
    ) THEN
        ALTER TABLE public.prediction_audit_predictions
            ADD CONSTRAINT prediction_audit_predictions_final_meta_check
            CHECK (jsonb_typeof(final_meta) = 'object');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.prediction_audit_final_reviews (
    id BIGSERIAL PRIMARY KEY,
    prediction_id BIGINT NOT NULL REFERENCES public.prediction_audit_predictions(id) ON DELETE CASCADE,
    review_run_id BIGINT REFERENCES public.prediction_audit_runs(id) ON DELETE SET NULL,
    event_family TEXT NOT NULL,
    final_status TEXT NOT NULL CHECK (
        final_status IN ('included_in_statistics', 'excluded_from_statistics', 'permanently_unresolved')
    ),
    is_primary BOOLEAN NOT NULL DEFAULT false,
    reviewer TEXT,
    rationale TEXT,
    review_meta JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(review_meta) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS prediction_audit_predictions_final_idx
    ON public.prediction_audit_predictions(event_family_final, final_status, last_final_review_run_id);

CREATE INDEX IF NOT EXISTS prediction_audit_final_reviews_prediction_idx
    ON public.prediction_audit_final_reviews(prediction_id, is_primary, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS prediction_audit_final_reviews_primary_uidx
    ON public.prediction_audit_final_reviews(prediction_id)
    WHERE is_primary;

COMMENT ON TABLE public.prediction_audit_final_reviews IS
    'Auditable final inclusion/exclusion decisions for statistical cohorts after matching and probability assignment.';

COMMENT ON COLUMN public.prediction_audit_predictions.final_status IS
    'Final cohort status after explicit adjudication: included, excluded, or permanently unresolved.';

COMMIT;

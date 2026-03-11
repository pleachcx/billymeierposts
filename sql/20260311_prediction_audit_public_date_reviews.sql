BEGIN;

ALTER TABLE public.prediction_audit_runs
    DROP CONSTRAINT IF EXISTS prediction_audit_runs_stage_check;

ALTER TABLE public.prediction_audit_runs
    ADD CONSTRAINT prediction_audit_runs_stage_check
    CHECK (
        stage IN (
            'stage0_corpus_freeze',
            'stage0_provenance',
            'stage1_candidate_extraction',
            'stage2_eligibility',
            'stage3_event_ledger',
            'stage4_match_scoring',
            'stage5_probability_model',
            'stage6_bundle_probability_rollup',
            'stage7_final_adjudication',
            'stage8_publication_adjudication'
        )
    );

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS public_date_status TEXT NOT NULL DEFAULT 'pending';

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS public_date_reason TEXT;

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS last_public_date_review_run_id BIGINT REFERENCES public.prediction_audit_runs(id) ON DELETE SET NULL;

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS public_date_meta JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'prediction_audit_predictions_public_date_status_check'
          AND conrelid = 'public.prediction_audit_predictions'::regclass
    ) THEN
        ALTER TABLE public.prediction_audit_predictions
            ADD CONSTRAINT prediction_audit_predictions_public_date_status_check
            CHECK (public_date_status IN ('pending', 'no_public_date_evidence', 'public_date_ok', 'event_precedes_publication'));
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'prediction_audit_predictions_public_date_meta_check'
          AND conrelid = 'public.prediction_audit_predictions'::regclass
    ) THEN
        ALTER TABLE public.prediction_audit_predictions
            ADD CONSTRAINT prediction_audit_predictions_public_date_meta_check
            CHECK (jsonb_typeof(public_date_meta) = 'object');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.prediction_audit_public_date_reviews (
    id BIGSERIAL PRIMARY KEY,
    prediction_id BIGINT NOT NULL REFERENCES public.prediction_audit_predictions(id) ON DELETE CASCADE,
    review_run_id BIGINT REFERENCES public.prediction_audit_runs(id) ON DELETE SET NULL,
    event_family TEXT NOT NULL,
    public_date_status TEXT NOT NULL CHECK (
        public_date_status IN ('no_public_date_evidence', 'public_date_ok', 'event_precedes_publication')
    ),
    is_primary BOOLEAN NOT NULL DEFAULT false,
    reviewer TEXT,
    rationale TEXT,
    review_meta JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(review_meta) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS prediction_audit_predictions_public_date_idx
    ON public.prediction_audit_predictions(event_family_final, public_date_status, last_public_date_review_run_id);

CREATE INDEX IF NOT EXISTS prediction_audit_public_date_reviews_prediction_idx
    ON public.prediction_audit_public_date_reviews(prediction_id, is_primary, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS prediction_audit_public_date_reviews_primary_uidx
    ON public.prediction_audit_public_date_reviews(prediction_id)
    WHERE is_primary;

COMMENT ON TABLE public.prediction_audit_public_date_reviews IS
    'Auditable publication-date adjudications that compare observed event timing against the current earliest provable public date evidence.';

COMMIT;

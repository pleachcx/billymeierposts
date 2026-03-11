BEGIN;

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS last_stage2_run_id BIGINT REFERENCES public.prediction_audit_runs(id) ON DELETE SET NULL;

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS stage2_reviewed_at TIMESTAMPTZ;

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS stage2_meta JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'prediction_audit_predictions_stage2_meta_check'
          AND conrelid = 'public.prediction_audit_predictions'::regclass
    ) THEN
        ALTER TABLE public.prediction_audit_predictions
            ADD CONSTRAINT prediction_audit_predictions_stage2_meta_check
            CHECK (jsonb_typeof(stage2_meta) = 'object');
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS prediction_audit_predictions_last_stage2_run_idx
    ON public.prediction_audit_predictions(last_stage2_run_id, stage2_reviewed_at DESC);

COMMENT ON COLUMN public.prediction_audit_predictions.last_stage2_run_id IS
    'Most recent Stage 2 normalization/review run that updated this prediction row.';

COMMENT ON COLUMN public.prediction_audit_predictions.stage2_reviewed_at IS
    'Timestamp of the most recent Stage 2 normalization/review pass for this prediction row.';

COMMENT ON COLUMN public.prediction_audit_predictions.stage2_meta IS
    'Structured metadata from Stage 2 normalization, including family key inputs and scoring reasons.';

COMMIT;

BEGIN;

ALTER TABLE public.prediction_audit_bundles
    ADD COLUMN IF NOT EXISTS last_stage4_run_id BIGINT REFERENCES public.prediction_audit_runs(id) ON DELETE SET NULL;

ALTER TABLE public.prediction_audit_bundles
    ADD COLUMN IF NOT EXISTS bundle_match_status TEXT NOT NULL DEFAULT 'unreviewed';

ALTER TABLE public.prediction_audit_bundles
    ADD COLUMN IF NOT EXISTS stage4_meta JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'prediction_audit_bundles_bundle_match_status_check'
          AND conrelid = 'public.prediction_audit_bundles'::regclass
    ) THEN
        ALTER TABLE public.prediction_audit_bundles
            ADD CONSTRAINT prediction_audit_bundles_bundle_match_status_check
            CHECK (bundle_match_status IN ('unreviewed', 'exact_hit', 'near_hit', 'similar_only', 'miss', 'partial_hit', 'unresolved'));
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'prediction_audit_bundles_stage4_meta_check'
          AND conrelid = 'public.prediction_audit_bundles'::regclass
    ) THEN
        ALTER TABLE public.prediction_audit_bundles
            ADD CONSTRAINT prediction_audit_bundles_stage4_meta_check
            CHECK (jsonb_typeof(stage4_meta) = 'object');
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS prediction_audit_bundles_stage4_idx
    ON public.prediction_audit_bundles(last_stage4_run_id, bundle_match_status);

COMMENT ON COLUMN public.prediction_audit_bundles.last_stage4_run_id IS
    'Most recent Stage 4 scoring run that rolled bundle-level outcomes from child prediction matches.';

COMMENT ON COLUMN public.prediction_audit_bundles.bundle_match_status IS
    'Bundle-level outcome derived from all child prediction match statuses.';

COMMENT ON COLUMN public.prediction_audit_bundles.stage4_meta IS
    'Structured Stage 4 bundle rollup data, including child-status counts and scoring rationale.';

COMMIT;

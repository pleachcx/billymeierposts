BEGIN;

CREATE TABLE IF NOT EXISTS public.prediction_audit_bundles (
    id BIGSERIAL PRIMARY KEY,
    parse_run_id BIGINT NOT NULL REFERENCES public.prediction_audit_runs(id) ON DELETE RESTRICT,
    contact_report_id INTEGER NOT NULL REFERENCES public.contact_reports(id) ON DELETE RESTRICT,
    report_number INTEGER NOT NULL,
    claimed_contact_date DATE NOT NULL,
    bundle_key TEXT NOT NULL UNIQUE,
    bundle_seq INTEGER NOT NULL CHECK (bundle_seq > 0),
    bundle_kind TEXT NOT NULL CHECK (bundle_kind IN ('compound_multi_event')),
    source_quote TEXT NOT NULL,
    source_start_offset INTEGER,
    source_end_offset INTEGER,
    component_count INTEGER NOT NULL CHECK (component_count >= 2),
    event_family_hint TEXT,
    bundle_significant BOOLEAN NOT NULL DEFAULT false,
    bundle_meta JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (parse_run_id, contact_report_id, bundle_seq),
    CHECK (source_start_offset IS NULL OR source_start_offset >= 0),
    CHECK (source_end_offset IS NULL OR source_end_offset >= 0),
    CHECK (source_start_offset IS NULL OR source_end_offset IS NULL OR source_end_offset >= source_start_offset),
    CHECK (jsonb_typeof(bundle_meta) = 'object')
);

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS bundle_key TEXT;

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS bundle_component_seq INTEGER;

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS bundle_component_count INTEGER;

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS bundle_role TEXT NOT NULL DEFAULT 'standalone';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'prediction_audit_predictions_bundle_role_check'
          AND conrelid = 'public.prediction_audit_predictions'::regclass
    ) THEN
        ALTER TABLE public.prediction_audit_predictions
            ADD CONSTRAINT prediction_audit_predictions_bundle_role_check
            CHECK (bundle_role IN ('standalone', 'compound_child'));
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'prediction_audit_predictions_bundle_component_seq_check'
          AND conrelid = 'public.prediction_audit_predictions'::regclass
    ) THEN
        ALTER TABLE public.prediction_audit_predictions
            ADD CONSTRAINT prediction_audit_predictions_bundle_component_seq_check
            CHECK (bundle_component_seq IS NULL OR bundle_component_seq > 0);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'prediction_audit_predictions_bundle_component_count_check'
          AND conrelid = 'public.prediction_audit_predictions'::regclass
    ) THEN
        ALTER TABLE public.prediction_audit_predictions
            ADD CONSTRAINT prediction_audit_predictions_bundle_component_count_check
            CHECK (bundle_component_count IS NULL OR bundle_component_count >= 2);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS prediction_audit_bundles_parse_run_idx
    ON public.prediction_audit_bundles(parse_run_id, report_number, bundle_seq);

CREATE INDEX IF NOT EXISTS prediction_audit_predictions_bundle_key_idx
    ON public.prediction_audit_predictions(bundle_key, bundle_component_seq);

COMMENT ON TABLE public.prediction_audit_bundles IS
    'Compound prediction bundles that contain multiple atomic child predictions from one source passage.';

COMMENT ON COLUMN public.prediction_audit_predictions.bundle_key IS
    'Logical parent bundle key when this prediction row is one child of a compound multi-event prediction.';

COMMENT ON COLUMN public.prediction_audit_predictions.bundle_component_seq IS
    '1-based position of this child prediction inside its compound bundle.';

COMMENT ON COLUMN public.prediction_audit_predictions.bundle_component_count IS
    'Total number of atomic child predictions inside the compound bundle.';

COMMENT ON COLUMN public.prediction_audit_predictions.bundle_role IS
    'Standalone prediction row or one child of a compound multi-event bundle.';

COMMIT;

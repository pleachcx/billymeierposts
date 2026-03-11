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
            'stage7_final_adjudication'
        )
    );

CREATE TABLE IF NOT EXISTS public.prediction_audit_report_provenance (
    id BIGSERIAL PRIMARY KEY,
    provenance_run_id BIGINT REFERENCES public.prediction_audit_runs(id) ON DELETE SET NULL,
    contact_report_id INTEGER NOT NULL REFERENCES public.contact_reports(id) ON DELETE CASCADE,
    report_number INTEGER NOT NULL,
    claimed_contact_date DATE,
    evidence_kind TEXT NOT NULL CHECK (
        evidence_kind IN (
            'claimed_contact_date_only',
            'repo_artifact',
            'manual_source_link',
            'external_archive',
            'publication_snapshot',
            'edition_note'
        )
    ),
    evidence_quality SMALLINT NOT NULL CHECK (evidence_quality BETWEEN 0 AND 2),
    evidence_public_date DATE,
    source_label TEXT,
    source_path TEXT,
    source_url TEXT,
    language TEXT,
    edition_or_translation TEXT,
    translator TEXT,
    source_hash TEXT,
    notes TEXT,
    raw_evidence JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(raw_evidence) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS prediction_audit_report_provenance_report_idx
    ON public.prediction_audit_report_provenance(contact_report_id, report_number, created_at DESC);

CREATE INDEX IF NOT EXISTS prediction_audit_report_provenance_quality_idx
    ON public.prediction_audit_report_provenance(evidence_quality, evidence_public_date, report_number);

COMMENT ON TABLE public.prediction_audit_report_provenance IS
    'Report-level provenance evidence used to establish or challenge earliest provable public dates for prediction scoring.';

COMMENT ON COLUMN public.prediction_audit_report_provenance.evidence_quality IS
    '0=unknown/unreliable, 1=plausible but weak, 2=well evidenced.';

COMMIT;

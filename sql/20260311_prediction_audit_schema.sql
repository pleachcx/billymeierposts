BEGIN;

CREATE TABLE IF NOT EXISTS public.prediction_audit_runs (
    id BIGSERIAL PRIMARY KEY,
    run_key TEXT NOT NULL UNIQUE,
    stage TEXT NOT NULL CHECK (
        stage IN (
            'stage0_corpus_freeze',
            'stage1_candidate_extraction',
            'stage2_eligibility',
            'stage3_event_ledger',
            'stage4_match_scoring',
            'stage5_probability_model'
        )
    ),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'running', 'completed', 'failed', 'abandoned')
    ),
    parser_version TEXT,
    prompt_version TEXT,
    source_corpus TEXT NOT NULL DEFAULT 'public.contact_reports.english_content',
    source_filter JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(source_filter) = 'object'),
    notes TEXT,
    run_meta JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(run_meta) = 'object'),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.prediction_audit_predictions (
    id BIGSERIAL PRIMARY KEY,
    parse_run_id BIGINT NOT NULL REFERENCES public.prediction_audit_runs(id) ON DELETE RESTRICT,
    contact_report_id INTEGER NOT NULL REFERENCES public.contact_reports(id) ON DELETE RESTRICT,
    report_number INTEGER NOT NULL,
    claimed_contact_date DATE NOT NULL,
    earliest_provable_public_date DATE,
    public_date_basis TEXT,
    source_language TEXT NOT NULL DEFAULT 'english' CHECK (source_language IN ('english', 'german')),
    source_text_hash TEXT NOT NULL,
    candidate_seq INTEGER NOT NULL CHECK (candidate_seq > 0),
    source_quote TEXT NOT NULL,
    source_start_offset INTEGER,
    source_end_offset INTEGER,
    future_claim_present BOOLEAN NOT NULL DEFAULT true,
    candidate_class TEXT NOT NULL CHECK (
        candidate_class IN (
            'discrete_event',
            'state_change',
            'trend_claim',
            'conditional_future_claim',
            'ambiguous_future_claim'
        )
    ),
    claim_normalized TEXT NOT NULL,
    event_family_provisional TEXT,
    time_text TEXT,
    location_text TEXT,
    actor_text TEXT,
    magnitude_text TEXT,
    conditionality TEXT NOT NULL DEFAULT 'none',
    ambiguity_flags JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(ambiguity_flags) = 'array'),
    extractor_confidence NUMERIC(5,4) CHECK (extractor_confidence BETWEEN 0 AND 1),
    extractor_model TEXT,
    extractor_meta JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(extractor_meta) = 'object'),
    stage2_label TEXT NOT NULL DEFAULT 'pending_review' CHECK (
        stage2_label IN (
            'pending_review',
            'not_a_prediction',
            'prediction_but_not_meaningful',
            'prediction_but_not_measurable',
            'prediction_with_weak_provenance',
            'eligible_prediction',
            'significant_prediction',
            'duplicate_restating_prior_prediction'
        )
    ),
    meaningfulness_score SMALLINT CHECK (meaningfulness_score BETWEEN 0 AND 3),
    measurability_score SMALLINT CHECK (measurability_score BETWEEN 0 AND 4),
    provenance_score SMALLINT CHECK (provenance_score BETWEEN 0 AND 2),
    event_family_final TEXT,
    time_window_start DATE,
    time_window_end DATE,
    target_type TEXT CHECK (target_type IN ('point', 'polygon', 'region', 'country', 'actor', 'none')),
    target_name TEXT,
    target_lat NUMERIC(9,6),
    target_lon NUMERIC(9,6),
    target_radius_km NUMERIC(8,2) CHECK (target_radius_km IS NULL OR target_radius_km >= 0),
    actor_name TEXT,
    magnitude_min NUMERIC(6,2),
    magnitude_max NUMERIC(6,2),
    severity_band TEXT,
    prediction_family_key TEXT,
    duplicate_of_prediction_id BIGINT REFERENCES public.prediction_audit_predictions(id) ON DELETE SET NULL,
    rulebook_version TEXT,
    eligible BOOLEAN NOT NULL DEFAULT false,
    significant BOOLEAN NOT NULL DEFAULT false,
    match_status TEXT NOT NULL DEFAULT 'unreviewed' CHECK (
        match_status IN (
            'unreviewed',
            'exact_hit',
            'near_hit',
            'similar_only',
            'miss',
            'unresolved',
            'excluded'
        )
    ),
    p_exact_under_null NUMERIC(14,12) CHECK (p_exact_under_null IS NULL OR (p_exact_under_null >= 0 AND p_exact_under_null <= 1)),
    p_near_under_null NUMERIC(14,12) CHECK (p_near_under_null IS NULL OR (p_near_under_null >= 0 AND p_near_under_null <= 1)),
    p_similar_under_null NUMERIC(14,12) CHECK (p_similar_under_null IS NULL OR (p_similar_under_null >= 0 AND p_similar_under_null <= 1)),
    p_miss_under_null NUMERIC(14,12) CHECK (p_miss_under_null IS NULL OR (p_miss_under_null >= 0 AND p_miss_under_null <= 1)),
    probability_model_version TEXT,
    probability_notes TEXT,
    probability_meta JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(probability_meta) = 'object'),
    review_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (parse_run_id, contact_report_id, candidate_seq),
    CHECK (source_start_offset IS NULL OR source_start_offset >= 0),
    CHECK (source_end_offset IS NULL OR source_end_offset >= 0),
    CHECK (source_start_offset IS NULL OR source_end_offset IS NULL OR source_end_offset >= source_start_offset),
    CHECK (time_window_start IS NULL OR time_window_end IS NULL OR time_window_end >= time_window_start),
    CHECK (magnitude_min IS NULL OR magnitude_max IS NULL OR magnitude_max >= magnitude_min)
);

CREATE TABLE IF NOT EXISTS public.prediction_audit_event_ledger (
    id BIGSERIAL PRIMARY KEY,
    prediction_id BIGINT NOT NULL REFERENCES public.prediction_audit_predictions(id) ON DELETE CASCADE,
    ledger_run_id BIGINT REFERENCES public.prediction_audit_runs(id) ON DELETE SET NULL,
    source_name TEXT NOT NULL,
    source_version TEXT,
    external_event_id TEXT,
    event_family TEXT NOT NULL,
    event_title TEXT,
    event_start_date DATE,
    event_end_date DATE,
    location_name TEXT,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    distance_km NUMERIC(8,2),
    time_delta_days INTEGER,
    magnitude_value NUMERIC(6,2),
    severity_band TEXT,
    exact_band BOOLEAN NOT NULL DEFAULT false,
    near_band BOOLEAN NOT NULL DEFAULT false,
    log_only_band BOOLEAN NOT NULL DEFAULT false,
    source_url TEXT,
    source_excerpt TEXT,
    raw_event JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(raw_event) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (event_end_date IS NULL OR event_start_date IS NULL OR event_end_date >= event_start_date),
    CHECK (distance_km IS NULL OR distance_km >= 0)
);

CREATE TABLE IF NOT EXISTS public.prediction_audit_match_reviews (
    id BIGSERIAL PRIMARY KEY,
    prediction_id BIGINT NOT NULL REFERENCES public.prediction_audit_predictions(id) ON DELETE CASCADE,
    event_ledger_id BIGINT REFERENCES public.prediction_audit_event_ledger(id) ON DELETE SET NULL,
    review_run_id BIGINT REFERENCES public.prediction_audit_runs(id) ON DELETE SET NULL,
    match_status TEXT NOT NULL CHECK (
        match_status IN ('exact_hit', 'near_hit', 'similar_only', 'miss', 'unresolved')
    ),
    is_primary BOOLEAN NOT NULL DEFAULT false,
    reviewer TEXT,
    confidence NUMERIC(5,4) CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    rationale TEXT,
    review_meta JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(review_meta) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS prediction_audit_runs_stage_status_idx
    ON public.prediction_audit_runs(stage, status, created_at DESC);

CREATE INDEX IF NOT EXISTS prediction_audit_predictions_report_idx
    ON public.prediction_audit_predictions(contact_report_id, report_number, candidate_seq);

CREATE INDEX IF NOT EXISTS prediction_audit_predictions_stage2_idx
    ON public.prediction_audit_predictions(stage2_label, significant, event_family_final);

CREATE INDEX IF NOT EXISTS prediction_audit_predictions_match_idx
    ON public.prediction_audit_predictions(match_status, significant, event_family_final);

CREATE INDEX IF NOT EXISTS prediction_audit_predictions_family_idx
    ON public.prediction_audit_predictions(prediction_family_key)
    WHERE prediction_family_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS prediction_audit_event_ledger_prediction_idx
    ON public.prediction_audit_event_ledger(prediction_id, exact_band, near_band, log_only_band);

CREATE INDEX IF NOT EXISTS prediction_audit_event_ledger_source_idx
    ON public.prediction_audit_event_ledger(source_name, external_event_id);

CREATE INDEX IF NOT EXISTS prediction_audit_match_reviews_prediction_idx
    ON public.prediction_audit_match_reviews(prediction_id, is_primary, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS prediction_audit_match_reviews_primary_uidx
    ON public.prediction_audit_match_reviews(prediction_id)
    WHERE is_primary;

ALTER TABLE public.prediction_audit_predictions
    ADD COLUMN IF NOT EXISTS best_event_ledger_id BIGINT REFERENCES public.prediction_audit_event_ledger(id) ON DELETE SET NULL;

COMMENT ON TABLE public.prediction_audit_runs IS
    'Versioned runs for staged prediction extraction, normalization, matching, and probability modeling.';

COMMENT ON TABLE public.prediction_audit_predictions IS
    'One row per extracted future claim candidate, later normalized and scored as a measurable prediction.';

COMMENT ON TABLE public.prediction_audit_event_ledger IS
    'External observed events collected against frozen prediction definitions before final hit/miss scoring.';

COMMENT ON TABLE public.prediction_audit_match_reviews IS
    'Auditable match decisions linking prediction rows to event-ledger rows or explicit misses.';

COMMIT;

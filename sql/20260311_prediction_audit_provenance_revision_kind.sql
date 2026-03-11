BEGIN;

ALTER TABLE public.prediction_audit_report_provenance
    DROP CONSTRAINT IF EXISTS prediction_audit_report_provenance_evidence_kind_check;

ALTER TABLE public.prediction_audit_report_provenance
    ADD CONSTRAINT prediction_audit_report_provenance_evidence_kind_check
    CHECK (
        evidence_kind IN (
            'claimed_contact_date_only',
            'repo_artifact',
            'manual_source_link',
            'external_archive',
            'publication_snapshot',
            'edition_note',
            'wiki_first_revision'
        )
    );

COMMIT;

#!/usr/bin/env python3

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any


TIER_CLAIMED_ONLY = "claimed_contact_date_only"
TIER_OFFICIAL_ORIGINAL = "official_figu_original"
TIER_OFFICIAL_TRANSLATION = "official_figu_translation"
TIER_MIRROR_TRANSLATION = "mirror_translation_page"
TIER_MIRROR_REVISION = "mirror_revision_timestamp"
TIER_ARCHIVE = "external_archive"
TIER_SECONDARY = "secondary_source"
TIER_REPO = "repo_artifact"

PUBLIC_DATE_COHORT_INCLUDED = "included_in_current_public_date_cohort"
PUBLIC_DATE_COHORT_EXCLUDED = "excluded_currently_unrescued"
PUBLIC_DATE_COHORT_PENDING = "pending_more_public_evidence"

BUCKET_PRIMARY = "primary_official"
BUCKET_MIRROR = "mirror"
BUCKET_SECONDARY = "secondary"
BUCKET_CLAIMED = "claimed_only"

TIER_RANK = {
    TIER_OFFICIAL_ORIGINAL: 400,
    TIER_OFFICIAL_TRANSLATION: 350,
    TIER_MIRROR_TRANSLATION: 250,
    TIER_MIRROR_REVISION: 220,
    TIER_ARCHIVE: 180,
    TIER_SECONDARY: 160,
    TIER_REPO: 120,
    TIER_CLAIMED_ONLY: 0,
}


def resolve_stage2_run(cur, run_key: str | None) -> dict[str, Any]:
    if run_key:
        cur.execute(
            """
            SELECT id, run_key, status, completed_at
            FROM public.prediction_audit_runs
            WHERE stage = 'stage2_eligibility' AND run_key = %s
            """,
            (run_key,),
        )
    else:
        cur.execute(
            """
            SELECT id, run_key, status, completed_at
            FROM public.prediction_audit_runs
            WHERE stage = 'stage2_eligibility'
              AND status = 'completed'
            ORDER BY completed_at DESC NULLS LAST, id DESC
            LIMIT 1
            """
        )
    row = cur.fetchone()
    if not row:
        if run_key:
            raise RuntimeError(f"Missing Stage 2 run {run_key}.")
        raise RuntimeError("No completed Stage 2 run found.")
    return dict(row)


def fetch_report_provenance_rows(cur, report_numbers: list[int]) -> list[dict[str, Any]]:
    if not report_numbers:
        return []
    cur.execute(
        """
        SELECT
            rp.id,
            rp.report_number,
            rp.evidence_kind,
            rp.evidence_quality,
            rp.evidence_public_date,
            rp.source_label,
            rp.source_url,
            rp.language,
            rp.edition_or_translation,
            rp.translator,
            rp.created_at
        FROM public.prediction_audit_report_provenance rp
        WHERE rp.report_number = ANY(%s)
        ORDER BY rp.report_number, rp.evidence_public_date NULLS LAST, rp.evidence_quality DESC, rp.created_at ASC, rp.id ASC
        """,
        (report_numbers,),
    )
    return [dict(row) for row in cur.fetchall()]


def classify_provenance_row(row: dict[str, Any]) -> dict[str, Any]:
    evidence_kind = (row.get("evidence_kind") or "").strip().lower()
    source_url = (row.get("source_url") or "").strip().lower()
    source_label = (row.get("source_label") or "").strip().lower()
    language = (row.get("language") or "").strip().lower()

    tier = TIER_SECONDARY
    bucket = BUCKET_SECONDARY

    if evidence_kind == "claimed_contact_date_only":
        tier = TIER_CLAIMED_ONLY
        bucket = BUCKET_CLAIMED
    elif "figu.org" in source_url or "figu.ch" in source_url or "official figu" in source_label:
        bucket = BUCKET_PRIMARY
        tier = TIER_OFFICIAL_ORIGINAL if language == "german" else TIER_OFFICIAL_TRANSLATION
    elif "futureofmankind.co.uk" in source_url:
        bucket = BUCKET_MIRROR
        tier = TIER_MIRROR_REVISION if evidence_kind == "wiki_first_revision" else TIER_MIRROR_TRANSLATION
    elif evidence_kind == "external_archive":
        tier = TIER_ARCHIVE
        bucket = BUCKET_SECONDARY
    elif evidence_kind == "repo_artifact":
        tier = TIER_REPO
        bucket = BUCKET_SECONDARY

    return {
        "provenance_source_tier": tier,
        "provenance_source_bucket": bucket,
        "provenance_source_rank": TIER_RANK[tier],
    }


def classify_gap_bucket(lag_days: int | None) -> str | None:
    if lag_days is None:
        return None
    if lag_days >= 0:
        return "not_conflict"
    if lag_days >= -7:
        return "tiny_gap"
    if lag_days >= -30:
        return "small_gap"
    if lag_days >= -180:
        return "medium_gap"
    if lag_days >= -730:
        return "large_gap"
    return "deep_archive_gap"


def derive_public_date_cohort_status(public_date_status: str | None) -> str:
    if public_date_status == "public_date_ok":
        return PUBLIC_DATE_COHORT_INCLUDED
    if public_date_status == "event_precedes_publication":
        return PUBLIC_DATE_COHORT_EXCLUDED
    return PUBLIC_DATE_COHORT_PENDING


def _earliest_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("evidence_public_date") or date.max,
        -(row.get("evidence_quality") or 0),
        row.get("created_at"),
        row.get("id"),
    )


def _best_tier_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    classified = classify_provenance_row(row)
    return (
        -classified["provenance_source_rank"],
        -(row.get("evidence_quality") or 0),
        row.get("evidence_public_date") or date.max,
        row.get("created_at"),
        row.get("id"),
    )


def annotate_predictions_with_provenance(
    prediction_rows: list[dict[str, Any]],
    provenance_rows: list[dict[str, Any]],
) -> None:
    by_report: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for provenance_row in provenance_rows:
        by_report[int(provenance_row["report_number"])].append(provenance_row)

    for prediction in prediction_rows:
        report_rows = by_report.get(int(prediction["report_number"]), [])
        current_row = min(report_rows, key=_earliest_sort_key) if report_rows else None
        best_row = min(report_rows, key=_best_tier_sort_key) if report_rows else None

        current_classification = classify_provenance_row(current_row) if current_row else None
        best_classification = classify_provenance_row(best_row) if best_row else None

        earliest_primary = min(
            (row["evidence_public_date"] for row in report_rows if classify_provenance_row(row)["provenance_source_bucket"] == BUCKET_PRIMARY and row.get("evidence_public_date")),
            default=None,
        )
        earliest_mirror = min(
            (row["evidence_public_date"] for row in report_rows if classify_provenance_row(row)["provenance_source_bucket"] == BUCKET_MIRROR and row.get("evidence_public_date")),
            default=None,
        )
        earliest_secondary = min(
            (row["evidence_public_date"] for row in report_rows if classify_provenance_row(row)["provenance_source_bucket"] == BUCKET_SECONDARY and row.get("evidence_public_date")),
            default=None,
        )

        prediction["current_public_evidence_kind"] = current_row.get("evidence_kind") if current_row else None
        prediction["current_public_source_label"] = current_row.get("source_label") if current_row else None
        prediction["current_public_source_url"] = current_row.get("source_url") if current_row else None
        prediction["current_public_source_tier"] = current_classification["provenance_source_tier"] if current_classification else None
        prediction["current_public_source_bucket"] = current_classification["provenance_source_bucket"] if current_classification else None

        prediction["best_available_source_label"] = best_row.get("source_label") if best_row else None
        prediction["best_available_source_url"] = best_row.get("source_url") if best_row else None
        prediction["best_available_source_tier"] = best_classification["provenance_source_tier"] if best_classification else None
        prediction["best_available_source_bucket"] = best_classification["provenance_source_bucket"] if best_classification else None

        prediction["earliest_primary_source_date"] = earliest_primary
        prediction["earliest_mirror_source_date"] = earliest_mirror
        prediction["earliest_secondary_source_date"] = earliest_secondary

        event_date = prediction.get("event_start_date")
        if earliest_primary and event_date:
            prediction["publication_lag_days_vs_primary_source"] = (event_date - earliest_primary).days
        else:
            prediction["publication_lag_days_vs_primary_source"] = None
        prediction["publication_conflict_gap_bucket"] = classify_gap_bucket(prediction.get("publication_lag_days_vs_event"))

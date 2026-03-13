#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from provenance_export_helpers import resolve_stage2_run


SCRIPT_VERSION = "unscored_prediction_queue_v1"
OUTPUT_ROOT = Path("data") / "exports" / "unscored"
P3_SUPPORTED_FAMILIES = {"epidemic", "volcano", "storm", "politics_election", "aviation_space"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a ranked queue of unscored eligible/significant prediction rows.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key to scope the queue. Defaults to the latest completed Stage 2 run.")
    parser.add_argument("--output-dir", help="Output directory. Defaults to data/exports/unscored/unscored-prediction-queue-<timestamp>.")
    return parser.parse_args()


def csv_safe(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: csv_safe(row.get(field)) for field in fieldnames})


def derive_family_guess(row: dict[str, Any]) -> str | None:
    return row.get("event_family_final") or row.get("event_family_provisional")


def derive_recovery_bucket(row: dict[str, Any]) -> str:
    family = row["family_guess"]
    time_window_end = row.get("time_window_end")
    claimed_contact_date = row.get("claimed_contact_date")

    if time_window_end and claimed_contact_date and time_window_end < claimed_contact_date:
        return "retire_past_event_reference"
    if row["stage2_label"] == "prediction_but_not_measurable" and family in P3_SUPPORTED_FAMILIES:
        return "stage2_revisit_in_supported_family"
    if family in P3_SUPPORTED_FAMILIES:
        return "promote_via_existing_family_pipeline"
    if family == "earthquake":
        return "existing_pipeline_outside_current_p3_scope"
    if family is None:
        return "needs_parser_or_stage2_family_resolution"
    return "outside_current_rulebook_scope"


def derive_recovery_rationale(row: dict[str, Any]) -> str:
    family = row["family_guess"]
    bucket = row["recovery_bucket"]

    if bucket == "retire_past_event_reference":
        return "Time window ends before the claimed contact date, so this row should retire instead of being scored."
    if bucket == "stage2_revisit_in_supported_family":
        return f"{family} is in the active P3 family set, but Stage 2 excluded the row as not measurable; review notes and family-specific overrides may still support a bounded recovery or explicit retirement."
    if bucket == "promote_via_existing_family_pipeline":
        return f"{family} already has Stage 3-7 scaffolding in the current pack; this row is a candidate for a curated override or clean retirement."
    if bucket == "existing_pipeline_outside_current_p3_scope":
        return "Earthquake scaffolding exists, but this queue exporter flags the row for later handling because the current pack is scoped to other recovery families first."
    if bucket == "needs_parser_or_stage2_family_resolution":
        return "Stage 2 left the row without a stable family assignment, so parser/review logic must clarify it before family scoring."
    return f"{family} lacks an active P3 recovery rulebook, so this row should be deferred or explicitly retired instead of stretched into the wrong family."


def rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    bucket_rank = {
        "promote_via_existing_family_pipeline": 0,
        "stage2_revisit_in_supported_family": 1,
        "retire_past_event_reference": 2,
        "existing_pipeline_outside_current_p3_scope": 3,
        "needs_parser_or_stage2_family_resolution": 4,
        "outside_current_rulebook_scope": 5,
    }
    return (
        bucket_rank.get(row["recovery_bucket"], 9),
        not row["significant"],
        row["family_guess"] not in P3_SUPPORTED_FAMILIES,
        row["time_window_start"] is None,
        row["report_number"],
        row["candidate_seq"],
    )


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "prediction_count": len(rows),
        "family_guess_counts": dict(Counter(row["family_guess"] or "unknown" for row in rows)),
        "stage2_label_counts": dict(Counter(row["stage2_label"] for row in rows)),
        "recovery_bucket_counts": dict(Counter(row["recovery_bucket"] for row in rows)),
        "significant_count": sum(1 for row in rows if row["significant"]),
        "supported_family_count": sum(1 for row in rows if row["family_guess"] in P3_SUPPORTED_FAMILIES),
    }


def main() -> int:
    args = parse_args()
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        print(f"Missing DSN env var: {args.dsn_env}", file=sys.stderr)
        return 2

    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            stage2 = resolve_stage2_run(cur, args.stage2_run_key)
            cur.execute(
                """
                SELECT
                    p.report_number,
                    p.candidate_seq,
                    p.claimed_contact_date,
                    p.stage2_label,
                    p.eligible,
                    p.significant,
                    p.event_family_final,
                    p.event_family_provisional,
                    p.match_status,
                    p.final_status,
                    p.meaningfulness_score,
                    p.measurability_score,
                    p.provenance_score,
                    p.time_window_start,
                    p.time_window_end,
                    p.target_type,
                    p.target_name,
                    p.actor_name,
                    p.claim_normalized,
                    p.source_quote,
                    p.review_notes,
                    p.stage2_meta
                FROM public.prediction_audit_predictions p
                WHERE p.last_stage2_run_id = %s
                  AND (
                    p.stage2_label IN ('eligible_prediction', 'significant_prediction')
                    OR (
                        p.stage2_label = 'prediction_but_not_measurable'
                        AND COALESCE(p.event_family_final, p.event_family_provisional) = ANY(%s)
                    )
                  )
                  AND p.match_status NOT IN ('exact_hit', 'near_hit', 'similar_only', 'miss')
                  AND COALESCE(p.final_status, 'pending') = 'pending'
                ORDER BY p.report_number, p.candidate_seq
                """,
                (stage2["id"], list(P3_SUPPORTED_FAMILIES)),
            )
            rows = [dict(row) for row in cur.fetchall()]

        for row in rows:
            row["family_guess"] = derive_family_guess(row)
            row["recovery_bucket"] = derive_recovery_bucket(row)
            row["recovery_rationale"] = derive_recovery_rationale(row)

        rows.sort(key=rank_key)
        for index, row in enumerate(rows, start=1):
            row["priority_rank"] = index

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "script_version": SCRIPT_VERSION,
            "stage2_run_key": stage2["run_key"],
            "queue_summary": summarize_rows(rows),
            "top_rows": [
                {
                    "priority_rank": row["priority_rank"],
                    "report_number": row["report_number"],
                    "candidate_seq": row["candidate_seq"],
                    "family_guess": row["family_guess"],
                    "stage2_label": row["stage2_label"],
                    "significant": row["significant"],
                    "match_status": row["match_status"],
                    "recovery_bucket": row["recovery_bucket"],
                }
                for row in rows[:15]
            ],
        }

        family_rows = []
        for family_guess in sorted({row["family_guess"] or "unknown" for row in rows}):
            family_subset = [row for row in rows if (row["family_guess"] or "unknown") == family_guess]
            family_rows.append(
                {
                    "family_guess": family_guess,
                    **summarize_rows(family_subset),
                    "top_priority_rank": min(row["priority_rank"] for row in family_subset),
                    "top_report_number": family_subset[0]["report_number"],
                    "top_candidate_seq": family_subset[0]["candidate_seq"],
                }
            )

        output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_ROOT / ("unscored-prediction-queue-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        output_dir.mkdir(parents=True, exist_ok=True)

        summary_path = output_dir / "summary.json"
        queue_path = output_dir / "queue.csv"
        family_summary_path = output_dir / "family_summary.csv"
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        write_csv(
            queue_path,
            rows,
            [
                "priority_rank",
                "report_number",
                "candidate_seq",
                "claimed_contact_date",
                "stage2_label",
                "eligible",
                "significant",
                "family_guess",
                "event_family_final",
                "event_family_provisional",
                "match_status",
                "final_status",
                "meaningfulness_score",
                "measurability_score",
                "provenance_score",
                "time_window_start",
                "time_window_end",
                "target_type",
                "target_name",
                "actor_name",
                "recovery_bucket",
                "recovery_rationale",
                "claim_normalized",
                "source_quote",
                "review_notes",
                "stage2_meta",
            ],
        )
        write_csv(
            family_summary_path,
            family_rows,
            [
                "family_guess",
                "prediction_count",
                "family_guess_counts",
                "stage2_label_counts",
                "recovery_bucket_counts",
                "significant_count",
                "supported_family_count",
                "top_priority_rank",
                "top_report_number",
                "top_candidate_seq",
            ],
        )

        print(json.dumps({"summary_path": str(summary_path), "queue_path": str(queue_path), "output_dir": str(output_dir), **summary}, indent=2))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

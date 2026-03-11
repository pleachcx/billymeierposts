#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from provenance_export_helpers import (
    annotate_predictions_with_provenance,
    fetch_report_provenance_rows,
    resolve_stage2_run,
)

SCRIPT_VERSION = "public_date_research_queue_v2"
OUTPUT_ROOT = Path("data") / "exports" / "provenance"
OBSERVED_PROBABILITY_FIELD = {
    "exact_hit": "p_exact_under_null",
    "near_hit": "p_near_under_null",
    "similar_only": "p_similar_under_null",
    "miss": "p_miss_under_null",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a ranked public-date research queue for publication conflicts.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key to scope predictions. Defaults to the latest completed Stage 2 run.")
    parser.add_argument("--output-dir", help="Output directory. Defaults to data/exports/provenance/public-date-research-queue-<timestamp>.")
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


def observed_probability(row: dict[str, Any]) -> float | None:
    field_name = OBSERVED_PROBABILITY_FIELD.get(row["match_status"])
    if not field_name:
        return None
    value = row.get(field_name)
    return float(value) if value is not None else None


def aggregate_probabilities(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [row["observed_probability_under_null"] for row in rows if row["observed_probability_under_null"] is not None and row["observed_probability_under_null"] > 0]
    if not values:
        return {"count": 0, "log10_sum": None, "ln_sum": None}
    return {
        "count": len(values),
        "log10_sum": round(sum(math.log10(value) for value in values), 6),
        "ln_sum": round(sum(math.log(value) for value in values), 6),
    }


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "prediction_count": len(rows),
        "family_counts": dict(Counter(row["event_family_final"] for row in rows)),
        "match_status_counts": dict(Counter(row["match_status"] for row in rows)),
        "current_public_source_tier_counts": dict(Counter(row["current_public_source_tier"] for row in rows)),
        "best_available_source_tier_counts": dict(Counter(row["best_available_source_tier"] for row in rows)),
        "conflict_gap_bucket_counts": dict(Counter(row["publication_conflict_gap_bucket"] for row in rows if row["publication_conflict_gap_bucket"])),
        "combined_observed_probability": aggregate_probabilities(rows),
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
                    p.event_family_final,
                    p.report_number,
                    p.candidate_seq,
                    p.claimed_contact_date,
                    p.earliest_provable_public_date,
                    p.public_date_basis,
                    p.provenance_score,
                    p.public_date_status,
                    p.public_date_reason,
                    p.match_status,
                    p.final_status,
                    p.claim_normalized,
                    p.source_quote,
                    p.p_exact_under_null,
                    p.p_near_under_null,
                    p.p_similar_under_null,
                    p.p_miss_under_null,
                    el.event_start_date,
                    el.event_title,
                    el.source_url AS event_source_url
                FROM public.prediction_audit_predictions p
                LEFT JOIN public.prediction_audit_event_ledger el ON el.id = p.best_event_ledger_id
                WHERE p.last_stage2_run_id = %s
                  AND p.final_status = 'included_in_statistics'
                  AND p.public_date_status = 'event_precedes_publication'
                  AND p.match_status IN ('exact_hit', 'near_hit', 'similar_only', 'miss')
                ORDER BY p.event_family_final, p.report_number, p.candidate_seq
                """,
                (stage2["id"],),
            )
            rows = [dict(row) for row in cur.fetchall()]
            provenance_rows = fetch_report_provenance_rows(cur, sorted({int(row["report_number"]) for row in rows}))

        for row in rows:
            row["observed_probability_under_null"] = observed_probability(row)
            probability = row["observed_probability_under_null"]
            row["observed_probability_log10"] = round(math.log10(probability), 6) if probability and probability > 0 else None
            row["surprisal_log10"] = round(-math.log10(probability), 6) if probability and probability > 0 else None
            if row["earliest_provable_public_date"] and row["event_start_date"]:
                lag_days = (row["event_start_date"] - row["earliest_provable_public_date"]).days
            else:
                lag_days = None
            row["publication_lag_days_vs_event"] = lag_days
        annotate_predictions_with_provenance(rows, provenance_rows)

        rows.sort(
            key=lambda row: (
                row["surprisal_log10"] is None,
                -(row["surprisal_log10"] or 0.0),
                row["event_family_final"],
                row["report_number"],
                row["candidate_seq"],
            )
        )
        for index, row in enumerate(rows, start=1):
            row["priority_rank"] = index

        family_rank = Counter()
        for row in rows:
            family_rank[row["event_family_final"]] += 1
            row["family_priority_rank"] = family_rank[row["event_family_final"]]

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "script_version": SCRIPT_VERSION,
            "stage2_run_key": stage2["run_key"],
            "queue_summary": summarize_rows(rows),
            "top_rows": [
                {
                    "priority_rank": row["priority_rank"],
                    "event_family_final": row["event_family_final"],
                    "report_number": row["report_number"],
                    "candidate_seq": row["candidate_seq"],
                    "match_status": row["match_status"],
                    "surprisal_log10": row["surprisal_log10"],
                    "publication_lag_days_vs_event": row["publication_lag_days_vs_event"],
                    "publication_conflict_gap_bucket": row["publication_conflict_gap_bucket"],
                    "current_public_source_tier": row["current_public_source_tier"],
                }
                for row in rows[:10]
            ],
        }

        output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_ROOT / ("public-date-research-queue-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        output_dir.mkdir(parents=True, exist_ok=True)

        summary_path = output_dir / "summary.json"
        queue_path = output_dir / "research_queue.csv"
        family_summary_path = output_dir / "family_summary.csv"
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        write_csv(
            queue_path,
            rows,
            [
                "priority_rank",
                "family_priority_rank",
                "event_family_final",
                "report_number",
                "candidate_seq",
                "match_status",
                "observed_probability_under_null",
                "observed_probability_log10",
                "surprisal_log10",
                "claimed_contact_date",
                "earliest_provable_public_date",
                "publication_lag_days_vs_event",
                "publication_lag_days_vs_primary_source",
                "publication_conflict_gap_bucket",
                "public_date_basis",
                "provenance_score",
                "current_public_evidence_kind",
                "current_public_source_tier",
                "current_public_source_bucket",
                "current_public_source_label",
                "current_public_source_url",
                "best_available_source_tier",
                "best_available_source_bucket",
                "best_available_source_label",
                "best_available_source_url",
                "earliest_primary_source_date",
                "earliest_mirror_source_date",
                "earliest_secondary_source_date",
                "public_date_reason",
                "event_start_date",
                "event_title",
                "event_source_url",
                "claim_normalized",
                "source_quote",
            ],
        )
        family_rows = []
        for family in sorted({row["event_family_final"] for row in rows}):
            family_subset = [row for row in rows if row["event_family_final"] == family]
            family_rows.append(
                {
                    "event_family_final": family,
                    **summarize_rows(family_subset),
                    "top_priority_rank": min(row["priority_rank"] for row in family_subset),
                    "top_report_number": family_subset[0]["report_number"],
                    "top_candidate_seq": family_subset[0]["candidate_seq"],
                }
            )
        write_csv(
            family_summary_path,
            family_rows,
            [
                "event_family_final",
                "prediction_count",
                "family_counts",
                "match_status_counts",
                "combined_observed_probability",
                "top_priority_rank",
                "top_report_number",
                "top_candidate_seq",
            ],
        )

        print(
            json.dumps(
                {
                    "summary_path": str(summary_path),
                    "queue_path": str(queue_path),
                    "family_summary_path": str(family_summary_path),
                    **summary,
                },
                indent=2,
            )
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

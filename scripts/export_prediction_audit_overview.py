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


SCRIPT_VERSION = "prediction_audit_overview_v1"
OUTPUT_ROOT = Path("data") / "exports" / "overview"
OBSERVED_PROBABILITY_FIELD = {
    "exact_hit": "p_exact_under_null",
    "near_hit": "p_near_under_null",
    "similar_only": "p_similar_under_null",
    "miss": "p_miss_under_null",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a single overview of current prediction-audit status.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key to scope the overview. Defaults to the latest completed Stage 2 run.")
    parser.add_argument("--output-dir", help="Output directory. Defaults to data/exports/overview/prediction-audit-overview-<timestamp>.")
    return parser.parse_args()


def observed_probability(row: dict[str, Any]) -> float | None:
    field_name = OBSERVED_PROBABILITY_FIELD.get(row["match_status"])
    if not field_name:
        return None
    value = row.get(field_name)
    return float(value) if value is not None else None


def aggregate_probability(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [row["observed_probability_under_null"] for row in rows if row["observed_probability_under_null"] is not None and row["observed_probability_under_null"] > 0]
    if not values:
        return {"count": 0, "log10_sum": None, "ln_sum": None}
    return {
        "count": len(values),
        "log10_sum": round(sum(math.log10(v) for v in values), 6),
        "ln_sum": round(sum(math.log(v) for v in values), 6),
    }


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
                    parse_run_id,
                    COUNT(*) AS candidate_count,
                    COUNT(*) FILTER (WHERE stage2_label = 'eligible_prediction') AS eligible_count,
                    COUNT(*) FILTER (WHERE stage2_label = 'significant_prediction') AS significant_count
                FROM public.prediction_audit_predictions
                WHERE last_stage2_run_id = %s
                GROUP BY parse_run_id
                """,
                (stage2["id"],),
            )
            parse_counts = dict(cur.fetchone() or {})

            cur.execute(
                """
                SELECT
                    p.event_family_final,
                    p.report_number,
                    p.candidate_seq,
                    p.stage2_label,
                    p.match_status,
                    p.final_status,
                    p.public_date_status,
                    p.earliest_provable_public_date,
                    p.public_date_basis,
                    p.claim_normalized,
                    p.p_exact_under_null,
                    p.p_near_under_null,
                    p.p_similar_under_null,
                    p.p_miss_under_null,
                    el.event_start_date
                FROM public.prediction_audit_predictions p
                LEFT JOIN public.prediction_audit_event_ledger el ON el.id = p.best_event_ledger_id
                WHERE p.last_stage2_run_id = %s
                  AND p.final_status = 'included_in_statistics'
                  AND p.match_status IN ('exact_hit', 'near_hit', 'similar_only', 'miss')
                ORDER BY p.event_family_final, p.report_number, p.candidate_seq
                """,
                (stage2["id"],),
            )
            scored_rows = [dict(row) for row in cur.fetchall()]
            provenance_rows = fetch_report_provenance_rows(cur, sorted({int(row["report_number"]) for row in scored_rows}))

        for row in scored_rows:
            row["observed_probability_under_null"] = observed_probability(row)
        annotate_predictions_with_provenance(scored_rows, provenance_rows)

        claimed_hits = [row for row in scored_rows if row["match_status"] in {"exact_hit", "near_hit", "similar_only"}]
        public_clean_rows = [row for row in scored_rows if row["public_date_status"] == "public_date_ok"]
        public_clean_hits = [row for row in public_clean_rows if row["match_status"] in {"exact_hit", "near_hit", "similar_only"}]

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "script_version": SCRIPT_VERSION,
            "stage2_run_key": stage2["run_key"],
            "parse_counts": {
                "candidate_count": int(parse_counts.get("candidate_count") or 0),
                "eligible_count": int(parse_counts.get("eligible_count") or 0),
                "significant_count": int(parse_counts.get("significant_count") or 0),
            },
            "scored_counts": {
                "included_scored_count": len(scored_rows),
                "claimed_hit_count": len(claimed_hits),
                "claimed_exact_hit_count": sum(1 for row in scored_rows if row["match_status"] == "exact_hit"),
                "public_date_clean_count": len(public_clean_rows),
                "public_date_clean_hit_count": len(public_clean_hits),
                "public_date_clean_exact_hit_count": sum(1 for row in public_clean_hits if row["match_status"] == "exact_hit"),
            },
            "match_status_counts": dict(Counter(row["match_status"] for row in scored_rows)),
            "public_date_status_counts": dict(Counter(row["public_date_status"] for row in scored_rows)),
            "family_counts": dict(Counter(row["event_family_final"] for row in scored_rows)),
            "current_public_source_tier_counts": dict(Counter(row["current_public_source_tier"] for row in scored_rows)),
            "best_available_source_tier_counts": dict(Counter(row["best_available_source_tier"] for row in scored_rows)),
            "combined_observed_probability": {
                "claimed_date_baseline": aggregate_probability(scored_rows),
                "public_date_clean": aggregate_probability(public_clean_rows),
                "public_date_currently_unrescued": aggregate_probability(
                    [row for row in scored_rows if row["public_date_status"] == "event_precedes_publication"]
                ),
            },
        }

        family_rows: list[dict[str, Any]] = []
        for family in sorted({row["event_family_final"] for row in scored_rows}):
            family_subset = [row for row in scored_rows if row["event_family_final"] == family]
            family_rows.append(
                {
                    "event_family_final": family,
                    "included_scored_count": len(family_subset),
                    "claimed_hit_count": sum(1 for row in family_subset if row["match_status"] in {"exact_hit", "near_hit", "similar_only"}),
                    "claimed_exact_hit_count": sum(1 for row in family_subset if row["match_status"] == "exact_hit"),
                    "public_date_clean_count": sum(1 for row in family_subset if row["public_date_status"] == "public_date_ok"),
                    "public_date_clean_hit_count": sum(
                        1 for row in family_subset if row["public_date_status"] == "public_date_ok" and row["match_status"] in {"exact_hit", "near_hit", "similar_only"}
                    ),
                    "public_date_clean_exact_hit_count": sum(
                        1 for row in family_subset if row["public_date_status"] == "public_date_ok" and row["match_status"] == "exact_hit"
                    ),
                    "match_status_counts": dict(Counter(row["match_status"] for row in family_subset)),
                    "public_date_status_counts": dict(Counter(row["public_date_status"] for row in family_subset)),
                    "best_available_source_tier_counts": dict(Counter(row["best_available_source_tier"] for row in family_subset)),
                }
            )

        output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_ROOT / ("prediction-audit-overview-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        output_dir.mkdir(parents=True, exist_ok=True)
        summary_path = output_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        write_csv(
            output_dir / "family_summary.csv",
            family_rows,
            [
                "event_family_final",
                "included_scored_count",
                "claimed_hit_count",
                "claimed_exact_hit_count",
                "public_date_clean_count",
                "public_date_clean_hit_count",
                "public_date_clean_exact_hit_count",
                "match_status_counts",
                "public_date_status_counts",
                "best_available_source_tier_counts",
            ],
        )

        print(json.dumps({"summary_path": str(summary_path), "output_dir": str(output_dir), **summary}, indent=2))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

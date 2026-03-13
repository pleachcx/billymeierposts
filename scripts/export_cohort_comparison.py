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
    derive_public_date_cohort_status,
    fetch_report_provenance_rows,
    resolve_stage2_run,
)

SCRIPT_VERSION = "cohort_comparison_v4"
OUTPUT_ROOT = Path("data") / "exports" / "provenance"
OBSERVED_PROBABILITY_FIELD = {
    "exact_hit": "p_exact_under_null",
    "near_hit": "p_near_under_null",
    "similar_only": "p_similar_under_null",
    "miss": "p_miss_under_null",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export claimed-date vs public-date cohort comparisons.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key to scope predictions. Defaults to the latest completed Stage 2 run.")
    parser.add_argument("--output-dir", help="Output directory. Defaults to data/exports/provenance/cohort-comparison-<timestamp>.")
    return parser.parse_args()


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


def json_safe_counter(counter: Counter[Any]) -> dict[str, int]:
    return {("(none)" if key is None else str(key)): value for key, value in counter.items()}


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "prediction_count": len(rows),
        "family_counts": json_safe_counter(Counter(row["event_family_final"] for row in rows)),
        "match_status_counts": json_safe_counter(Counter(row["match_status"] for row in rows)),
        "public_date_status_counts": json_safe_counter(Counter(row["public_date_status"] for row in rows)),
        "public_date_cohort_status_counts": json_safe_counter(Counter(row["public_date_cohort_status"] for row in rows)),
        "current_public_source_tier_counts": json_safe_counter(Counter(row["current_public_source_tier"] for row in rows)),
        "best_available_source_tier_counts": json_safe_counter(Counter(row["best_available_source_tier"] for row in rows)),
        "current_public_source_bucket_counts": json_safe_counter(Counter(row["current_public_source_bucket"] for row in rows)),
        "combined_observed_probability": aggregate_probabilities(rows),
    }


def summarize_by_family(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    families = sorted({row["event_family_final"] for row in rows})
    return {
        family: summarize([row for row in rows if row["event_family_final"] == family])
        for family in families
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
                    p.match_status,
                    p.final_status,
                    p.public_date_status,
                    p.public_date_cohort_status,
                    p.public_date_cohort_reason,
                    p.claimed_contact_date,
                    p.earliest_provable_public_date,
                    p.public_date_basis,
                    p.claim_normalized,
                    p.p_exact_under_null,
                    p.p_near_under_null,
                    p.p_similar_under_null,
                    p.p_miss_under_null
                FROM public.prediction_audit_predictions p
                WHERE p.last_stage2_run_id = %s
                  AND p.final_status = 'included_in_statistics'
                  AND p.match_status IN ('exact_hit', 'near_hit', 'similar_only', 'miss')
                ORDER BY p.event_family_final, p.report_number, p.candidate_seq
                """,
                (stage2["id"],),
            )
            rows = [dict(row) for row in cur.fetchall()]
            provenance_rows = fetch_report_provenance_rows(cur, sorted({int(row["report_number"]) for row in rows}))

        for row in rows:
            row["observed_probability_under_null"] = observed_probability(row)
            row["public_date_cohort_status"] = row.get("public_date_cohort_status") or derive_public_date_cohort_status(row.get("public_date_status"))
        annotate_predictions_with_provenance(rows, provenance_rows)

        cohorts = {
            "claimed_date_baseline": rows,
            "public_date_not_disproven": [row for row in rows if row["public_date_status"] != "event_precedes_publication"],
            "public_date_strict_clean": [row for row in rows if row["public_date_status"] == "public_date_ok"],
            "public_date_excluded": [row for row in rows if row["public_date_status"] == "event_precedes_publication"],
            "public_date_currently_unrescued": [row for row in rows if row["public_date_cohort_status"] == "excluded_currently_unrescued"],
            "public_date_pending_evidence": [row for row in rows if row["public_date_cohort_status"] == "pending_more_public_evidence"],
            "public_date_primary_supported_clean": [
                row
                for row in rows
                if row["public_date_cohort_status"] == "included_in_current_public_date_cohort" and row["best_available_source_bucket"] == "primary_official"
            ],
            "public_date_mirror_supported_clean": [
                row
                for row in rows
                if row["public_date_cohort_status"] == "included_in_current_public_date_cohort" and row["best_available_source_bucket"] == "mirror"
            ],
        }

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "script_version": SCRIPT_VERSION,
            "stage2_run_key": stage2["run_key"],
            "cohorts": {name: summarize(cohort_rows) for name, cohort_rows in cohorts.items()},
            "cohorts_by_family": {name: summarize_by_family(cohort_rows) for name, cohort_rows in cohorts.items()},
        }

        output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_ROOT / ("cohort-comparison-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        output_dir.mkdir(parents=True, exist_ok=True)

        summary_path = output_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        for name, cohort_rows in cohorts.items():
            write_csv(
                output_dir / f"{name}.csv",
                cohort_rows,
                [
                    "event_family_final",
                    "report_number",
                    "candidate_seq",
                    "match_status",
                    "final_status",
                    "public_date_status",
                    "public_date_cohort_status",
                    "public_date_cohort_reason",
                    "claimed_contact_date",
                    "earliest_provable_public_date",
                    "public_date_basis",
                    "current_public_source_tier",
                    "current_public_source_bucket",
                    "best_available_source_tier",
                    "best_available_source_bucket",
                    "earliest_primary_source_date",
                    "earliest_mirror_source_date",
                    "observed_probability_under_null",
                    "claim_normalized",
                ],
            )

        family_rows: list[dict[str, Any]] = []
        for cohort_name, family_summary in summary["cohorts_by_family"].items():
            for family, values in family_summary.items():
                family_rows.append(
                    {
                        "cohort_name": cohort_name,
                        "event_family_final": family,
                        "prediction_count": values["prediction_count"],
                        "family_counts": values["family_counts"],
                        "match_status_counts": values["match_status_counts"],
                        "public_date_status_counts": values["public_date_status_counts"],
                        "combined_observed_probability": values["combined_observed_probability"],
                    }
                )
        write_csv(
            output_dir / "cohort_family_summary.csv",
            family_rows,
            [
                "cohort_name",
                "event_family_final",
                "prediction_count",
                "family_counts",
                "match_status_counts",
                "public_date_status_counts",
                "combined_observed_probability",
            ],
        )

        print(
            json.dumps(
                {
                    "summary_path": str(summary_path),
                    "output_dir": str(output_dir),
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

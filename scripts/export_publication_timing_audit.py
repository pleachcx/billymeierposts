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


SCRIPT_VERSION = "publication_timing_audit_v1"
OUTPUT_ROOT = Path("data") / "exports" / "provenance"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a cross-family audit of event timing versus earliest provable public dates.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", default="stage2-20260310T232950Z", help="Stage 2 run key to scope predictions.")
    parser.add_argument("--output-dir", help="Output directory. Defaults to data/exports/provenance/publication-timing-audit-<timestamp>.")
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


def main() -> int:
    args = parse_args()
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        print(f"Missing DSN env var: {args.dsn_env}", file=sys.stderr)
        return 2

    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM public.prediction_audit_runs
                WHERE stage = 'stage2_eligibility' AND run_key = %s
                """,
                (args.stage2_run_key,),
            )
            stage2 = cur.fetchone()
            if not stage2:
                raise RuntimeError(f"Missing Stage 2 run {args.stage2_run_key}.")

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
                    p.match_status,
                    p.final_status,
                    p.claim_normalized,
                    el.event_start_date,
                    el.event_title,
                    CASE
                        WHEN p.earliest_provable_public_date IS NOT NULL AND el.event_start_date IS NOT NULL
                        THEN (el.event_start_date - p.earliest_provable_public_date)
                        ELSE NULL
                    END AS publication_lag_days_vs_event
                FROM public.prediction_audit_predictions p
                LEFT JOIN public.prediction_audit_event_ledger el ON el.id = p.best_event_ledger_id
                WHERE p.last_stage2_run_id = %s
                  AND p.final_status = 'included_in_statistics'
                  AND p.match_status IN ('exact_hit', 'near_hit', 'similar_only', 'miss')
                ORDER BY p.event_family_final, p.report_number, p.candidate_seq
                """,
                (stage2["id"],),
            )
            rows = [dict(row) for row in cur.fetchall()]

        for row in rows:
            lag = row.get("publication_lag_days_vs_event")
            row["observed_event_before_publication"] = lag is not None and lag < 0

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "script_version": SCRIPT_VERSION,
            "stage2_run_key": args.stage2_run_key,
            "included_scored_prediction_count": len(rows),
            "family_counts": dict(Counter(row["event_family_final"] for row in rows)),
            "match_status_counts": dict(Counter(row["match_status"] for row in rows)),
            "earliest_public_date_populated_count": sum(1 for row in rows if row["earliest_provable_public_date"] is not None),
            "observed_event_before_publication_count": sum(1 for row in rows if row["observed_event_before_publication"]),
            "observed_event_before_publication_by_family": dict(
                Counter(row["event_family_final"] for row in rows if row["observed_event_before_publication"])
            ),
        }

        default_dir = OUTPUT_ROOT / ("publication-timing-audit-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        output_dir = Path(args.output_dir) if args.output_dir else default_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        summary_path = output_dir / "summary.json"
        audit_path = output_dir / "timing_audit.csv"
        flagged_path = output_dir / "timing_conflicts.csv"
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        fieldnames = [
            "event_family_final",
            "report_number",
            "candidate_seq",
            "claimed_contact_date",
            "earliest_provable_public_date",
            "public_date_basis",
            "provenance_score",
            "match_status",
            "final_status",
            "event_start_date",
            "publication_lag_days_vs_event",
            "observed_event_before_publication",
            "event_title",
            "claim_normalized",
        ]
        write_csv(audit_path, rows, fieldnames)
        write_csv(flagged_path, [row for row in rows if row["observed_event_before_publication"]], fieldnames)

        print(
            json.dumps(
                {
                    "summary_path": str(summary_path),
                    "audit_path": str(audit_path),
                    "flagged_path": str(flagged_path),
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

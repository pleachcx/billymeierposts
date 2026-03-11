#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor


SCRIPT_VERSION = "epidemic_export_v1"
OUTPUT_ROOT = Path("data") / "exports" / "epidemic"


@dataclass
class RunSet:
    stage2_run_id: int
    stage2_run_key: str
    stage3_run_id: int | None
    stage3_run_key: str | None
    stage4_run_id: int
    stage4_run_key: str
    stage7_run_id: int | None
    stage7_run_key: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export epidemic prediction analysis artifacts.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage4-run-key", help="Stage 4 epidemic run key. Defaults to latest completed Stage 4 epidemic run.")
    parser.add_argument("--stage2-run-key", help="Optional Stage 2 run key override.")
    parser.add_argument("--output-dir", help="Output directory. Defaults to data/exports/epidemic/<stage4_run_key>.")
    return parser.parse_args()


def fetch_run(cur, stage: str, run_key: str | None, family: str | None = None) -> dict[str, Any]:
    family_clause = "AND run_meta->>'family' = %s" if family else ""
    if run_key:
        sql = f"""
            SELECT id, run_key, source_filter, run_meta
            FROM public.prediction_audit_runs
            WHERE stage = %s AND run_key = %s {family_clause}
        """
        params: list[Any] = [stage, run_key]
        if family:
            params.append(family)
        cur.execute(sql, params)
    else:
        sql = f"""
            SELECT id, run_key, source_filter, run_meta
            FROM public.prediction_audit_runs
            WHERE stage = %s AND status = 'completed' {family_clause}
            ORDER BY created_at DESC
            LIMIT 1
        """
        params = [stage]
        if family:
            params.append(family)
        cur.execute(sql, params)
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"No completed run found for stage {stage}.")
    return row


def resolve_run_set(cur, args: argparse.Namespace) -> RunSet:
    stage4 = fetch_run(cur, "stage4_match_scoring", args.stage4_run_key, family="epidemic")
    stage4_filter = stage4["source_filter"] or {}

    stage2_run_key = args.stage2_run_key or stage4_filter.get("stage2_run_key")
    if not stage2_run_key:
        raise RuntimeError("Could not infer Stage 2 run key from Stage 4 epidemic metadata.")
    stage2 = fetch_run(cur, "stage2_eligibility", stage2_run_key)

    stage3_run_key = stage4_filter.get("stage3_run_key")
    stage3 = fetch_run(cur, "stage3_event_ledger", stage3_run_key) if stage3_run_key else None

    cur.execute(
        """
        SELECT id, run_key
        FROM public.prediction_audit_runs
        WHERE stage = 'stage7_final_adjudication'
          AND status = 'completed'
          AND source_filter->>'stage4_run_key' = %s
          AND source_filter->>'family' = 'epidemic'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (stage4["run_key"],),
    )
    stage7 = cur.fetchone()

    return RunSet(
        stage2_run_id=stage2["id"],
        stage2_run_key=stage2["run_key"],
        stage3_run_id=stage3["id"] if stage3 else None,
        stage3_run_key=stage3["run_key"] if stage3 else None,
        stage4_run_id=stage4["id"],
        stage4_run_key=stage4["run_key"],
        stage7_run_id=stage7["id"] if stage7 else None,
        stage7_run_key=stage7["run_key"] if stage7 else None,
    )


def load_predictions(cur, runs: RunSet) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            p.id AS prediction_id,
            p.report_number,
            p.candidate_seq,
            p.stage2_label,
            p.significant,
            p.claimed_contact_date,
            p.claim_normalized,
            p.source_quote,
            p.time_window_start,
            p.time_window_end,
            p.target_name,
            p.target_type,
            p.match_status,
            p.final_status,
            p.final_reason,
            p.final_meta,
            mr.rationale AS review_rationale,
            mr.review_meta,
            el.external_event_id,
            el.event_title,
            el.event_start_date,
            el.location_name AS observed_location_name,
            el.severity_band AS observed_event_type,
            el.time_delta_days,
            el.raw_event->>'effective_window_start' AS effective_window_start,
            el.raw_event->>'effective_window_end' AS effective_window_end,
            el.source_name,
            el.source_url
        FROM public.prediction_audit_match_reviews mr
        JOIN public.prediction_audit_predictions p ON p.id = mr.prediction_id
        LEFT JOIN public.prediction_audit_event_ledger el ON el.id = mr.event_ledger_id
        WHERE mr.review_run_id = %s
        ORDER BY p.report_number, p.candidate_seq
        """,
        (runs.stage4_run_id,),
    )
    return [dict(row) for row in cur.fetchall()]


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
            runs = resolve_run_set(cur, args)
            predictions = load_predictions(cur, runs)

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "script_version": SCRIPT_VERSION,
            "run_keys": {
                "stage2_run_key": runs.stage2_run_key,
                "stage3_run_key": runs.stage3_run_key,
                "stage4_run_key": runs.stage4_run_key,
                "stage7_run_key": runs.stage7_run_key,
            },
            "scoped_prediction_count": len(predictions),
            "match_status_counts": dict(Counter(row["match_status"] for row in predictions)),
            "final_status_counts": dict(Counter(row["final_status"] for row in predictions)),
            "significant_count": sum(1 for row in predictions if row["significant"]),
            "named_target_count": sum(1 for row in predictions if row["target_name"]),
        }

        output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_ROOT / runs.stage4_run_key
        output_dir.mkdir(parents=True, exist_ok=True)

        summary_path = output_dir / "summary.json"
        predictions_path = output_dir / "predictions.csv"
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        write_csv(
            predictions_path,
            predictions,
            [
                "prediction_id",
                "report_number",
                "candidate_seq",
                "stage2_label",
                "significant",
                "claimed_contact_date",
                "claim_normalized",
                "source_quote",
                "time_window_start",
                "time_window_end",
                "effective_window_start",
                "effective_window_end",
                "target_name",
                "target_type",
                "match_status",
                "final_status",
                "final_reason",
                "final_meta",
                "review_rationale",
                "review_meta",
                "external_event_id",
                "event_title",
                "event_start_date",
                "observed_location_name",
                "observed_event_type",
                "time_delta_days",
                "source_name",
                "source_url",
            ],
        )

        print(
            json.dumps(
                {
                    "summary_path": str(summary_path),
                    "predictions_path": str(predictions_path),
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

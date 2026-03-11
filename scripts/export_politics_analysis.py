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


SCRIPT_VERSION = "politics_export_v1"
OUTPUT_ROOT = Path("data") / "exports" / "politics"


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
    parser = argparse.ArgumentParser(description="Export politics prediction analysis artifacts.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage4-run-key", help="Stage 4 politics run key. Defaults to latest completed Stage 4 politics run.")
    parser.add_argument("--stage2-run-key", help="Optional Stage 2 run key override.")
    parser.add_argument("--output-dir", help="Output directory. Defaults to data/exports/politics/<stage4_run_key>.")
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
    stage4 = fetch_run(cur, "stage4_match_scoring", args.stage4_run_key, family="politics_election")
    stage4_filter = stage4["source_filter"] or {}
    stage2_run_key = args.stage2_run_key or stage4_filter.get("stage2_run_key")
    if not stage2_run_key:
        raise RuntimeError("Could not infer Stage 2 run key from Stage 4 politics metadata.")
    stage2 = fetch_run(cur, "stage2_eligibility", stage2_run_key)
    stage3_run_key = stage4_filter.get("stage3_run_key")
    stage3 = fetch_run(cur, "stage3_event_ledger", stage3_run_key, family="politics_election") if stage3_run_key else None

    cur.execute(
        """
        SELECT id, run_key
        FROM public.prediction_audit_runs
        WHERE stage = 'stage7_final_adjudication'
          AND status = 'completed'
          AND source_filter->>'stage4_run_key' = %s
          AND source_filter->>'family' = 'politics_election'
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
            p.event_family_final,
            p.stage2_label,
            p.significant,
            p.claimed_contact_date,
            p.earliest_provable_public_date,
            p.public_date_basis,
            p.provenance_score,
            p.public_date_status,
            p.public_date_reason,
            p.claim_normalized,
            p.source_quote,
            p.time_window_start,
            p.time_window_end,
            p.target_name,
            p.target_type,
            p.match_status,
            p.final_status,
            p.final_reason,
            mr.rationale AS review_rationale,
            mr.review_meta,
            el.external_event_id,
            el.event_title,
            el.event_start_date,
            el.location_name AS observed_location_name,
            el.severity_band AS observed_event_type,
            el.time_delta_days,
            el.source_name,
            el.source_url,
            el.raw_event
        FROM public.prediction_audit_match_reviews mr
        JOIN public.prediction_audit_predictions p ON p.id = mr.prediction_id
        LEFT JOIN public.prediction_audit_event_ledger el ON el.id = mr.event_ledger_id
        WHERE mr.review_run_id = %s
        ORDER BY p.report_number, p.candidate_seq
        """,
        (runs.stage4_run_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def annotate_publication_timing(row: dict[str, Any]) -> None:
    public_date = row.get("earliest_provable_public_date")
    event_date = row.get("event_start_date")
    if not public_date or not event_date:
        row["observed_event_before_publication"] = None
        row["publication_lag_days_vs_event"] = None
        return
    lag_days = (event_date - public_date).days
    row["publication_lag_days_vs_event"] = lag_days
    row["observed_event_before_publication"] = lag_days < 0


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

        for prediction in predictions:
            annotate_publication_timing(prediction)

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
            "earliest_public_date_populated_count": sum(1 for row in predictions if row["earliest_provable_public_date"] is not None),
            "observed_event_before_publication_count": sum(1 for row in predictions if row["observed_event_before_publication"] is True),
            "public_date_status_counts": dict(Counter(row["public_date_status"] for row in predictions)),
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
                "event_family_final",
                "stage2_label",
                "significant",
                "claimed_contact_date",
                "earliest_provable_public_date",
                "public_date_basis",
                "provenance_score",
                "public_date_status",
                "public_date_reason",
                "claim_normalized",
                "source_quote",
                "time_window_start",
                "time_window_end",
                "target_name",
                "target_type",
                "match_status",
                "final_status",
                "final_reason",
                "observed_event_before_publication",
                "publication_lag_days_vs_event",
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
                "raw_event",
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

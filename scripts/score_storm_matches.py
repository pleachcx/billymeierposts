#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import Json, execute_batch


SCRIPT_VERSION = "stage4_storm_tornado_catalog_v1"
REVIEWER = "script:stage4_storm_tornado_catalog_v1"
REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class Prediction:
    prediction_id: int
    report_number: int
    candidate_seq: int
    claim_normalized: str


@dataclass
class LedgerRow:
    event_ledger_id: int
    prediction_id: int
    event_start_date: Any
    event_title: str | None
    location_name: str | None
    severity_band: str | None
    exact_band: bool
    near_band: bool
    log_only_band: bool
    time_delta_days: int | None
    source_url: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 4 storm match scorer for the curated tornado catalog slice.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key. Defaults to latest completed Stage 2 run.")
    parser.add_argument("--stage3-run-key", help="Stage 3 storm run key. Defaults to latest completed Stage 3 storm run.")
    parser.add_argument("--run-key", help="Unique Stage 4 storm run key. Defaults to a timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument(
        "--overrides-path",
        default=str(REPO_ROOT / "data" / "storm_prediction_overrides.json"),
        help="Path to storm prediction override JSON keyed by report_number:candidate_seq.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Score without writing match reviews.")
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage4-storm-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def fetch_run(cur, stage: str, run_key: str | None, family: str | None = None) -> tuple[int, str]:
    family_clause = "AND run_meta->>'family' = %s" if family else ""
    if run_key:
        sql = f"""
            SELECT id, run_key
            FROM public.prediction_audit_runs
            WHERE stage = %s AND run_key = %s {family_clause}
        """
        params: list[Any] = [stage, run_key]
        if family:
            params.append(family)
        cur.execute(sql, params)
    else:
        sql = f"""
            SELECT id, run_key
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
    return row[0], row[1]


def insert_run(cur, run_key: str, source_filter: dict[str, Any], notes: str | None) -> int:
    cur.execute(
        """
        INSERT INTO public.prediction_audit_runs (
            run_key,
            stage,
            status,
            parser_version,
            prompt_version,
            source_corpus,
            source_filter,
            notes,
            run_meta,
            started_at
        )
        VALUES (%s, 'stage4_match_scoring', 'running', %s, %s, %s, %s, %s, %s, now())
        RETURNING id
        """,
        (
            run_key,
            SCRIPT_VERSION,
            "none",
            "public.prediction_audit_event_ledger",
            Json(source_filter),
            notes or None,
            Json({"family": "storm", "scope": "tornado_catalog_v1"}),
        ),
    )
    return cur.fetchone()[0]


def update_run(cur, run_id: int, status: str, run_meta: dict[str, Any]) -> None:
    cur.execute(
        """
        UPDATE public.prediction_audit_runs
        SET status = %s,
            completed_at = CASE WHEN %s IN ('completed', 'failed', 'abandoned') THEN now() ELSE completed_at END,
            run_meta = COALESCE(run_meta, '{}'::jsonb) || %s::jsonb
        WHERE id = %s
        """,
        (status, status, json.dumps(run_meta), run_id),
    )


def load_overrides(path: str) -> dict[str, dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def fetch_predictions(cur, stage2_run_id: int, override_keys: list[str]) -> list[Prediction]:
    pairs = [(int(key.split(":")[0]), int(key.split(":")[1])) for key in override_keys]
    cur.execute(
        """
        SELECT id, report_number, candidate_seq, claim_normalized
        FROM public.prediction_audit_predictions
        WHERE last_stage2_run_id = %s
          AND stage2_label IN ('eligible_prediction', 'significant_prediction')
          AND (report_number, candidate_seq) IN %s
        ORDER BY report_number, candidate_seq
        """,
        (stage2_run_id, tuple(pairs)),
    )
    return [Prediction(*row) for row in cur.fetchall()]


def fetch_ledger_rows(cur, run_id: int) -> dict[int, list[LedgerRow]]:
    cur.execute(
        """
        SELECT
            id,
            prediction_id,
            event_start_date,
            event_title,
            location_name,
            severity_band,
            exact_band,
            near_band,
            log_only_band,
            time_delta_days,
            source_url
        FROM public.prediction_audit_event_ledger
        WHERE ledger_run_id = %s
        ORDER BY prediction_id, exact_band DESC, near_band DESC, log_only_band DESC, time_delta_days NULLS LAST
        """,
        (run_id,),
    )
    by_prediction: dict[int, list[LedgerRow]] = {}
    for row in cur.fetchall():
        by_prediction.setdefault(row[1], []).append(LedgerRow(*row))
    return by_prediction


def best_row(rows: list[LedgerRow]) -> LedgerRow | None:
    if not rows:
        return None
    return sorted(
        rows,
        key=lambda row: (
            0 if row.exact_band else 1 if row.near_band else 2,
            row.time_delta_days if row.time_delta_days is not None else 999999,
            row.event_start_date,
        ),
    )[0]


def classify(rows: list[LedgerRow]) -> tuple[str, int | None, str, dict[str, Any]]:
    exact = [row for row in rows if row.exact_band]
    near = [row for row in rows if row.near_band]
    log_only = [row for row in rows if row.log_only_band]
    meta = {
        "script_version": SCRIPT_VERSION,
        "candidate_event_count": len(rows),
        "exact_candidate_count": len(exact),
        "near_candidate_count": len(near),
        "log_candidate_count": len(log_only),
    }

    if exact:
        row = best_row(exact)
        return "exact_hit", row.event_ledger_id, "Exact storm event found in curated official catalog.", meta
    if near:
        row = best_row(near)
        return "near_hit", row.event_ledger_id, "Near-window storm event found in curated official catalog.", meta
    if log_only:
        row = best_row(log_only)
        return "similar_only", row.event_ledger_id, "Same storm location/event type found, but timing only partially matches.", meta
    return "miss", None, "No official catalog event matched the scoped storm prediction.", meta


def demote_existing(cur, prediction_ids: list[int]) -> None:
    if not prediction_ids:
        return
    cur.execute(
        """
        UPDATE public.prediction_audit_match_reviews
        SET is_primary = false
        WHERE prediction_id = ANY(%s) AND is_primary = true
        """,
        (prediction_ids,),
    )


def update_predictions(cur, decisions: list[tuple[str, int | None, int]]) -> None:
    execute_batch(
        cur,
        """
        UPDATE public.prediction_audit_predictions
        SET match_status = %s,
            best_event_ledger_id = %s
        WHERE id = %s
        """,
        decisions,
        page_size=100,
    )


def insert_reviews(cur, run_id: int, decisions: list[tuple[int, str, int | None, str, dict[str, Any]]]) -> None:
    execute_batch(
        cur,
        """
        INSERT INTO public.prediction_audit_match_reviews (
            prediction_id,
            event_ledger_id,
            review_run_id,
            match_status,
            is_primary,
            reviewer,
            confidence,
            rationale,
            review_meta
        )
        VALUES (%s, %s, %s, %s, true, %s, %s, %s, %s)
        """,
        [
            (
                prediction_id,
                event_ledger_id,
                run_id,
                match_status,
                REVIEWER,
                0.97,
                rationale,
                Json(review_meta),
            )
            for prediction_id, match_status, event_ledger_id, rationale, review_meta in decisions
        ],
        page_size=100,
    )


def main() -> int:
    args = parse_args()
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        print(f"Missing DSN env var: {args.dsn_env}", file=sys.stderr)
        return 2

    overrides = load_overrides(args.overrides_path)
    run_key = args.run_key or generate_run_key()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    run_id: int | None = None

    try:
        with conn.cursor() as cur:
            stage2_run_id, resolved_stage2_run_key = fetch_run(cur, "stage2_eligibility", args.stage2_run_key)
            stage3_run_id, resolved_stage3_run_key = fetch_run(cur, "stage3_event_ledger", args.stage3_run_key, family="storm")
            source_filter = {
                "stage2_run_key": resolved_stage2_run_key,
                "stage3_run_key": resolved_stage3_run_key,
                "family": "storm",
                "override_keys": sorted(overrides.keys()),
            }
            run_id = insert_run(cur, run_key, source_filter, args.notes)
            predictions = fetch_predictions(cur, stage2_run_id, sorted(overrides.keys()))
            ledger_lookup = fetch_ledger_rows(cur, stage3_run_id)

            decisions = []
            update_rows = []
            prediction_ids = []
            for prediction in predictions:
                match_status, event_ledger_id, rationale, review_meta = classify(ledger_lookup.get(prediction.prediction_id, []))
                decisions.append((prediction.prediction_id, match_status, event_ledger_id, rationale, review_meta))
                update_rows.append((match_status, event_ledger_id, prediction.prediction_id))
                prediction_ids.append(prediction.prediction_id)

            if not args.dry_run:
                demote_existing(cur, prediction_ids)
                update_predictions(cur, update_rows)
                insert_reviews(cur, run_id, decisions)

            update_run(
                cur,
                run_id,
                "completed",
                {
                    "family": "storm",
                    "prediction_count": len(predictions),
                    "status_counts": dict(Counter(item[1] for item in decisions)),
                },
            )
        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

        print(
            json.dumps(
                {
                    "run_key": run_key,
                    "family": "storm",
                    "prediction_count": len(predictions),
                    "status_counts": dict(Counter(item[1] for item in decisions)),
                },
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        conn.rollback()
        if run_id is not None:
            with conn.cursor() as cur:
                update_run(cur, run_id, "failed", {"error": str(exc), "family": "storm"})
            conn.commit()
        print(f"Stage 4 storm run failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

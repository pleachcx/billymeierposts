#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_batch


SCRIPT_VERSION = "stage8_publication_adjudication_v1"
REVIEWER = "script:stage8_publication_adjudication_v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Review publication-date timing against observed events for the included scored cohort.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", default="stage2-20260310T232950Z", help="Stage 2 run key to scope predictions.")
    parser.add_argument("--run-key", help="Unique Stage 8 run key. Defaults to a timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument("--dry-run", action="store_true", help="Compute adjudications without writing DB changes.")
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage8-publication-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def fetch_stage2_run(cur, run_key: str) -> dict[str, Any]:
    cur.execute(
        """
        SELECT id, run_key
        FROM public.prediction_audit_runs
        WHERE stage = 'stage2_eligibility' AND run_key = %s
        """,
        (run_key,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Missing Stage 2 run {run_key}.")
    return row


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
        VALUES (%s, 'stage8_publication_adjudication', 'running', %s, %s, %s, %s, %s, %s, now())
        RETURNING id
        """,
        (
            run_key,
            SCRIPT_VERSION,
            "none",
            "public.prediction_audit_predictions",
            Json(source_filter),
            notes or None,
            Json({"script_version": SCRIPT_VERSION, "scope": "included_scored_predictions"}),
        ),
    )
    return cur.fetchone()["id"]


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


def fetch_predictions(cur, stage2_run_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            p.id,
            p.report_number,
            p.candidate_seq,
            p.event_family_final,
            p.match_status,
            p.final_status,
            p.earliest_provable_public_date,
            p.public_date_basis,
            el.event_start_date,
            el.event_title
        FROM public.prediction_audit_predictions p
        LEFT JOIN public.prediction_audit_event_ledger el ON el.id = p.best_event_ledger_id
        WHERE p.last_stage2_run_id = %s
          AND p.final_status = 'included_in_statistics'
          AND p.match_status IN ('exact_hit', 'near_hit', 'similar_only', 'miss')
        ORDER BY p.event_family_final, p.report_number, p.candidate_seq
        """,
        (stage2_run_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def decide_status(prediction: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    public_date = prediction["earliest_provable_public_date"]
    event_date = prediction["event_start_date"]
    if public_date is None:
        return (
            "no_public_date_evidence",
            "No provable public date is recorded yet; current evaluation still depends on the claimed contact date fallback.",
            {"script_version": SCRIPT_VERSION},
        )
    if event_date is not None and event_date < public_date:
        lag_days = (event_date - public_date).days
        return (
            "event_precedes_publication",
            "The currently recorded provable public date is later than the observed event date.",
            {
                "script_version": SCRIPT_VERSION,
                "event_start_date": event_date.isoformat(),
                "earliest_provable_public_date": public_date.isoformat(),
                "publication_lag_days_vs_event": lag_days,
            },
        )
    return (
        "public_date_ok",
        "The current provable public date does not postdate the observed event date.",
        {
            "script_version": SCRIPT_VERSION,
            "event_start_date": event_date.isoformat() if event_date else None,
            "earliest_provable_public_date": public_date.isoformat(),
        },
    )


def demote_existing_primary_reviews(cur, prediction_ids: list[int]) -> None:
    if not prediction_ids:
        return
    cur.execute(
        """
        UPDATE public.prediction_audit_public_date_reviews
        SET is_primary = false
        WHERE prediction_id = ANY(%s) AND is_primary = true
        """,
        (prediction_ids,),
    )


def update_predictions(cur, run_id: int, rows: list[tuple[int, str, str, dict[str, Any]]]) -> None:
    params = [
        (status, rationale, run_id, Json(meta), prediction_id)
        for prediction_id, status, rationale, meta in rows
    ]
    execute_batch(
        cur,
        """
        UPDATE public.prediction_audit_predictions
        SET public_date_status = %s,
            public_date_reason = %s,
            last_public_date_review_run_id = %s,
            public_date_meta = %s
        WHERE id = %s
        """,
        params,
        page_size=200,
    )


def insert_reviews(cur, run_id: int, rows: list[tuple[int, str, str, dict[str, Any], str]]) -> None:
    params = [
        (
            prediction_id,
            run_id,
            event_family,
            status,
            True,
            REVIEWER,
            rationale,
            Json(meta),
        )
        for prediction_id, status, rationale, meta, event_family in rows
    ]
    execute_batch(
        cur,
        """
        INSERT INTO public.prediction_audit_public_date_reviews (
            prediction_id,
            review_run_id,
            event_family,
            public_date_status,
            is_primary,
            reviewer,
            rationale,
            review_meta
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        params,
        page_size=200,
    )


def main() -> int:
    args = parse_args()
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        print(f"Missing DSN env var: {args.dsn_env}", file=sys.stderr)
        return 2

    run_key = args.run_key or generate_run_key()
    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    conn.autocommit = False
    run_id: int | None = None

    try:
        with conn.cursor() as cur:
            stage2 = fetch_stage2_run(cur, args.stage2_run_key)
            predictions = fetch_predictions(cur, stage2["id"])

            if not args.dry_run:
                run_id = insert_run(
                    cur,
                    run_key,
                    {"stage2_run_key": stage2["run_key"], "family": "cross_family"},
                    args.notes,
                )
                conn.commit()

        decisions: list[tuple[int, str, str, dict[str, Any]]] = []
        review_rows: list[tuple[int, str, str, dict[str, Any], str]] = []
        counts: Counter[str] = Counter()
        for prediction in predictions:
            status, rationale, meta = decide_status(prediction)
            decisions.append((prediction["id"], status, rationale, meta))
            review_rows.append((prediction["id"], status, rationale, meta, prediction["event_family_final"]))
            counts[status] += 1

        if not args.dry_run and run_id is not None:
            with conn.cursor() as cur:
                demote_existing_primary_reviews(cur, [prediction["id"] for prediction in predictions])
                update_predictions(cur, run_id, decisions)
                insert_reviews(cur, run_id, review_rows)
                update_run(
                    cur,
                    run_id,
                    "completed",
                    {
                        "stage2_run_key": stage2["run_key"],
                        "scoped_prediction_count": len(predictions),
                        "public_date_status_counts": dict(counts),
                        "script_version": SCRIPT_VERSION,
                    },
                )
            conn.commit()

        print(
            json.dumps(
                {
                    "run_key": run_key,
                    "dry_run": args.dry_run,
                    "stage2_run_key": stage2["run_key"],
                    "scoped_prediction_count": len(predictions),
                    "public_date_status_counts": dict(counts),
                    "script_version": SCRIPT_VERSION,
                },
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        conn.rollback()
        if not args.dry_run and run_id is not None:
            with conn.cursor() as cur:
                update_run(cur, run_id, "failed", {"error": str(exc)[:1000], "script_version": SCRIPT_VERSION})
            conn.commit()
        print(f"Stage 8 publication adjudication failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

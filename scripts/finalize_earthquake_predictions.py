#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import Json, execute_batch


SCRIPT_VERSION = "stage7_earthquake_final_v1"
REVIEWER = "script:stage7_earthquake_final_v1"
REPO_ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finalize earthquake cohort inclusion/exclusion decisions.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key. Defaults to latest completed Stage 2 run.")
    parser.add_argument("--stage5-run-key", help="Stage 5 run key. Defaults to latest completed Stage 5 run.")
    parser.add_argument("--run-key", help="Unique Stage 7 run key. Defaults to a timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument(
        "--adjudications-path",
        default=str(REPO_ROOT / "data" / "earthquake_final_adjudications.json"),
        help="Path to manual final-adjudication rules keyed by report_number:candidate_seq.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute final statuses without writing DB updates.")
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage7-earthquake-final-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def fetch_run(cur, stage: str, run_key: str | None) -> tuple[int, str, dict[str, Any]]:
    if run_key:
        cur.execute(
            """
            SELECT id, run_key, source_filter
            FROM public.prediction_audit_runs
            WHERE stage = %s AND run_key = %s
            """,
            (stage, run_key),
        )
    else:
        cur.execute(
            """
            SELECT id, run_key, source_filter
            FROM public.prediction_audit_runs
            WHERE stage = %s AND status = 'completed'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (stage,),
        )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"No completed run found for stage {stage}.")
    return row[0], row[1], row[2] or {}


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
        VALUES (%s, 'stage7_final_adjudication', 'running', %s, %s, %s, %s, %s, %s, now())
        RETURNING id
        """,
        (
            run_key,
            SCRIPT_VERSION,
            "none",
            "public.prediction_audit_predictions",
            Json(source_filter),
            notes or None,
            Json({"family": "earthquake"}),
        ),
    )
    return cur.fetchone()[0]


def update_run(cur, run_id: int, status: str, run_meta: dict[str, Any], notes: str | None = None) -> None:
    cur.execute(
        """
        UPDATE public.prediction_audit_runs
        SET status = %s,
            completed_at = CASE WHEN %s IN ('completed', 'failed', 'abandoned') THEN now() ELSE completed_at END,
            notes = COALESCE(%s, notes),
            run_meta = COALESCE(run_meta, '{}'::jsonb) || %s::jsonb
        WHERE id = %s
        """,
        (status, status, notes, json.dumps(run_meta), run_id),
    )


def load_adjudications(path: str) -> dict[str, dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def fetch_predictions(cur, stage2_run_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            id,
            report_number,
            candidate_seq,
            match_status,
            claim_normalized,
            target_name,
            p_exact_under_null,
            p_near_under_null,
            p_similar_under_null,
            p_miss_under_null
        FROM public.prediction_audit_predictions
        WHERE last_stage2_run_id = %s
          AND event_family_final = 'earthquake'
          AND stage2_label IN ('eligible_prediction', 'significant_prediction')
          AND time_window_start IS NOT NULL
          AND time_window_end IS NOT NULL
        ORDER BY report_number, candidate_seq
        """,
        (stage2_run_id,),
    )
    columns = [description[0] for description in cur.description]
    return [dict(zip(columns, row, strict=False)) for row in cur.fetchall()]


def decide_final_status(prediction: dict[str, Any], adjudications: dict[str, dict[str, Any]]) -> tuple[str, str, dict[str, Any]]:
    probability_ready = all(prediction[field] is not None for field in ("p_exact_under_null", "p_near_under_null", "p_similar_under_null", "p_miss_under_null"))
    if prediction["match_status"] in {"exact_hit", "near_hit", "similar_only", "miss"} and probability_ready:
        return (
            "included_in_statistics",
            "Prediction has a resolved earthquake match outcome and a complete null-model probability vector.",
            {
                "script_version": SCRIPT_VERSION,
                "reason_code": "scored_and_probability_ready",
            },
        )

    key = f"{prediction['report_number']}:{prediction['candidate_seq']}"
    rule = adjudications.get(key)
    if not rule:
        raise RuntimeError(f"Missing final adjudication rule for unresolved earthquake prediction {key}.")

    return (
        rule["final_status"],
        rule["rationale"],
        {
            "script_version": SCRIPT_VERSION,
            "reason_code": rule.get("reason_code"),
            "rule_key": key,
        },
    )


def demote_existing_primary_reviews(cur, prediction_ids: list[int]) -> None:
    if not prediction_ids:
        return
    cur.execute(
        """
        UPDATE public.prediction_audit_final_reviews
        SET is_primary = false
        WHERE prediction_id = ANY(%s) AND is_primary = true
        """,
        (prediction_ids,),
    )


def update_predictions(cur, run_id: int, decisions: list[tuple[int, str, str, dict[str, Any]]]) -> None:
    params = [
        (status, rationale, run_id, Json(meta), prediction_id)
        for prediction_id, status, rationale, meta in decisions
    ]
    execute_batch(
        cur,
        """
        UPDATE public.prediction_audit_predictions
        SET final_status = %s,
            final_reason = %s,
            last_final_review_run_id = %s,
            final_meta = %s
        WHERE id = %s
        """,
        params,
        page_size=200,
    )


def insert_reviews(cur, run_id: int, decisions: list[tuple[int, str, str, dict[str, Any]]]) -> None:
    params = [
        (
            prediction_id,
            run_id,
            "earthquake",
            status,
            True,
            REVIEWER,
            rationale,
            Json(meta),
        )
        for prediction_id, status, rationale, meta in decisions
    ]
    execute_batch(
        cur,
        """
        INSERT INTO public.prediction_audit_final_reviews (
            prediction_id,
            review_run_id,
            event_family,
            final_status,
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

    adjudications = load_adjudications(args.adjudications_path)
    run_key = args.run_key or generate_run_key()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    final_run_id: int | None = None

    try:
        with conn.cursor() as cur:
            _, resolved_stage5_run_key, stage5_filter = fetch_run(cur, "stage5_probability_model", args.stage5_run_key)
            stage2_run_key = args.stage2_run_key or stage5_filter.get("stage2_run_key")
            if not stage2_run_key:
                raise RuntimeError("Could not infer Stage 2 run key from Stage 5 metadata.")
            stage2_run_id, resolved_stage2_run_key, _ = fetch_run(cur, "stage2_eligibility", stage2_run_key)
            predictions = fetch_predictions(cur, stage2_run_id)

            if not args.dry_run:
                final_run_id = insert_run(
                    cur,
                    run_key,
                    {
                        "stage2_run_key": resolved_stage2_run_key,
                        "stage5_run_key": resolved_stage5_run_key,
                        "adjudications_path": args.adjudications_path,
                        "family": "earthquake",
                    },
                    args.notes,
                )
                conn.commit()

        decisions = []
        counts: Counter[str] = Counter()
        for prediction in predictions:
            status, rationale, meta = decide_final_status(prediction, adjudications)
            decisions.append((prediction["id"], status, rationale, meta))
            counts[status] += 1

        if not args.dry_run and final_run_id is not None:
            with conn.cursor() as cur:
                demote_existing_primary_reviews(cur, [prediction_id for prediction_id, _, _, _ in decisions])
                update_predictions(cur, final_run_id, decisions)
                insert_reviews(cur, final_run_id, decisions)
                update_run(
                    cur,
                    final_run_id,
                    "completed",
                    {
                        "stage2_run_key": resolved_stage2_run_key,
                        "stage5_run_key": resolved_stage5_run_key,
                        "final_status_counts": dict(counts),
                        "scoped_prediction_count": len(decisions),
                        "script_version": SCRIPT_VERSION,
                    },
                )
            conn.commit()

        print(
            json.dumps(
                {
                    "run_key": run_key,
                    "dry_run": args.dry_run,
                    "stage2_run_key": resolved_stage2_run_key,
                    "stage5_run_key": resolved_stage5_run_key,
                    "scoped_prediction_count": len(decisions),
                    "final_status_counts": dict(counts),
                    "script_version": SCRIPT_VERSION,
                },
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        conn.rollback()
        if not args.dry_run and final_run_id is not None:
            with conn.cursor() as cur:
                update_run(cur, final_run_id, "failed", {"error": str(exc)[:1000], "script_version": SCRIPT_VERSION})
            conn.commit()
        print(f"Stage 7 earthquake final adjudication failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

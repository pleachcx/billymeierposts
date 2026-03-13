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


SCRIPT_VERSION = "stage7_storm_final_v3"
REVIEWER = "script:stage7_storm_final_v3"
REPO_ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finalize storm cohort inclusion/exclusion decisions for the scoped storm slice.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key. Defaults to latest completed Stage 2 run.")
    parser.add_argument("--stage4-run-key", help="Stage 4 storm run key. Defaults to latest completed Stage 4 storm run.")
    parser.add_argument("--run-key", help="Unique Stage 7 storm run key. Defaults to a timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument(
        "--overrides-path",
        default=str(REPO_ROOT / "data" / "storm_prediction_overrides.json"),
        help="Path to storm prediction override JSON keyed by report_number:candidate_seq.",
    )
    parser.add_argument(
        "--adjudications-path",
        default=str(REPO_ROOT / "data" / "storm_final_adjudications.json"),
        help="Path to storm final adjudication JSON keyed by report_number:candidate_seq.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute final statuses without writing DB updates.")
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage7-storm-final-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def fetch_run(cur, stage: str, run_key: str | None, family: str | None = None) -> tuple[int, str, dict[str, Any]]:
    family_clause = "AND run_meta->>'family' = %s" if family else ""
    if run_key:
        sql = f"""
            SELECT id, run_key, source_filter
            FROM public.prediction_audit_runs
            WHERE stage = %s AND run_key = %s {family_clause}
        """
        params: list[Any] = [stage, run_key]
        if family:
            params.append(family)
        cur.execute(sql, params)
    else:
        sql = f"""
            SELECT id, run_key, source_filter
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
            Json({"family": "storm", "scope": "storm_catalog_v2"}),
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


def load_json(path: str) -> dict[str, Any]:
    if not Path(path).exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def fetch_predictions(cur, stage2_run_id: int, stage4_run_id: int, scoped_keys: list[str]) -> list[dict[str, Any]]:
    pairs = [(int(key.split(":")[0]), int(key.split(":")[1])) for key in scoped_keys]
    cur.execute(
        """
        SELECT
            p.id,
            p.report_number,
            p.candidate_seq,
            mr.match_status,
            p.claim_normalized
        FROM public.prediction_audit_predictions p
        LEFT JOIN public.prediction_audit_match_reviews mr
          ON mr.prediction_id = p.id
         AND mr.review_run_id = %s
        WHERE last_stage2_run_id = %s
          AND (report_number, candidate_seq) IN %s
        ORDER BY report_number, candidate_seq
        """,
        (stage4_run_id, stage2_run_id, tuple(pairs)),
    )
    columns = [description[0] for description in cur.description]
    return [dict(zip(columns, row, strict=False)) for row in cur.fetchall()]


def decide_final_status(prediction: dict[str, Any], adjudications: dict[str, dict[str, Any]]) -> tuple[str, str, dict[str, Any]]:
    key = f"{prediction['report_number']}:{prediction['candidate_seq']}"
    rule = adjudications.get(key)
    if rule:
        return (
            rule["final_status"],
            rule["rationale"],
            {
                "script_version": SCRIPT_VERSION,
                "reason_code": rule.get("reason_code"),
                "rule_key": key,
            },
        )

    if prediction["match_status"] in {"exact_hit", "near_hit", "similar_only", "miss"}:
        return (
            "included_in_statistics",
            "Prediction belongs to the scoped storm slice and has a resolved match outcome.",
            {
                "script_version": SCRIPT_VERSION,
                "reason_code": "resolved_storm_slice",
            },
        )

    raise RuntimeError(f"Missing storm final adjudication rule for unresolved prediction {key}.")


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
    params = [(status, rationale, run_id, Json(meta), prediction_id) for prediction_id, status, rationale, meta in decisions]
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
        page_size=100,
    )


def insert_reviews(cur, run_id: int, decisions: list[tuple[int, str, str, dict[str, Any]]]) -> None:
    params = [
        (prediction_id, run_id, "storm", status, True, REVIEWER, rationale, Json(meta))
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
        page_size=100,
    )


def main() -> int:
    args = parse_args()
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        print(f"Missing DSN env var: {args.dsn_env}", file=sys.stderr)
        return 2

    overrides = load_json(args.overrides_path)
    adjudications = load_json(args.adjudications_path)
    run_key = args.run_key or generate_run_key()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    run_id: int | None = None

    try:
        with conn.cursor() as cur:
            stage4_run_id, resolved_stage4_run_key, stage4_filter = fetch_run(cur, "stage4_match_scoring", args.stage4_run_key, family="storm")
            stage2_run_key = args.stage2_run_key or stage4_filter.get("stage2_run_key")
            if not stage2_run_key:
                raise RuntimeError("Could not infer Stage 2 run key from Stage 4 storm metadata.")
            stage2_run_id, resolved_stage2_run_key, _ = fetch_run(cur, "stage2_eligibility", stage2_run_key)
            source_filter = {
                "stage2_run_key": resolved_stage2_run_key,
                "stage4_run_key": resolved_stage4_run_key,
                "family": "storm",
                "override_keys": sorted(overrides.keys()),
                "adjudication_keys": sorted(adjudications.keys()),
                "adjudications_path": args.adjudications_path,
            }
            run_id = insert_run(cur, run_key, source_filter, args.notes)
            scoped_keys = sorted(set(overrides.keys()) | set(adjudications.keys()))
            predictions = fetch_predictions(cur, stage2_run_id, stage4_run_id, scoped_keys)
            decisions = [(prediction["id"], *decide_final_status(prediction, adjudications)) for prediction in predictions]

            if not args.dry_run:
                demote_existing_primary_reviews(cur, [prediction["id"] for prediction in predictions])
                update_predictions(cur, run_id, decisions)
                insert_reviews(cur, run_id, decisions)

            update_run(
                cur,
                run_id,
                "completed",
                {
                    "family": "storm",
                    "prediction_count": len(predictions),
                    "final_status_counts": dict(Counter(item[1] for item in decisions)),
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
                    "final_status_counts": dict(Counter(item[1] for item in decisions)),
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
        print(f"Stage 7 storm run failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

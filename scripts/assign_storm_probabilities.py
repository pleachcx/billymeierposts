#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import Json, execute_batch


SCRIPT_VERSION = "stage5_storm_location_risk_window_v2"
REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class Prediction:
    prediction_id: int
    report_number: int
    candidate_seq: int
    match_status: str
    time_window_start: date | None
    time_window_end: date | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assign provisional null-model probabilities for the scoped storm slice.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key. Defaults to latest completed Stage 2 run.")
    parser.add_argument("--stage4-run-key", help="Stage 4 storm run key. Defaults to latest completed Stage 4 storm run.")
    parser.add_argument("--run-key", help="Unique Stage 5 storm run key. Defaults to a timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument(
        "--baselines-path",
        default=str(REPO_ROOT / "data" / "storm_probability_baselines.json"),
        help="Path to storm probability baseline JSON keyed by report_number:candidate_seq.",
    )
    parser.add_argument(
        "--overrides-path",
        default=str(REPO_ROOT / "data" / "storm_prediction_overrides.json"),
        help="Path to storm prediction override JSON keyed by report_number:candidate_seq.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute probabilities without writing DB updates.")
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage5-storm-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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
        VALUES (%s, 'stage5_probability_model', 'running', %s, %s, %s, %s, %s, %s, now())
        RETURNING id
        """,
        (
            run_key,
            SCRIPT_VERSION,
            "none",
            "public.prediction_audit_predictions",
            Json(source_filter),
            notes or None,
            Json({"family": "storm", "model": "location_risk_window_single_event_v1"}),
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


def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_iso_date(raw: str | None) -> date | None:
    if not raw:
        return None
    return date.fromisoformat(raw)


def fetch_predictions(cur, stage2_run_id: int, override_keys: list[str]) -> list[Prediction]:
    pairs = [(int(key.split(":")[0]), int(key.split(":")[1])) for key in override_keys]
    cur.execute(
        """
        SELECT id, report_number, candidate_seq, match_status, time_window_start, time_window_end
        FROM public.prediction_audit_predictions
        WHERE last_stage2_run_id = %s
          AND event_family_final = 'storm'
          AND (report_number, candidate_seq) IN %s
        ORDER BY report_number, candidate_seq
        """,
        (stage2_run_id, tuple(pairs)),
    )
    return [Prediction(*row) for row in cur.fetchall()]


def days_inclusive(start: date, end: date) -> int:
    return max(1, (end - start).days + 1)


def clamp_non_negative(value: int) -> int:
    return max(0, value)


def build_update(prediction: Prediction, override: dict[str, Any], baseline: dict[str, Any]) -> tuple[Any, ...]:
    exact_start = parse_iso_date(override.get("window_start")) or prediction.time_window_start
    exact_end = parse_iso_date(override.get("window_end")) or prediction.time_window_end
    if exact_start is None or exact_end is None:
        raise RuntimeError(f"Prediction {prediction.report_number}:{prediction.candidate_seq} lacks an exact time window.")

    calibration_start = parse_iso_date(baseline.get("calibration_start"))
    calibration_end = parse_iso_date(baseline.get("calibration_end"))
    if calibration_start is None or calibration_end is None:
        raise RuntimeError(f"Baseline for {prediction.report_number}:{prediction.candidate_seq} lacks calibration dates.")
    if calibration_end < calibration_start:
        raise RuntimeError(f"Baseline for {prediction.report_number}:{prediction.candidate_seq} has an inverted calibration window.")

    total_days = days_inclusive(calibration_start, calibration_end)
    event_count = int(baseline.get("event_count", 1))
    if event_count < 0:
        raise RuntimeError(f"Baseline for {prediction.report_number}:{prediction.candidate_seq} has a negative event count.")

    exact_days = days_inclusive(exact_start, exact_end)
    near_expansion_days = int(baseline.get("near_expansion_days", 7))
    similar_horizon_days = int(baseline.get("similar_horizon_days", 365))
    near_total_days = clamp_non_negative((exact_days + (near_expansion_days * 2)) - exact_days)
    similar_total_days = clamp_non_negative(similar_horizon_days - exact_days - near_total_days)

    rate_per_day = event_count / total_days
    lambda_exact = rate_per_day * exact_days
    lambda_near = rate_per_day * near_total_days
    lambda_similar = rate_per_day * similar_total_days

    p_exact = 1.0 - math.exp(-lambda_exact)
    p_near = math.exp(-lambda_exact) * (1.0 - math.exp(-lambda_near))
    p_similar = math.exp(-(lambda_exact + lambda_near)) * (1.0 - math.exp(-lambda_similar))
    p_miss = math.exp(-(lambda_exact + lambda_near + lambda_similar))

    meta = {
        "script_version": SCRIPT_VERSION,
        "model": baseline.get("model", "actor_risk_window_single_event_v1"),
        "probability_key": baseline["probability_key"],
        "event_count": event_count,
        "calibration_window": {
            "start": calibration_start.isoformat(),
            "end": calibration_end.isoformat(),
            "days": total_days,
        },
        "window_days": {
            "exact": exact_days,
            "near_only": near_total_days,
            "similar_only": similar_total_days,
        },
        "rate_per_day": rate_per_day,
        "lambda": {
            "exact": lambda_exact,
            "near_only": lambda_near,
            "similar_only": lambda_similar,
        },
        "probabilities": {
            "exact": p_exact,
            "near_only": p_near,
            "similar_only": p_similar,
            "miss": p_miss,
        },
        "baseline_notes": baseline.get("notes"),
    }

    return (
        p_exact,
        p_near,
        p_similar,
        p_miss,
        SCRIPT_VERSION,
        "provisional storm null from location-specific at-risk window",
        Json(meta),
        prediction.prediction_id,
    )


def update_predictions(cur, rows: list[tuple[Any, ...]]) -> None:
    execute_batch(
        cur,
        """
        UPDATE public.prediction_audit_predictions
        SET p_exact_under_null = %s,
            p_near_under_null = %s,
            p_similar_under_null = %s,
            p_miss_under_null = %s,
            probability_model_version = %s,
            probability_notes = %s,
            probability_meta = %s
        WHERE id = %s
        """,
        rows,
        page_size=100,
    )


def main() -> int:
    args = parse_args()
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        print(f"Missing DSN env var: {args.dsn_env}", file=sys.stderr)
        return 2

    baselines = load_json(args.baselines_path)
    overrides = load_json(args.overrides_path)
    run_key = args.run_key or generate_run_key()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    run_id: int | None = None

    try:
        with conn.cursor() as cur:
            _, resolved_stage4_run_key, stage4_filter = fetch_run(cur, "stage4_match_scoring", args.stage4_run_key, family="storm")
            stage2_run_key = args.stage2_run_key or stage4_filter.get("stage2_run_key")
            if not stage2_run_key:
                raise RuntimeError("Could not infer Stage 2 run key from storm Stage 4 metadata.")
            stage2_run_id, resolved_stage2_run_key, _ = fetch_run(cur, "stage2_eligibility", stage2_run_key)
            override_keys = sorted(overrides.keys())
            predictions = fetch_predictions(cur, stage2_run_id, override_keys)

            if not args.dry_run:
                run_id = insert_run(
                    cur,
                    run_key,
                    {
                        "stage2_run_key": resolved_stage2_run_key,
                        "stage4_run_key": resolved_stage4_run_key,
                        "family": "storm",
                        "scope": "storm_catalog_v2",
                    },
                    args.notes,
                )
                conn.commit()

        updates = []
        for prediction in predictions:
            key = f"{prediction.report_number}:{prediction.candidate_seq}"
            baseline = baselines.get(key)
            override = overrides.get(key)
            if baseline is None or override is None:
                raise RuntimeError(f"Missing storm baseline or override for {key}.")
            updates.append(build_update(prediction, override, baseline))

        if not args.dry_run and run_id is not None:
            with conn.cursor() as cur:
                update_predictions(cur, updates)
                update_run(
                    cur,
                    run_id,
                    "completed",
                    {
                        "family": "storm",
                        "prediction_count": len(updates),
                        "probability_ready_count": len(updates),
                        "script_version": SCRIPT_VERSION,
                    },
                )
            conn.commit()

        print(
            json.dumps(
                {
                    "run_key": run_key,
                    "family": "storm",
                    "prediction_count": len(updates),
                    "probability_ready_count": len(updates),
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
        print(f"Stage 5 storm probability run failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

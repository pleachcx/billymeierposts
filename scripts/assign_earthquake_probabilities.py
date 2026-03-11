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
import requests
from psycopg2.extras import Json, execute_batch

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


SCRIPT_VERSION = "stage5_earthquake_probability_v1"
USGS_QUERY_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
CALIBRATION_START = date(1973, 1, 1)
CALIBRATION_END = date(2025, 12, 31)
USGS_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "billymeierposts-stage5-earthquake/1.0 (+https://github.com/pleachcx/billymeierposts)",
}


@dataclass
class Prediction:
    prediction_id: int
    bundle_key: str | None
    report_number: int
    candidate_seq: int
    match_status: str
    target_name: str | None
    target_type: str | None
    target_lat: float | None
    target_lon: float | None
    target_radius_km: float | None
    magnitude_min: float | None
    magnitude_max: float | None
    severity_band: str | None
    time_window_start: date | None
    time_window_end: date | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 5 earthquake probability assignment.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key. Defaults to latest completed Stage 2 run.")
    parser.add_argument("--stage4-run-key", help="Stage 4 run key. Defaults to latest completed Stage 4 run.")
    parser.add_argument("--run-key", help="Unique Stage 5 run key. Defaults to timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument("--limit", type=int, help="Limit scored predictions processed.")
    parser.add_argument("--dry-run", action="store_true", help="Compute probabilities without writing DB updates.")
    parser.add_argument("--batch-size", type=int, default=100, help="DB update batch size.")
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage5-earthquake-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def fetch_run(cur, stage: str, run_key: str | None) -> tuple[int, str]:
    if run_key:
        cur.execute(
            """
            SELECT id, run_key
            FROM public.prediction_audit_runs
            WHERE run_key = %s AND stage = %s
            """,
            (run_key, stage),
        )
    else:
        cur.execute(
            """
            SELECT id, run_key
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
            Json({"family": "earthquake", "model": "poisson_rate_exclusive_bands"}),
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


def fetch_predictions(cur, stage2_run_id: int, limit: int | None) -> list[Prediction]:
    sql = """
        SELECT
            id,
            bundle_key,
            report_number,
            candidate_seq,
            match_status,
            target_name,
            target_type,
            target_lat,
            target_lon,
            target_radius_km,
            magnitude_min,
            magnitude_max,
            severity_band,
            time_window_start,
            time_window_end
        FROM public.prediction_audit_predictions
        WHERE last_stage2_run_id = %s
          AND event_family_final = 'earthquake'
          AND stage2_label IN ('eligible_prediction', 'significant_prediction')
          AND time_window_start IS NOT NULL
          AND time_window_end IS NOT NULL
        ORDER BY time_window_start, report_number, candidate_seq
    """
    params: list[Any] = [stage2_run_id]
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    cur.execute(sql, params)
    predictions: list[Prediction] = []
    for row in cur.fetchall():
        predictions.append(
            Prediction(
                prediction_id=row[0],
                bundle_key=row[1],
                report_number=row[2],
                candidate_seq=row[3],
                match_status=row[4],
                target_name=row[5],
                target_type=row[6],
                target_lat=float(row[7]) if row[7] is not None else None,
                target_lon=float(row[8]) if row[8] is not None else None,
                target_radius_km=float(row[9]) if row[9] is not None else None,
                magnitude_min=float(row[10]) if row[10] is not None else None,
                magnitude_max=float(row[11]) if row[11] is not None else None,
                severity_band=row[12],
                time_window_start=row[13],
                time_window_end=row[14],
            )
        )
    return predictions


def classify_target_radii(prediction: Prediction) -> tuple[float, float, float]:
    if prediction.target_type == "point":
        return 25.0, 50.0, 100.0
    exact = prediction.target_radius_km or 25.0
    return exact, exact + 50.0, exact + 100.0


def grace_days(prediction: Prediction) -> int:
    window_days = (prediction.time_window_end - prediction.time_window_start).days + 1
    if window_days <= 1:
        return 7
    if window_days <= 31:
        return 31
    return min(365, max(1, math.ceil(window_days * 0.25)))


def exclusive_window_days(prediction: Prediction) -> tuple[int, int, int]:
    exact_days = (prediction.time_window_end - prediction.time_window_start).days + 1
    near_total = exact_days + 2 * grace_days(prediction)
    log_total = near_total + 2 * grace_days(prediction)
    near_only = max(0, near_total - exact_days)
    log_only = max(0, log_total - near_total)
    return exact_days, near_only, log_only


def band_min_magnitude(prediction: Prediction) -> float:
    if prediction.magnitude_min is not None:
        return max(0.0, prediction.magnitude_min - 1.0)
    if prediction.severity_band == "devastating":
        return 6.5
    if prediction.severity_band == "severe":
        return 6.0
    return 5.5


def magnitude_bands(prediction: Prediction, magnitude_value: float | None) -> tuple[bool, bool]:
    if magnitude_value is None:
        return False, False
    if prediction.magnitude_min is not None and prediction.magnitude_max is not None:
        target_mag = (prediction.magnitude_min + prediction.magnitude_max) / 2.0
        exact_ok = abs(magnitude_value - target_mag) <= 0.5
        near_ok = abs(magnitude_value - target_mag) <= 1.0
        return exact_ok, near_ok
    if prediction.severity_band == "devastating":
        return magnitude_value >= 7.5, magnitude_value >= 6.5
    if prediction.severity_band == "severe":
        return magnitude_value >= 6.0, magnitude_value >= 5.5
    if prediction.severity_band == "strong":
        return magnitude_value >= 5.5, magnitude_value >= 5.0
    return True, True


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return radius * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def fetch_calibration_events(prediction: Prediction) -> list[dict[str, Any]]:
    _, _, log_radius = classify_target_radii(prediction)
    params = {
        "format": "geojson",
        "orderby": "time-asc",
        "starttime": CALIBRATION_START.isoformat(),
        "endtime": CALIBRATION_END.isoformat(),
        "latitude": prediction.target_lat,
        "longitude": prediction.target_lon,
        "maxradiuskm": round(log_radius, 2),
        "minmagnitude": band_min_magnitude(prediction),
        "limit": 20000,
    }
    response = requests.get(USGS_QUERY_URL, params=params, headers=USGS_HEADERS, timeout=30)
    response.raise_for_status()
    payload = response.json()
    return payload.get("features", [])


def classify_calibration_events(prediction: Prediction, features: list[dict[str, Any]]) -> tuple[int, int, int, int]:
    exact_radius, near_radius, log_radius = classify_target_radii(prediction)
    exact_count = 0
    near_count = 0
    log_count = 0
    total_count = 0
    for feature in features:
        coords = feature.get("geometry", {}).get("coordinates") or [None, None]
        lon = coords[0]
        lat = coords[1]
        if lat is None or lon is None:
            continue
        total_count += 1
        distance_km = haversine_km(prediction.target_lat, prediction.target_lon, lat, lon)
        magnitude_value = feature.get("properties", {}).get("mag")
        magnitude_value = float(magnitude_value) if magnitude_value is not None else None
        exact_mag_ok, near_mag_ok = magnitude_bands(prediction, magnitude_value)
        if distance_km <= exact_radius and exact_mag_ok:
            exact_count += 1
        elif distance_km <= near_radius and near_mag_ok:
            near_count += 1
        elif distance_km <= log_radius:
            log_count += 1
    return total_count, exact_count, near_count, log_count


def poisson_at_least_one(rate_per_year: float, window_days: int) -> float:
    if rate_per_year <= 0 or window_days <= 0:
        return 0.0
    lambda_value = rate_per_year * (window_days / 365.25)
    return 1.0 - math.exp(-lambda_value)


def build_probability_update(prediction: Prediction, calibration_counts: tuple[int, int, int, int] | None, query_status: str) -> tuple[Any, ...]:
    if calibration_counts is None:
        meta = {
            "script_version": SCRIPT_VERSION,
            "reason": query_status,
        }
        return (
            None,
            None,
            None,
            None,
            SCRIPT_VERSION,
            query_status,
            Json(meta),
            prediction.prediction_id,
        )

    total_count, exact_count, near_count, log_count = calibration_counts
    calibration_years = (CALIBRATION_END - CALIBRATION_START).days / 365.25
    exact_days, near_days, log_days = exclusive_window_days(prediction)
    exact_rate = exact_count / calibration_years
    near_rate = near_count / calibration_years
    log_rate = log_count / calibration_years
    lambda_exact = exact_rate * (exact_days / 365.25)
    lambda_near = near_rate * (near_days / 365.25)
    lambda_log = log_rate * (log_days / 365.25)
    p_exact = 1.0 - math.exp(-lambda_exact)
    p_near_only = math.exp(-lambda_exact) * (1.0 - math.exp(-lambda_near))
    p_similar_only = math.exp(-(lambda_exact + lambda_near)) * (1.0 - math.exp(-lambda_log))
    p_miss = math.exp(-(lambda_exact + lambda_near + lambda_log))

    meta = {
        "script_version": SCRIPT_VERSION,
        "model": "poisson_rate_exclusive_bands",
        "calibration_start": CALIBRATION_START.isoformat(),
        "calibration_end": CALIBRATION_END.isoformat(),
        "calibration_years": calibration_years,
        "window_days": {
            "exact": exact_days,
            "near_only": near_days,
            "similar_only": log_days,
        },
        "event_counts": {
            "total_candidates": total_count,
            "exact_spatial_mag": exact_count,
            "near_spatial_mag_only": near_count,
            "log_spatial_only": log_count,
        },
        "annual_rates": {
            "exact": exact_rate,
            "near_only": near_rate,
            "similar_only": log_rate,
        },
        "lambda": {
            "exact": lambda_exact,
            "near_only": lambda_near,
            "similar_only": lambda_log,
        },
        "probabilities": {
            "exact": p_exact,
            "near_only": p_near_only,
            "similar_only": p_similar_only,
            "miss": p_miss,
        },
    }

    return (
        p_exact,
        p_near_only,
        p_similar_only,
        p_miss,
        SCRIPT_VERSION,
        "exclusive Poisson null from calibrated USGS target-band rates",
        Json(meta),
        prediction.prediction_id,
    )


def update_predictions(cur, params: list[tuple[Any, ...]], batch_size: int) -> None:
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
        params,
        page_size=batch_size,
    )


def main() -> int:
    args = parse_args()
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        print(f"Missing DSN env var: {args.dsn_env}", file=sys.stderr)
        return 2

    run_key = args.run_key or generate_run_key()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    stage5_run_id: int | None = None

    try:
        with conn.cursor() as cur:
            stage2_run_id, resolved_stage2_run_key = fetch_run(cur, "stage2_eligibility", args.stage2_run_key)
            stage4_run_id, resolved_stage4_run_key = fetch_run(cur, "stage4_match_scoring", args.stage4_run_key)
            predictions = fetch_predictions(cur, stage2_run_id, args.limit)

            if not args.dry_run:
                stage5_run_id = insert_run(
                    cur,
                    run_key,
                    {
                        "stage2_run_key": resolved_stage2_run_key,
                        "stage4_run_key": resolved_stage4_run_key,
                        "family": "earthquake",
                        "limit": args.limit,
                    },
                    args.notes,
                )
                conn.commit()

        updates: list[tuple[Any, ...]] = []
        summary_counter = {"probability_ready": 0, "probability_unresolved": 0, "query_failures": 0}

        for prediction in predictions:
            if prediction.target_lat is None or prediction.target_lon is None:
                summary_counter["probability_unresolved"] += 1
                updates.append(build_probability_update(prediction, None, "target unresolved for probability model"))
                continue

            try:
                features = fetch_calibration_events(prediction)
            except Exception as exc:
                summary_counter["query_failures"] += 1
                updates.append(build_probability_update(prediction, None, f"usgs calibration query failed: {str(exc)[:300]}"))
                continue

            calibration_counts = classify_calibration_events(prediction, features)
            updates.append(build_probability_update(prediction, calibration_counts, "computed"))
            summary_counter["probability_ready"] += 1

        if not args.dry_run:
            for index in range(0, len(updates), args.batch_size):
                batch = updates[index : index + args.batch_size]
                with conn.cursor() as cur:
                    update_predictions(cur, batch, args.batch_size)
                conn.commit()

            with conn.cursor() as cur:
                update_run(
                    cur,
                    stage5_run_id,
                    "completed",
                    {
                        "stage2_run_key": resolved_stage2_run_key,
                        "stage4_run_key": resolved_stage4_run_key,
                        "scored_predictions": len(predictions),
                        **summary_counter,
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
                    "stage4_run_key": resolved_stage4_run_key,
                    "scored_predictions": len(predictions),
                    **summary_counter,
                    "script_version": SCRIPT_VERSION,
                },
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        conn.rollback()
        if not args.dry_run and stage5_run_id is not None:
            with conn.cursor() as cur:
                update_run(cur, stage5_run_id, "failed", {"error": str(exc)[:1000], "script_version": SCRIPT_VERSION})
            conn.commit()
        print(f"Stage 5 earthquake probability assignment failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

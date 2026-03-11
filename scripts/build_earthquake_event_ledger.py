#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2
import requests
from psycopg2.extras import Json, execute_values

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


SCRIPT_VERSION = "stage3_earthquake_usgs_v1"
USGS_QUERY_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
USGS_DOC_URL = "https://earthquake.usgs.gov/fdsnws/event/1/"
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "billymeierposts-stage3-earthquake/1.0 (+https://github.com/pleachcx/billymeierposts)",
}
DATE_PHRASE_PATTERN = re.compile(
    r"""
    \b(
        on\s+the\s+\d{1,2}(?:st|nd|rd|th)?\s+of\s+[A-Z][a-z]+(?:,\s*\d{4})?|
        on\s+[A-Z][a-z]+\s+\d{1,2},?\s+\d{4}|
        tomorrow|
        next\s+week|
        next\s+month|
        next\s+year|
        later\s+this\s+year|
        middle\s+of\s+this\s+year|
        in\s+\d{4}|
        by\s+\d{4}
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)


@dataclass
class PredictionRow:
    prediction_id: int
    bundle_key: str | None
    bundle_role: str
    report_number: int
    candidate_seq: int
    claim_normalized: str
    claimed_contact_date: date
    time_window_start: date
    time_window_end: date
    target_name: str | None
    target_type: str | None
    target_lat: float | None
    target_lon: float | None
    target_radius_km: float | None
    magnitude_min: float | None
    magnitude_max: float | None
    severity_band: str | None
    stage2_label: str
    stage2_meta: dict[str, Any]


@dataclass
class ResolvedTarget:
    canonical_name: str
    lat: float
    lon: float
    target_type: str
    radius_km: float
    resolution_source: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 3 earthquake event-ledger builder using the USGS catalog.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key. Defaults to latest completed Stage 2 run.")
    parser.add_argument("--run-key", help="Unique Stage 3 run key. Defaults to an auto-generated timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument("--limit", type=int, help="Limit prediction rows processed.")
    parser.add_argument("--only-significant", action="store_true", help="Restrict to significant predictions only.")
    parser.add_argument(
        "--match-statuses",
        default="unreviewed",
        help="Comma-separated prediction match statuses to rebuild, or 'all'. Defaults to 'unreviewed'.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Resolve and query but do not write DB rows.")
    parser.add_argument("--batch-size", type=int, default=500, help="Ledger insert batch size.")
    parser.add_argument(
        "--overrides-path",
        default=str(REPO_ROOT / "data" / "earthquake_location_overrides.json"),
        help="Path to the local earthquake location override JSON.",
    )
    parser.add_argument(
        "--prediction-overrides-path",
        default=str(REPO_ROOT / "data" / "earthquake_prediction_overrides.json"),
        help="Path to the per-prediction earthquake target override JSON.",
    )
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage3-earthquake-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_match_statuses(raw_value: str) -> list[str] | None:
    value = (raw_value or "").strip().lower()
    if not value or value == "all":
        return None

    allowed_statuses = {
        "unreviewed",
        "exact_hit",
        "near_hit",
        "similar_only",
        "miss",
        "unresolved",
    }
    statuses = [part.strip().lower() for part in value.split(",") if part.strip()]
    invalid = [status for status in statuses if status not in allowed_statuses]
    if invalid:
        raise ValueError(f"Unsupported match statuses: {', '.join(sorted(set(invalid)))}")
    if not statuses:
        raise ValueError("At least one match status is required when --match-statuses is provided.")
    return statuses


def fetch_stage2_run(cur, stage2_run_key: str | None) -> tuple[int, str]:
    if stage2_run_key:
        cur.execute(
            """
            SELECT id, run_key
            FROM public.prediction_audit_runs
            WHERE run_key = %s AND stage = 'stage2_eligibility'
            """,
            (stage2_run_key,),
        )
    else:
        cur.execute(
            """
            SELECT id, run_key
            FROM public.prediction_audit_runs
            WHERE stage = 'stage2_eligibility' AND status = 'completed'
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("No completed Stage 2 run found.")
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
        VALUES (%s, 'stage3_event_ledger', 'running', %s, %s, %s, %s, %s, %s, now())
        RETURNING id
        """,
        (
            run_key,
            SCRIPT_VERSION,
            "none",
            "public.prediction_audit_predictions",
            Json(source_filter),
            notes or None,
            Json({"reference_source": "usgs_fdsn_event_v1"}),
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


def fetch_predictions(
    cur,
    stage2_run_id: int,
    only_significant: bool,
    match_statuses: list[str] | None,
    limit: int | None,
) -> list[PredictionRow]:
    label_clause = "AND p.stage2_label = 'significant_prediction'" if only_significant else "AND p.stage2_label IN ('eligible_prediction','significant_prediction')"
    status_clause = "AND p.match_status = ANY(%s)" if match_statuses else ""
    sql = f"""
        SELECT
            p.id,
            p.bundle_key,
            p.bundle_role,
            p.report_number,
            p.candidate_seq,
            p.claim_normalized,
            p.claimed_contact_date,
            p.time_window_start,
            p.time_window_end,
            p.target_name,
            p.target_type,
            p.target_lat,
            p.target_lon,
            p.target_radius_km,
            p.magnitude_min,
            p.magnitude_max,
            p.severity_band,
            p.stage2_label,
            p.stage2_meta
        FROM public.prediction_audit_predictions p
        WHERE p.last_stage2_run_id = %s
          AND p.event_family_final = 'earthquake'
          {label_clause}
          {status_clause}
          AND p.time_window_start IS NOT NULL
          AND p.time_window_end IS NOT NULL
        ORDER BY p.time_window_start, p.report_number, p.candidate_seq
    """
    params: list[Any] = [stage2_run_id]
    if match_statuses:
        params.append(match_statuses)
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    cur.execute(sql, params)
    rows = []
    for row in cur.fetchall():
        rows.append(
            PredictionRow(
                prediction_id=row[0],
                bundle_key=row[1],
                bundle_role=row[2],
                report_number=row[3],
                candidate_seq=row[4],
                claim_normalized=row[5],
                claimed_contact_date=row[6],
                time_window_start=row[7],
                time_window_end=row[8],
                target_name=row[9],
                target_type=row[10],
                target_lat=float(row[11]) if row[11] is not None else None,
                target_lon=float(row[12]) if row[12] is not None else None,
                target_radius_km=float(row[13]) if row[13] is not None else None,
                magnitude_min=float(row[14]) if row[14] is not None else None,
                magnitude_max=float(row[15]) if row[15] is not None else None,
                severity_band=row[16],
                stage2_label=row[17],
                stage2_meta=row[18] or {},
            )
        )
    return rows


def load_overrides(path: str) -> dict[str, dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def load_prediction_overrides(path: str) -> dict[str, dict[str, Any]]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def build_alias_lookup(overrides: dict[str, dict[str, Any]]) -> list[tuple[str, str]]:
    alias_pairs: list[tuple[str, str]] = []
    for canonical_name, payload in overrides.items():
        alias_pairs.append((canonical_name.lower(), canonical_name))
        for alias in payload.get("aliases", []):
            alias_pairs.append((alias.lower(), canonical_name))
    alias_pairs.sort(key=lambda item: len(item[0]), reverse=True)
    return alias_pairs


def resolve_target(
    prediction: PredictionRow,
    overrides: dict[str, dict[str, Any]],
    alias_lookup: list[tuple[str, str]],
    prediction_overrides: dict[str, dict[str, Any]],
) -> ResolvedTarget | None:
    prediction_key = f"{prediction.report_number}:{prediction.candidate_seq}"
    override = prediction_overrides.get(prediction_key)
    if override:
        return ResolvedTarget(
            canonical_name=override["target_name"],
            lat=float(override["lat"]),
            lon=float(override["lon"]),
            target_type=override.get("target_type", "region"),
            radius_km=float(override.get("radius_km", 100.0)),
            resolution_source=override.get("resolution_source", "prediction_override"),
        )

    if prediction.target_lat is not None and prediction.target_lon is not None:
        return ResolvedTarget(
            canonical_name=prediction.target_name or "manual_coordinates",
            lat=prediction.target_lat,
            lon=prediction.target_lon,
            target_type=prediction.target_type or "point",
            radius_km=prediction.target_radius_km or 25.0,
            resolution_source="prediction_row",
        )

    def from_override(name: str | None) -> ResolvedTarget | None:
        if not name:
            return None
        for alias, canonical in alias_lookup:
            if name.lower() == alias:
                payload = overrides[canonical]
                return ResolvedTarget(
                    canonical_name=canonical,
                    lat=float(payload["lat"]),
                    lon=float(payload["lon"]),
                    target_type=payload["target_type"],
                    radius_km=float(payload["radius_km"]),
                    resolution_source="target_name_override",
                )
        return None

    direct = from_override(prediction.target_name)
    if direct:
        return direct

    lower_claim = prediction.claim_normalized.lower()
    for alias, canonical in alias_lookup:
        if alias in lower_claim:
            payload = overrides[canonical]
            return ResolvedTarget(
                canonical_name=canonical,
                lat=float(payload["lat"]),
                lon=float(payload["lon"]),
                target_type=payload["target_type"],
                radius_km=float(payload["radius_km"]),
                resolution_source=f"claim_alias:{alias}",
            )
    return None


def is_compound_claim(prediction: PredictionRow, alias_lookup: list[tuple[str, str]]) -> bool:
    if prediction.bundle_role == "compound_child":
        return False
    lower_claim = prediction.claim_normalized.lower()
    date_hits = DATE_PHRASE_PATTERN.findall(prediction.claim_normalized)
    event_hits = re.findall(r"\b(?:earthquake|seaquake|quake|volcano|eruption)\b", lower_claim)
    if len(date_hits) > 1 and (" while " in lower_claim or " followed by " in lower_claim or " after which " in lower_claim or " as well as " in lower_claim):
        return True
    if len(event_hits) > 1 and (" while " in lower_claim or " followed by " in lower_claim or " after which " in lower_claim or " as well as " in lower_claim):
        return True
    return False


def grace_days(start: date, end: date) -> int:
    window_days = (end - start).days
    if window_days <= 1:
        return 7
    if window_days <= 31:
        return 31
    return min(365, max(1, math.ceil(window_days * 0.25)))


def classify_target_radii(target: ResolvedTarget) -> tuple[float, float, float]:
    if target.target_type in {"point"}:
        return 25.0, 50.0, 100.0
    exact = target.radius_km
    return exact, exact + 50.0, exact + 100.0


def band_min_magnitude(prediction: PredictionRow) -> float:
    if prediction.magnitude_min is not None:
        return max(0.0, prediction.magnitude_min - 1.0)
    if prediction.severity_band == "devastating":
        return 6.5
    if prediction.severity_band == "severe":
        return 6.0
    return 5.5


def band_for_event_magnitude(prediction: PredictionRow, event_mag: float | None) -> tuple[bool, bool]:
    if event_mag is None:
        return False, False
    if prediction.magnitude_min is not None and prediction.magnitude_max is not None:
        target_mag = (prediction.magnitude_min + prediction.magnitude_max) / 2
        exact = abs(event_mag - target_mag) <= 0.5
        near = abs(event_mag - target_mag) <= 1.0
        return exact, near

    if prediction.severity_band == "devastating":
        if event_mag >= 7.5:
            return True, True
        return False, event_mag >= 6.5
    if prediction.severity_band == "severe":
        if event_mag >= 6.0:
            return True, True
        return False, event_mag >= 5.5
    if prediction.severity_band == "strong":
        if event_mag >= 5.5:
            return True, True
        return False, event_mag >= 5.0
    return True, True


def query_window(prediction: PredictionRow) -> tuple[date, date, date, date]:
    grace = grace_days(prediction.time_window_start, prediction.time_window_end)
    near_start = prediction.time_window_start - timedelta(days=grace)
    near_end = prediction.time_window_end + timedelta(days=grace)
    log_start = near_start - timedelta(days=grace)
    log_end = near_end + timedelta(days=grace)
    return near_start, near_end, log_start, log_end


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return radius * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def event_date_from_feature(feature: dict[str, Any]) -> date:
    millis = feature["properties"]["time"]
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).date()


def distance_days_to_window(event_day: date, start: date, end: date) -> int:
    if start <= event_day <= end:
        return 0
    if event_day < start:
        return (start - event_day).days
    return (event_day - end).days


def fetch_usgs_events(prediction: PredictionRow, target: ResolvedTarget) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _, _, log_start, log_end = query_window(prediction)
    _, _, log_radius = classify_target_radii(target)
    params = {
        "format": "geojson",
        "orderby": "time-asc",
        "starttime": log_start.isoformat(),
        "endtime": log_end.isoformat(),
        "latitude": target.lat,
        "longitude": target.lon,
        "maxradiuskm": round(log_radius, 2),
        "minmagnitude": band_min_magnitude(prediction),
        "limit": 2000,
    }
    response = requests.get(USGS_QUERY_URL, params=params, headers=DEFAULT_HEADERS, timeout=30)
    response.raise_for_status()
    payload = response.json()
    return payload.get("features", []), {"query_url": response.url, "metadata": payload.get("metadata", {})}


def feature_to_ledger_row(prediction: PredictionRow, ledger_run_id: int, target: ResolvedTarget, feature: dict[str, Any], query_meta: dict[str, Any]) -> tuple[Any, ...] | None:
    coords = feature.get("geometry", {}).get("coordinates") or [None, None]
    lon = coords[0]
    lat = coords[1]
    if lat is None or lon is None:
        return None

    event_mag = feature["properties"].get("mag")
    event_day = event_date_from_feature(feature)
    exact_radius, near_radius, log_radius = classify_target_radii(target)
    near_start, near_end, log_start, log_end = query_window(prediction)
    distance_km = round(haversine_km(target.lat, target.lon, lat, lon), 2)
    delta_days = distance_days_to_window(event_day, prediction.time_window_start, prediction.time_window_end)
    exact_mag_ok, near_mag_ok = band_for_event_magnitude(prediction, float(event_mag) if event_mag is not None else None)

    exact_band = (
        prediction.time_window_start <= event_day <= prediction.time_window_end
        and distance_km <= exact_radius
        and exact_mag_ok
    )
    near_band = (
        near_start <= event_day <= near_end
        and distance_km <= near_radius
        and near_mag_ok
        and not exact_band
    )
    log_only_band = (
        log_start <= event_day <= log_end
        and distance_km <= log_radius
        and not exact_band
        and not near_band
    )

    if not any([exact_band, near_band, log_only_band]):
        return None

    return (
        prediction.prediction_id,
        ledger_run_id,
        "usgs_fdsn_event_v1",
        USGS_DOC_URL,
        feature["id"],
        "earthquake",
        feature["properties"].get("title"),
        event_day,
        event_day,
        feature["properties"].get("place"),
        lat,
        lon,
        distance_km,
        delta_days,
        event_mag,
        None,
        exact_band,
        near_band,
        log_only_band,
        feature["properties"].get("url") or query_meta.get("query_url"),
        feature["properties"].get("place"),
        Json(feature),
    )


def persist_target_resolution(cur, prediction_id: int, target: ResolvedTarget) -> None:
    cur.execute(
        """
        UPDATE public.prediction_audit_predictions
        SET target_name = %s,
            target_type = %s,
            target_lat = %s,
            target_lon = %s,
            target_radius_km = %s,
            stage2_meta = COALESCE(stage2_meta, '{}'::jsonb) || %s::jsonb
        WHERE id = %s
        """,
        (
            target.canonical_name,
            target.target_type,
            target.lat,
            target.lon,
            target.radius_km,
            json.dumps(
                {
                    "target_resolution": {
                        "canonical_name": target.canonical_name,
                        "lat": target.lat,
                        "lon": target.lon,
                        "target_type": target.target_type,
                        "radius_km": target.radius_km,
                        "source": target.resolution_source,
                    }
                }
            ),
            prediction_id,
        ),
    )


def persist_event_rows(cur, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    execute_values(
        cur,
        """
        INSERT INTO public.prediction_audit_event_ledger (
            prediction_id,
            ledger_run_id,
            source_name,
            source_version,
            external_event_id,
            event_family,
            event_title,
            event_start_date,
            event_end_date,
            location_name,
            latitude,
            longitude,
            distance_km,
            time_delta_days,
            magnitude_value,
            severity_band,
            exact_band,
            near_band,
            log_only_band,
            source_url,
            source_excerpt,
            raw_event
        )
        VALUES %s
        """,
        rows,
    )


def main() -> int:
    args = parse_args()
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        print(f"Missing DSN env var: {args.dsn_env}", file=sys.stderr)
        return 2

    overrides = load_overrides(args.overrides_path)
    prediction_overrides = load_prediction_overrides(args.prediction_overrides_path)
    alias_lookup = build_alias_lookup(overrides)
    match_statuses = parse_match_statuses(args.match_statuses)
    run_key = args.run_key or generate_run_key()

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    stage3_run_id: int | None = None

    try:
        with conn.cursor() as cur:
            stage2_run_id, resolved_stage2_run_key = fetch_stage2_run(cur, args.stage2_run_key)
            predictions = fetch_predictions(cur, stage2_run_id, args.only_significant, match_statuses, args.limit)

            if not args.dry_run:
                stage3_run_id = insert_run(
                    cur,
                    run_key,
                    {
                        "stage2_run_key": resolved_stage2_run_key,
                        "only_significant": args.only_significant,
                        "match_statuses": match_statuses or ["all"],
                        "limit": args.limit,
                        "overrides_path": args.overrides_path,
                        "prediction_overrides_path": args.prediction_overrides_path,
                        "family": "earthquake",
                    },
                    args.notes,
                )
                conn.commit()

        processed = 0
        resolved = 0
        unresolved = 0
        compound_skipped = 0
        queried = 0
        query_failures = 0
        total_ledger_rows = 0
        ledger_rows: list[tuple[Any, ...]] = []
        unresolved_rows: list[dict[str, Any]] = []

        for prediction in predictions:
            processed += 1
            if is_compound_claim(prediction, alias_lookup):
                compound_skipped += 1
                unresolved_rows.append(
                    {
                        "prediction_id": prediction.prediction_id,
                        "report_number": prediction.report_number,
                        "reason": "compound_claim",
                    }
                )
                continue

            target = resolve_target(prediction, overrides, alias_lookup, prediction_overrides)
            if not target:
                unresolved += 1
                unresolved_rows.append(
                    {
                        "prediction_id": prediction.prediction_id,
                        "report_number": prediction.report_number,
                        "reason": "unresolved_target",
                        "target_name": prediction.target_name,
                    }
                )
                continue

            resolved += 1
            if not args.dry_run and stage3_run_id is not None:
                with conn.cursor() as cur:
                    persist_target_resolution(cur, prediction.prediction_id, target)
                conn.commit()

            try:
                features, query_meta = fetch_usgs_events(prediction, target)
                queried += 1
            except Exception as exc:
                query_failures += 1
                unresolved_rows.append(
                    {
                        "prediction_id": prediction.prediction_id,
                        "report_number": prediction.report_number,
                        "reason": "usgs_query_failed",
                        "error": str(exc)[:400],
                    }
                )
                continue

            for feature in features:
                row = feature_to_ledger_row(prediction, stage3_run_id or 0, target, feature, query_meta)
                if row:
                    total_ledger_rows += 1
                    if args.dry_run:
                        continue
                    ledger_rows.append(row)
                if not args.dry_run and len(ledger_rows) >= args.batch_size:
                    with conn.cursor() as cur:
                        persist_event_rows(cur, ledger_rows)
                    conn.commit()
                    ledger_rows.clear()

        if not args.dry_run and ledger_rows:
            with conn.cursor() as cur:
                persist_event_rows(cur, ledger_rows)
            conn.commit()

        summary = {
            "stage2_run_key": resolved_stage2_run_key,
            "processed_predictions": processed,
            "resolved_targets": resolved,
            "unresolved_targets": unresolved,
            "compound_skipped": compound_skipped,
            "usgs_queries_completed": queried,
            "usgs_query_failures": query_failures,
            "inserted_ledger_rows": total_ledger_rows if args.dry_run else None,
            "unresolved_examples": unresolved_rows[:20],
            "script_version": SCRIPT_VERSION,
            "reference_source": "usgs_fdsn_event_v1",
        }

        if not args.dry_run and stage3_run_id is not None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*)
                    FROM public.prediction_audit_event_ledger
                    WHERE ledger_run_id = %s
                    """,
                    (stage3_run_id,),
                )
                total_inserted = cur.fetchone()[0]
                summary["inserted_ledger_rows"] = total_inserted
                update_run(cur, stage3_run_id, "completed", summary)
            conn.commit()

        print(
            json.dumps(
                {
                    "run_key": run_key,
                    "dry_run": args.dry_run,
                    **summary,
                },
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        conn.rollback()
        if not args.dry_run and stage3_run_id is not None:
            with conn.cursor() as cur:
                update_run(
                    cur,
                    stage3_run_id,
                    "failed",
                    {"error": str(exc)[:1000], "script_version": SCRIPT_VERSION},
                )
            conn.commit()
        print(f"Stage 3 earthquake ledger failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

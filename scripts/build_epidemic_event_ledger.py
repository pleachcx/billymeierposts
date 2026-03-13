#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import Json, execute_batch, execute_values


SCRIPT_VERSION = "stage3_epidemic_official_catalog_v2"
REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class PredictionRow:
    prediction_id: int
    report_number: int
    candidate_seq: int
    claim_normalized: str
    claimed_contact_date: date
    time_window_start: date | None
    time_window_end: date | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 3 epidemic event-ledger builder from curated official events.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key. Defaults to latest completed Stage 2 run.")
    parser.add_argument("--run-key", help="Unique Stage 3 epidemic run key. Defaults to an auto-generated key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument("--dry-run", action="store_true", help="Resolve predictions without writing ledger rows.")
    parser.add_argument(
        "--events-path",
        default=str(REPO_ROOT / "data" / "epidemic_official_events.json"),
        help="Path to curated official epidemic events JSON.",
    )
    parser.add_argument(
        "--overrides-path",
        default=str(REPO_ROOT / "data" / "epidemic_prediction_overrides.json"),
        help="Path to epidemic prediction override JSON keyed by report_number:candidate_seq.",
    )
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage3-epidemic-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def fetch_run(cur, stage: str, run_key: str | None) -> tuple[int, str]:
    if run_key:
        cur.execute(
            """
            SELECT id, run_key
            FROM public.prediction_audit_runs
            WHERE stage = %s AND run_key = %s
            """,
            (stage, run_key),
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
            Json({"family": "epidemic", "source": "curated_official_catalog"}),
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


def fetch_predictions(cur, stage2_run_id: int, override_keys: list[str]) -> list[PredictionRow]:
    pairs = []
    for key in override_keys:
        report_number, candidate_seq = key.split(":")
        pairs.append((int(report_number), int(candidate_seq)))

    cur.execute(
        """
        SELECT
            id,
            report_number,
            candidate_seq,
            claim_normalized,
            claimed_contact_date,
            time_window_start,
            time_window_end
        FROM public.prediction_audit_predictions
        WHERE last_stage2_run_id = %s
          AND (report_number, candidate_seq) IN %s
        ORDER BY report_number, candidate_seq
        """,
        (stage2_run_id, tuple(pairs)),
    )
    rows = []
    for row in cur.fetchall():
        rows.append(
            PredictionRow(
                prediction_id=row[0],
                report_number=row[1],
                candidate_seq=row[2],
                claim_normalized=row[3],
                claimed_contact_date=row[4],
                time_window_start=row[5],
                time_window_end=row[6],
            )
        )
    return rows


def apply_scoped_overrides(cur, stage2_run_id: int, overrides: dict[str, dict[str, Any]]) -> None:
    params = []
    for key in overrides:
        report_number, candidate_seq = key.split(":")
        params.append(
            (
                "epidemic",
                Json(
                    {
                        "manual_scope_override": {
                            "script_version": SCRIPT_VERSION,
                            "scoped_family": "epidemic",
                            "override_key": key,
                        }
                    }
                ),
                stage2_run_id,
                int(report_number),
                int(candidate_seq),
            )
        )
    if not params:
        return
    execute_batch(
        cur,
        """
        UPDATE public.prediction_audit_predictions
        SET event_family_final = %s,
            stage2_meta = COALESCE(stage2_meta, '{}'::jsonb) || %s
        WHERE last_stage2_run_id = %s
          AND report_number = %s
          AND candidate_seq = %s
        """,
        params,
        page_size=100,
    )


def parse_iso_date(raw: str | None) -> date | None:
    if not raw:
        return None
    return date.fromisoformat(raw)


def target_matches(event_keywords: list[str], target_keywords: list[str]) -> bool:
    event_lower = {keyword.lower() for keyword in event_keywords}
    for keyword in target_keywords:
        keyword_lower = keyword.lower()
        if keyword_lower in event_lower:
            return True
    return False


def classify_bands(event_date: date, window_start: date | None, window_end: date | None, exact_target: bool, expected_type: str, event_type: str) -> tuple[bool, bool, bool, int | None]:
    if window_start is None or window_end is None:
        return False, False, expected_type == event_type or exact_target, None

    if window_start <= event_date <= window_end and expected_type == event_type and exact_target:
        return True, False, True, 0

    if event_date < window_start:
        delta_days = (window_start - event_date).days
    elif event_date > window_end:
        delta_days = (event_date - window_end).days
    else:
        delta_days = 0

    if delta_days <= 31 and expected_type == event_type and exact_target:
        return False, True, True, delta_days

    log_only = expected_type == event_type or exact_target
    return False, False, log_only, delta_days


def build_ledger_rows(
    predictions: list[PredictionRow],
    overrides: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
    run_id: int,
) -> tuple[list[tuple[Any, ...]], list[dict[str, Any]]]:
    rows: list[tuple[Any, ...]] = []
    skipped: list[dict[str, Any]] = []

    for prediction in predictions:
        override = overrides.get(f"{prediction.report_number}:{prediction.candidate_seq}")
        if not override:
            skipped.append({"prediction_id": prediction.prediction_id, "reason": "missing_override"})
            continue

        disease_key = override["disease_key"]
        expected_type = override["expected_event_type"]
        target_keywords = override.get("target_keywords", [])
        window_start = parse_iso_date(override.get("window_start")) or prediction.time_window_start
        window_end = parse_iso_date(override.get("window_end")) or prediction.time_window_end

        matched_disease_event = False
        matched_post_claim_event = False
        for event in events:
            if event["disease_key"] != disease_key:
                continue
            matched_disease_event = True
            event_date = parse_iso_date(event["event_start_date"])
            if event_date is None:
                continue
            if prediction.claimed_contact_date and event_date < prediction.claimed_contact_date:
                continue
            matched_post_claim_event = True
            exact_target = target_matches(event.get("target_keywords", []), target_keywords)
            exact_band, near_band, log_only_band, delta_days = classify_bands(
                event_date,
                window_start,
                window_end,
                exact_target,
                expected_type,
                event["event_type"],
            )
            rows.append(
                (
                    prediction.prediction_id,
                    run_id,
                    event["source_name"],
                    SCRIPT_VERSION,
                    event["event_id"],
                    "epidemic",
                    event["event_title"],
                    event["event_start_date"],
                    event["event_start_date"],
                    event["location_name"],
                    None,
                    None,
                    None,
                    delta_days,
                    None,
                    event["event_type"],
                    exact_band,
                    near_band,
                    log_only_band,
                    event["source_url"],
                    None,
                    Json(
                        {
                            "disease_key": disease_key,
                            "expected_event_type": expected_type,
                            "target_name": override.get("target_name"),
                            "target_keywords": target_keywords,
                            "effective_window_start": window_start.isoformat() if window_start else None,
                            "effective_window_end": window_end.isoformat() if window_end else None,
                            "official_event": event,
                        }
                    ),
                )
            )

        if not matched_disease_event:
            skipped.append({"prediction_id": prediction.prediction_id, "reason": "no_catalog_events_for_disease"})
        elif not matched_post_claim_event:
            skipped.append({"prediction_id": prediction.prediction_id, "reason": "no_post_claim_catalog_events"})

    return rows, skipped


def persist_rows(cur, rows: list[tuple[Any, ...]]) -> None:
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

    overrides = load_json(args.overrides_path)
    events = load_json(args.events_path)
    run_key = args.run_key or generate_run_key()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    run_id: int | None = None

    try:
        with conn.cursor() as cur:
            stage2_run_id, resolved_stage2_run_key = fetch_run(cur, "stage2_eligibility", args.stage2_run_key)
            if not args.dry_run:
                apply_scoped_overrides(cur, stage2_run_id, overrides)
            predictions = fetch_predictions(cur, stage2_run_id, list(overrides.keys()))

            if not args.dry_run:
                run_id = insert_run(
                    cur,
                    run_key,
                    {
                        "stage2_run_key": resolved_stage2_run_key,
                        "family": "epidemic",
                        "events_path": args.events_path,
                        "overrides_path": args.overrides_path,
                        "scope": "named_disease_catalog_v1",
                    },
                    args.notes,
                )
                conn.commit()

        rows, skipped = build_ledger_rows(predictions, overrides, events, run_id or 0)

        if not args.dry_run and rows:
            with conn.cursor() as cur:
                persist_rows(cur, rows)
                update_run(
                    cur,
                    run_id,
                    "completed",
                    {
                        "stage2_run_key": resolved_stage2_run_key,
                        "scoped_predictions": len(predictions),
                        "inserted_ledger_rows": len(rows),
                        "skipped_predictions": skipped,
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
                    "scoped_predictions": len(predictions),
                    "inserted_ledger_rows": len(rows),
                    "skipped_predictions": skipped,
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
        print(f"Stage 3 epidemic ledger failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

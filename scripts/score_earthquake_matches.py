#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import Json, execute_batch

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


SCRIPT_VERSION = "stage4_earthquake_match_v1"
REVIEWER = "script:stage4_earthquake_match_v1"
COMPOUND_PATTERN = re.compile(r"\b(after which|followed by|as well as|while)\b", re.IGNORECASE)


@dataclass
class Prediction:
    prediction_id: int
    parse_run_id: int
    bundle_key: str | None
    report_number: int
    candidate_seq: int
    claim_normalized: str
    target_name: str | None
    target_lat: float | None
    target_lon: float | None
    time_window_start: Any
    time_window_end: Any
    match_status: str


@dataclass
class LedgerRow:
    event_ledger_id: int
    prediction_id: int
    event_start_date: Any
    location_name: str | None
    magnitude_value: float | None
    distance_km: float | None
    time_delta_days: int | None
    exact_band: bool
    near_band: bool
    log_only_band: bool
    source_name: str
    source_url: str | None
    event_title: str | None


@dataclass
class MatchDecision:
    prediction_id: int
    best_event_ledger_id: int | None
    match_status: str
    confidence: float | None
    rationale: str
    review_meta: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 4 earthquake match scorer.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key. Defaults to latest completed Stage 2 run.")
    parser.add_argument("--stage3-run-key", help="Stage 3 earthquake ledger run key. Defaults to latest completed Stage 3 run.")
    parser.add_argument("--run-key", help="Unique Stage 4 run key. Defaults to a timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument("--limit", type=int, help="Limit predictions scored.")
    parser.add_argument("--dry-run", action="store_true", help="Score without writing reviews or updates.")
    parser.add_argument("--batch-size", type=int, default=200, help="Update batch size.")
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage4-earthquake-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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


def fetch_predictions(cur, stage2_run_id: int, limit: int | None) -> list[Prediction]:
    sql = """
        SELECT
            id,
            parse_run_id,
            bundle_key,
            report_number,
            candidate_seq,
            claim_normalized,
            target_name,
            target_lat,
            target_lon,
            time_window_start,
            time_window_end,
            match_status
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
    rows = []
    for row in cur.fetchall():
        rows.append(
            Prediction(
                prediction_id=row[0],
                parse_run_id=row[1],
                bundle_key=row[2],
                report_number=row[3],
                candidate_seq=row[4],
                claim_normalized=row[5],
                target_name=row[6],
                target_lat=float(row[7]) if row[7] is not None else None,
                target_lon=float(row[8]) if row[8] is not None else None,
                time_window_start=row[9],
                time_window_end=row[10],
                match_status=row[11],
            )
        )
    return rows


def fetch_ledger_rows(cur, stage3_run_id: int) -> dict[int, list[LedgerRow]]:
    cur.execute(
        """
        SELECT
            id,
            prediction_id,
            event_start_date,
            location_name,
            magnitude_value,
            distance_km,
            time_delta_days,
            exact_band,
            near_band,
            log_only_band,
            source_name,
            source_url,
            event_title
        FROM public.prediction_audit_event_ledger
        WHERE ledger_run_id = %s
        ORDER BY prediction_id, event_start_date, distance_km NULLS LAST
        """,
        (stage3_run_id,),
    )
    by_prediction: dict[int, list[LedgerRow]] = {}
    for row in cur.fetchall():
        by_prediction.setdefault(row[1], []).append(
            LedgerRow(
                event_ledger_id=row[0],
                prediction_id=row[1],
                event_start_date=row[2],
                location_name=row[3],
                magnitude_value=float(row[4]) if row[4] is not None else None,
                distance_km=float(row[5]) if row[5] is not None else None,
                time_delta_days=row[6],
                exact_band=row[7],
                near_band=row[8],
                log_only_band=row[9],
                source_name=row[10],
                source_url=row[11],
                event_title=row[12],
            )
        )
    return by_prediction


def band_rank(ledger_row: LedgerRow) -> int:
    if ledger_row.exact_band:
        return 0
    if ledger_row.near_band:
        return 1
    return 2


def sort_key(ledger_row: LedgerRow) -> tuple[Any, ...]:
    return (
        band_rank(ledger_row),
        ledger_row.time_delta_days if ledger_row.time_delta_days is not None else math.inf,
        ledger_row.distance_km if ledger_row.distance_km is not None else math.inf,
        -(ledger_row.magnitude_value or -999),
        ledger_row.event_start_date,
    )


def choose_best(ledger_rows: list[LedgerRow]) -> LedgerRow | None:
    if not ledger_rows:
        return None
    return sorted(ledger_rows, key=sort_key)[0]


def classify_prediction(prediction: Prediction, ledger_rows: list[LedgerRow]) -> MatchDecision:
    exact_rows = [row for row in ledger_rows if row.exact_band]
    near_rows = [row for row in ledger_rows if row.near_band]
    log_rows = [row for row in ledger_rows if row.log_only_band]

    review_meta: dict[str, Any] = {
        "script_version": SCRIPT_VERSION,
        "candidate_event_count": len(ledger_rows),
        "exact_candidate_count": len(exact_rows),
        "near_candidate_count": len(near_rows),
        "log_candidate_count": len(log_rows),
    }

    if exact_rows:
        best = choose_best(exact_rows)
        review_meta["selected_band"] = "exact"
        return MatchDecision(
            prediction_id=prediction.prediction_id,
            best_event_ledger_id=best.event_ledger_id,
            match_status="exact_hit",
            confidence=0.95,
            rationale=f"Exact-band earthquake match selected from {len(exact_rows)} candidates.",
            review_meta=review_meta,
        )

    if near_rows:
        best = choose_best(near_rows)
        review_meta["selected_band"] = "near"
        return MatchDecision(
            prediction_id=prediction.prediction_id,
            best_event_ledger_id=best.event_ledger_id,
            match_status="near_hit",
            confidence=0.85,
            rationale=f"No exact-band event found; best near-band earthquake selected from {len(near_rows)} candidates.",
            review_meta=review_meta,
        )

    if log_rows:
        best = choose_best(log_rows)
        review_meta["selected_band"] = "log_only"
        return MatchDecision(
            prediction_id=prediction.prediction_id,
            best_event_ledger_id=best.event_ledger_id,
            match_status="similar_only",
            confidence=0.6,
            rationale=f"Only log-only similar events were found ({len(log_rows)} candidates).",
            review_meta=review_meta,
        )

    if prediction.target_lat is None or prediction.target_lon is None:
        review_meta["selected_band"] = "none"
        review_meta["reason"] = "unresolved_target"
        return MatchDecision(
            prediction_id=prediction.prediction_id,
            best_event_ledger_id=None,
            match_status="unresolved",
            confidence=0.25,
            rationale="Prediction could not be scored because the target location remains unresolved.",
            review_meta=review_meta,
        )

    if COMPOUND_PATTERN.search(prediction.claim_normalized):
        review_meta["selected_band"] = "none"
        review_meta["reason"] = "compound_remainder"
        return MatchDecision(
            prediction_id=prediction.prediction_id,
            best_event_ledger_id=None,
            match_status="unresolved",
            confidence=0.3,
            rationale="Prediction still contains a compound remainder that needs finer atomic splitting before scoring.",
            review_meta=review_meta,
        )

    review_meta["selected_band"] = "none"
    review_meta["reason"] = "no_catalog_events"
    return MatchDecision(
        prediction_id=prediction.prediction_id,
        best_event_ledger_id=None,
        match_status="miss",
        confidence=0.8,
        rationale="No USGS earthquake events were found within the configured exact, near, or log-only bands.",
        review_meta=review_meta,
    )


def demote_existing_primary_reviews(cur, prediction_ids: list[int]) -> None:
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


def insert_match_reviews(cur, run_id: int, decisions: list[MatchDecision], batch_size: int) -> None:
    params = [
        (
            decision.prediction_id,
            decision.best_event_ledger_id,
            run_id,
            decision.match_status,
            True,
            REVIEWER,
            decision.confidence,
            decision.rationale,
            Json(decision.review_meta),
        )
        for decision in decisions
    ]
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        params,
        page_size=batch_size,
    )


def update_predictions(cur, decisions: list[MatchDecision], batch_size: int) -> None:
    params = [
        (decision.match_status, decision.best_event_ledger_id, decision.prediction_id)
        for decision in decisions
    ]
    execute_batch(
        cur,
        """
        UPDATE public.prediction_audit_predictions
        SET match_status = %s,
            best_event_ledger_id = %s
        WHERE id = %s
        """,
        params,
        page_size=batch_size,
    )


def roll_up_bundles(cur, parse_run_id: int, stage4_run_id: int) -> Counter[str]:
    cur.execute(
        """
        SELECT
            b.id,
            b.bundle_key,
            array_agg(p.match_status ORDER BY p.bundle_component_seq, p.candidate_seq) AS statuses
        FROM public.prediction_audit_bundles b
        JOIN public.prediction_audit_predictions p ON p.bundle_key = b.bundle_key
        WHERE b.parse_run_id = %s
        GROUP BY b.id, b.bundle_key
        """,
        (parse_run_id,),
    )
    updates: list[tuple[Any, ...]] = []
    counts: Counter[str] = Counter()
    for bundle_id, bundle_key, statuses in cur.fetchall():
        statuses = statuses or []
        status_counter = Counter(statuses)
        if any(status == "unreviewed" for status in statuses):
            bundle_status = "unreviewed"
        elif any(status == "unresolved" for status in statuses):
            bundle_status = "unresolved"
        elif statuses and all(status == "exact_hit" for status in statuses):
            bundle_status = "exact_hit"
        elif statuses and all(status in {"exact_hit", "near_hit"} for status in statuses):
            bundle_status = "near_hit"
        elif statuses and all(status == "similar_only" for status in statuses):
            bundle_status = "similar_only"
        elif statuses and all(status == "miss" for status in statuses):
            bundle_status = "miss"
        elif any(status in {"exact_hit", "near_hit", "similar_only"} for status in statuses):
            bundle_status = "partial_hit"
        else:
            bundle_status = "unresolved"

        counts[bundle_status] += 1
        updates.append(
            (
                stage4_run_id,
                bundle_status,
                Json(
                    {
                        "script_version": SCRIPT_VERSION,
                        "child_status_counts": dict(status_counter),
                        "bundle_key": bundle_key,
                    }
                ),
                bundle_id,
            )
        )

    execute_batch(
        cur,
        """
        UPDATE public.prediction_audit_bundles
        SET last_stage4_run_id = %s,
            bundle_match_status = %s,
            stage4_meta = %s
        WHERE id = %s
        """,
        updates,
        page_size=200,
    )
    return counts


def main() -> int:
    args = parse_args()
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        print(f"Missing DSN env var: {args.dsn_env}", file=sys.stderr)
        return 2

    run_key = args.run_key or generate_run_key()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    stage4_run_id: int | None = None

    try:
        with conn.cursor() as cur:
            stage2_run_id, resolved_stage2_run_key = fetch_run(cur, "stage2_eligibility", args.stage2_run_key)
            stage3_run_id, resolved_stage3_run_key = fetch_run(cur, "stage3_event_ledger", args.stage3_run_key)
            predictions = fetch_predictions(cur, stage2_run_id, args.limit)
            ledger_by_prediction = fetch_ledger_rows(cur, stage3_run_id)

            if not args.dry_run:
                stage4_run_id = insert_run(
                    cur,
                    run_key,
                    {
                        "stage2_run_key": resolved_stage2_run_key,
                        "stage3_run_key": resolved_stage3_run_key,
                        "family": "earthquake",
                        "limit": args.limit,
                    },
                    args.notes,
                )
                conn.commit()

        decisions = [classify_prediction(prediction, ledger_by_prediction.get(prediction.prediction_id, [])) for prediction in predictions]
        counts = Counter(decision.match_status for decision in decisions)

        bundle_counts: Counter[str] = Counter()
        if not args.dry_run and stage4_run_id is not None:
            prediction_ids = [prediction.prediction_id for prediction in predictions]
            with conn.cursor() as cur:
                demote_existing_primary_reviews(cur, prediction_ids)
                update_predictions(cur, decisions, args.batch_size)
                insert_match_reviews(cur, stage4_run_id, decisions, args.batch_size)
                parse_run_ids = {prediction.parse_run_id for prediction in predictions}
                for parse_run_id in parse_run_ids:
                    bundle_counts.update(roll_up_bundles(cur, parse_run_id, stage4_run_id))
            conn.commit()

            with conn.cursor() as cur:
                update_run(
                    cur,
                    stage4_run_id,
                    "completed",
                    {
                        "stage2_run_key": resolved_stage2_run_key,
                        "stage3_run_key": resolved_stage3_run_key,
                        "prediction_match_counts": dict(counts),
                        "bundle_match_counts": dict(bundle_counts),
                        "scored_predictions": len(decisions),
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
                    "stage3_run_key": resolved_stage3_run_key,
                    "scored_predictions": len(decisions),
                    "prediction_match_counts": dict(counts),
                    "bundle_match_counts": dict(bundle_counts),
                    "script_version": SCRIPT_VERSION,
                },
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        conn.rollback()
        if not args.dry_run and stage4_run_id is not None:
            with conn.cursor() as cur:
                update_run(cur, stage4_run_id, "failed", {"error": str(exc)[:1000], "script_version": SCRIPT_VERSION})
            conn.commit()
        print(f"Stage 4 earthquake scoring failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

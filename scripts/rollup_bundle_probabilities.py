#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2.extras import Json, execute_batch


SCRIPT_VERSION = "stage6_bundle_probability_rollup_v1"


@dataclass
class ChildPrediction:
    bundle_id: int
    bundle_key: str
    report_number: int
    prediction_id: int
    candidate_seq: int
    match_status: str
    p_exact: float | None
    p_near: float | None
    p_similar: float | None
    p_miss: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Roll up bundle-level probability summaries from child predictions.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage5-run-key", help="Stage 5 run key. Defaults to latest completed Stage 5 run.")
    parser.add_argument("--stage2-run-key", help="Optional Stage 2 run key override.")
    parser.add_argument("--event-family", default="earthquake", help="Scoped event family to roll up.")
    parser.add_argument("--run-key", help="Unique Stage 6 run key. Defaults to a timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument("--dry-run", action="store_true", help="Compute rollups without writing DB rows.")
    parser.add_argument("--batch-size", type=int, default=200, help="Insert batch size.")
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage6-bundle-rollup-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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
        VALUES (%s, 'stage6_bundle_probability_rollup', 'running', %s, %s, %s, %s, %s, %s, now())
        RETURNING id
        """,
        (
            run_key,
            SCRIPT_VERSION,
            "none",
            "public.prediction_audit_predictions",
            Json(source_filter),
            notes or None,
            Json({"model": "bundle_product_rollup"}),
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


def fetch_children(cur, stage2_run_id: int, event_family: str) -> list[ChildPrediction]:
    cur.execute(
        """
        SELECT
            b.id AS bundle_id,
            b.bundle_key,
            b.report_number,
            p.id AS prediction_id,
            p.candidate_seq,
            p.match_status,
            p.p_exact_under_null,
            p.p_near_under_null,
            p.p_similar_under_null,
            p.p_miss_under_null
        FROM public.prediction_audit_bundles b
        JOIN public.prediction_audit_predictions p ON p.bundle_key = b.bundle_key
        WHERE p.last_stage2_run_id = %s
          AND p.event_family_final = %s
          AND p.stage2_label IN ('eligible_prediction', 'significant_prediction')
          AND p.time_window_start IS NOT NULL
          AND p.time_window_end IS NOT NULL
        ORDER BY b.report_number, b.bundle_seq, p.bundle_component_seq, p.candidate_seq
        """,
        (stage2_run_id, event_family),
    )
    rows = []
    for row in cur.fetchall():
        rows.append(
            ChildPrediction(
                bundle_id=row[0],
                bundle_key=row[1],
                report_number=row[2],
                prediction_id=row[3],
                candidate_seq=row[4],
                match_status=row[5],
                p_exact=float(row[6]) if row[6] is not None else None,
                p_near=float(row[7]) if row[7] is not None else None,
                p_similar=float(row[8]) if row[8] is not None else None,
                p_miss=float(row[9]) if row[9] is not None else None,
            )
        )
    return rows


def product(values: list[float]) -> float | None:
    if not values:
        return None
    result = 1.0
    for value in values:
        result *= value
    return result


def log10_or_none(value: float | None) -> float | None:
    if value is None or value <= 0:
        return None
    return math.log10(value)


def scoped_bundle_status(statuses: list[str]) -> str:
    if any(status == "unreviewed" for status in statuses):
        return "unreviewed"
    if any(status == "unresolved" for status in statuses):
        return "unresolved"
    if statuses and all(status == "exact_hit" for status in statuses):
        return "exact_hit"
    if statuses and all(status in {"exact_hit", "near_hit"} for status in statuses):
        return "near_hit"
    if statuses and all(status in {"exact_hit", "near_hit", "similar_only"} for status in statuses):
        return "similar_only"
    if statuses and all(status == "miss" for status in statuses):
        return "miss"
    if any(status in {"exact_hit", "near_hit", "similar_only"} for status in statuses):
        return "partial_hit"
    return "unresolved"


def observed_probability(child: ChildPrediction) -> float | None:
    if child.match_status == "exact_hit":
        return child.p_exact
    if child.match_status == "near_hit":
        return child.p_near
    if child.match_status == "similar_only":
        return child.p_similar
    if child.match_status == "miss":
        return child.p_miss
    return None


def build_rollups(children: list[ChildPrediction], event_family: str) -> list[tuple[Any, ...]]:
    grouped: dict[int, list[ChildPrediction]] = defaultdict(list)
    for child in children:
        grouped[child.bundle_id].append(child)

    rows: list[tuple[Any, ...]] = []
    for bundle_id, group in sorted(grouped.items(), key=lambda item: (item[1][0].report_number, item[1][0].bundle_key)):
        statuses = [child.match_status for child in group]
        status_counts = Counter(statuses)
        observed_values = [value for value in (observed_probability(child) for child in group) if value is not None]
        all_exact_values = [child.p_exact for child in group if child.p_exact is not None]
        all_near_or_better_values = [
            child.p_exact + child.p_near
            for child in group
            if child.p_exact is not None and child.p_near is not None
        ]
        all_similar_or_better_values = [
            child.p_exact + child.p_near + child.p_similar
            for child in group
            if child.p_exact is not None and child.p_near is not None and child.p_similar is not None
        ]
        all_miss_values = [child.p_miss for child in group if child.p_miss is not None]

        p_observed = product(observed_values) if len(observed_values) == len(group) else None
        p_all_exact = product(all_exact_values) if len(all_exact_values) == len(group) else None
        p_all_near_or_better = product(all_near_or_better_values) if len(all_near_or_better_values) == len(group) else None
        p_all_similar_or_better = product(all_similar_or_better_values) if len(all_similar_or_better_values) == len(group) else None
        p_all_miss = product(all_miss_values) if len(all_miss_values) == len(group) else None

        meta = {
            "script_version": SCRIPT_VERSION,
            "bundle_key": group[0].bundle_key,
            "report_number": group[0].report_number,
            "child_prediction_ids": [child.prediction_id for child in group],
            "child_candidate_seqs": [child.candidate_seq for child in group],
            "child_match_statuses": statuses,
            "child_status_counts": dict(status_counts),
            "probability_ready_child_count": len(observed_values),
        }

        rows.append(
            (
                bundle_id,
                event_family,
                len(group),
                len(observed_values),
                scoped_bundle_status(statuses),
                Json(dict(status_counts)),
                p_observed,
                log10_or_none(p_observed),
                p_all_exact,
                log10_or_none(p_all_exact),
                p_all_near_or_better,
                log10_or_none(p_all_near_or_better),
                p_all_similar_or_better,
                log10_or_none(p_all_similar_or_better),
                p_all_miss,
                log10_or_none(p_all_miss),
                SCRIPT_VERSION,
                "bundle rollup from child null-model vectors",
                Json(meta),
            )
        )
    return rows


def insert_rollups(cur, rollup_run_id: int, stage5_run_id: int, rows: list[tuple[Any, ...]], batch_size: int) -> None:
    params = [
        (
            bundle_id,
            rollup_run_id,
            stage5_run_id,
            event_family,
            scoped_prediction_count,
            probability_ready_count,
            scoped_match_status,
            scoped_status_counts,
            p_observed,
            observed_log10,
            p_all_exact,
            all_exact_log10,
            p_all_near_or_better,
            all_near_or_better_log10,
            p_all_similar_or_better,
            all_similar_or_better_log10,
            p_all_miss,
            all_miss_log10,
            rollup_model_version,
            rollup_notes,
            rollup_meta,
        )
        for (
            bundle_id,
            event_family,
            scoped_prediction_count,
            probability_ready_count,
            scoped_match_status,
            scoped_status_counts,
            p_observed,
            observed_log10,
            p_all_exact,
            all_exact_log10,
            p_all_near_or_better,
            all_near_or_better_log10,
            p_all_similar_or_better,
            all_similar_or_better_log10,
            p_all_miss,
            all_miss_log10,
            rollup_model_version,
            rollup_notes,
            rollup_meta,
        ) in rows
    ]

    execute_batch(
        cur,
        """
        INSERT INTO public.prediction_audit_bundle_rollups (
            bundle_id,
            rollup_run_id,
            stage5_run_id,
            event_family,
            scoped_prediction_count,
            probability_ready_count,
            scoped_match_status,
            scoped_status_counts,
            p_observed_under_null,
            observed_log10_under_null,
            p_all_exact_under_null,
            all_exact_log10_under_null,
            p_all_near_or_better_under_null,
            all_near_or_better_log10_under_null,
            p_all_similar_or_better_under_null,
            all_similar_or_better_log10_under_null,
            p_all_miss_under_null,
            all_miss_log10_under_null,
            rollup_model_version,
            rollup_notes,
            rollup_meta
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    rollup_run_id: int | None = None

    try:
        with conn.cursor() as cur:
            stage5_run_id, resolved_stage5_run_key, stage5_filter = fetch_run(cur, "stage5_probability_model", args.stage5_run_key)
            stage2_run_key = args.stage2_run_key or stage5_filter.get("stage2_run_key")
            if not stage2_run_key:
                raise RuntimeError("Could not infer Stage 2 run key from Stage 5 run metadata.")
            stage2_run_id, resolved_stage2_run_key, _ = fetch_run(cur, "stage2_eligibility", stage2_run_key)
            children = fetch_children(cur, stage2_run_id, args.event_family)

            if not args.dry_run:
                rollup_run_id = insert_run(
                    cur,
                    run_key,
                    {
                        "stage2_run_key": resolved_stage2_run_key,
                        "stage5_run_key": resolved_stage5_run_key,
                        "event_family": args.event_family,
                    },
                    args.notes,
                )
                conn.commit()

        rollup_rows = build_rollups(children, args.event_family)
        status_counts = Counter(row[4] for row in rollup_rows)
        probability_ready = sum(1 for row in rollup_rows if row[6] is not None)

        if not args.dry_run and rollup_run_id is not None:
            with conn.cursor() as cur:
                insert_rollups(cur, rollup_run_id, stage5_run_id, rollup_rows, args.batch_size)
                update_run(
                    cur,
                    rollup_run_id,
                    "completed",
                    {
                        "stage2_run_key": resolved_stage2_run_key,
                        "stage5_run_key": resolved_stage5_run_key,
                        "event_family": args.event_family,
                        "bundle_count": len(rollup_rows),
                        "probability_ready_bundle_count": probability_ready,
                        "scoped_match_counts": dict(status_counts),
                        "script_version": SCRIPT_VERSION,
                    },
                )
            conn.commit()

        print(
            json.dumps(
                {
                    "run_key": run_key,
                    "dry_run": args.dry_run,
                    "event_family": args.event_family,
                    "bundle_count": len(rollup_rows),
                    "probability_ready_bundle_count": probability_ready,
                    "scoped_match_counts": dict(status_counts),
                    "stage5_run_key": resolved_stage5_run_key,
                    "script_version": SCRIPT_VERSION,
                },
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        conn.rollback()
        if not args.dry_run and rollup_run_id is not None:
            with conn.cursor() as cur:
                update_run(cur, rollup_run_id, "failed", {"error": str(exc)[:1000], "script_version": SCRIPT_VERSION})
            conn.commit()
        print(f"Stage 6 bundle probability rollup failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

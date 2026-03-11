#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor


SCRIPT_VERSION = "earthquake_export_v1"
OUTPUT_ROOT = Path("data") / "exports" / "earthquake"
OBSERVED_PROBABILITY_FIELD = {
    "exact_hit": "p_exact_under_null",
    "near_hit": "p_near_under_null",
    "similar_only": "p_similar_under_null",
    "miss": "p_miss_under_null",
}


@dataclass
class RunSet:
    stage2_run_id: int
    stage2_run_key: str
    stage3_run_id: int | None
    stage3_run_key: str | None
    stage4_run_id: int
    stage4_run_key: str
    stage5_run_id: int
    stage5_run_key: str
    stage6_run_id: int | None
    stage6_run_key: str | None
    stage7_run_id: int | None
    stage7_run_key: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export earthquake prediction analysis artifacts.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage5-run-key", help="Stage 5 run key. Defaults to latest completed Stage 5 run.")
    parser.add_argument("--stage4-run-key", help="Optional Stage 4 run key override.")
    parser.add_argument("--stage2-run-key", help="Optional Stage 2 run key override.")
    parser.add_argument("--output-dir", help="Output directory. Defaults to data/exports/earthquake/<stage5_run_key>.")
    return parser.parse_args()


def fetch_run(cur, stage: str, run_key: str | None) -> dict[str, Any]:
    if run_key:
        cur.execute(
            """
            SELECT id, run_key, source_filter, run_meta
            FROM public.prediction_audit_runs
            WHERE stage = %s AND run_key = %s
            """,
            (stage, run_key),
        )
    else:
        cur.execute(
            """
            SELECT id, run_key, source_filter, run_meta
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
    return row


def resolve_run_set(cur, args: argparse.Namespace) -> RunSet:
    stage5 = fetch_run(cur, "stage5_probability_model", args.stage5_run_key)
    stage5_filter = stage5["source_filter"] or {}

    stage4_run_key = args.stage4_run_key or stage5_filter.get("stage4_run_key")
    if not stage4_run_key:
        raise RuntimeError("Could not infer Stage 4 run key from Stage 5 run metadata.")
    stage4 = fetch_run(cur, "stage4_match_scoring", stage4_run_key)
    stage4_filter = stage4["source_filter"] or {}

    stage2_run_key = args.stage2_run_key or stage5_filter.get("stage2_run_key") or stage4_filter.get("stage2_run_key")
    if not stage2_run_key:
        raise RuntimeError("Could not infer Stage 2 run key from Stage 4/5 run metadata.")
    stage2 = fetch_run(cur, "stage2_eligibility", stage2_run_key)

    stage3_run_key = stage4_filter.get("stage3_run_key")
    stage3 = fetch_run(cur, "stage3_event_ledger", stage3_run_key) if stage3_run_key else None

    cur.execute(
        """
        SELECT id, run_key
        FROM public.prediction_audit_runs
        WHERE stage = 'stage6_bundle_probability_rollup'
          AND status = 'completed'
          AND source_filter->>'stage5_run_key' = %s
          AND source_filter->>'event_family' = 'earthquake'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (stage5["run_key"],),
    )
    stage6 = cur.fetchone()

    cur.execute(
        """
        SELECT id, run_key
        FROM public.prediction_audit_runs
        WHERE stage = 'stage7_final_adjudication'
          AND status = 'completed'
          AND source_filter->>'stage5_run_key' = %s
          AND source_filter->>'family' = 'earthquake'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (stage5["run_key"],),
    )
    stage7 = cur.fetchone()

    return RunSet(
        stage2_run_id=stage2["id"],
        stage2_run_key=stage2["run_key"],
        stage3_run_id=stage3["id"] if stage3 else None,
        stage3_run_key=stage3["run_key"] if stage3 else None,
        stage4_run_id=stage4["id"],
        stage4_run_key=stage4["run_key"],
        stage5_run_id=stage5["id"],
        stage5_run_key=stage5["run_key"],
        stage6_run_id=stage6["id"] if stage6 else None,
        stage6_run_key=stage6["run_key"] if stage6 else None,
        stage7_run_id=stage7["id"] if stage7 else None,
        stage7_run_key=stage7["run_key"] if stage7 else None,
    )


def load_predictions(cur, runs: RunSet) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            p.id AS prediction_id,
            p.parse_run_id,
            p.contact_report_id,
            p.report_number,
            p.candidate_seq,
            p.bundle_key,
            p.bundle_role,
            p.bundle_component_seq,
            p.bundle_component_count,
            p.stage2_label,
            p.significant,
            p.claimed_contact_date,
            p.earliest_provable_public_date,
            p.public_date_basis,
            p.provenance_score,
            p.public_date_status,
            p.public_date_reason,
            p.claim_normalized,
            p.source_quote,
            p.target_name,
            p.target_type,
            p.target_lat,
            p.target_lon,
            p.target_radius_km,
            p.magnitude_min,
            p.magnitude_max,
            p.severity_band,
            p.time_window_start,
            p.time_window_end,
            p.match_status,
            p.best_event_ledger_id,
            p.p_exact_under_null,
            p.p_near_under_null,
            p.p_similar_under_null,
            p.p_miss_under_null,
            p.probability_model_version,
            p.probability_notes,
            p.probability_meta,
            p.final_status,
            p.final_reason,
            p.final_meta,
            mr.confidence AS review_confidence,
            mr.rationale AS review_rationale,
            mr.review_meta,
            el.external_event_id,
            el.event_title,
            el.event_start_date,
            el.location_name AS observed_location_name,
            el.distance_km,
            el.time_delta_days,
            el.magnitude_value AS observed_magnitude_value,
            el.source_url AS observed_source_url
        FROM public.prediction_audit_predictions p
        LEFT JOIN public.prediction_audit_match_reviews mr
          ON mr.prediction_id = p.id
         AND mr.review_run_id = %s
        LEFT JOIN public.prediction_audit_event_ledger el
          ON el.id = p.best_event_ledger_id
        WHERE p.last_stage2_run_id = %s
          AND p.event_family_final = 'earthquake'
          AND p.stage2_label IN ('eligible_prediction', 'significant_prediction')
          AND p.time_window_start IS NOT NULL
          AND p.time_window_end IS NOT NULL
        ORDER BY p.time_window_start, p.report_number, p.candidate_seq
        """,
        (runs.stage4_run_id, runs.stage2_run_id),
    )
    return [dict(row) for row in cur.fetchall()]


def load_bundles(cur, bundle_keys: list[str]) -> dict[str, dict[str, Any]]:
    if not bundle_keys:
        return {}
    cur.execute(
        """
        SELECT
            b.id,
            b.bundle_key,
            b.parse_run_id,
            b.contact_report_id,
            b.report_number,
            b.claimed_contact_date,
            b.bundle_seq,
            b.bundle_kind,
            b.component_count,
            b.event_family_hint,
            b.bundle_significant,
            b.bundle_match_status,
            b.bundle_meta,
            b.stage4_meta
        FROM public.prediction_audit_bundles b
        WHERE b.bundle_key = ANY(%s)
        ORDER BY b.report_number, b.bundle_seq
        """,
        (bundle_keys,),
    )
    return {row["bundle_key"]: dict(row) for row in cur.fetchall()}


def load_bundle_rollups(cur, runs: RunSet) -> dict[str, dict[str, Any]]:
    if runs.stage6_run_id is None:
        return {}
    cur.execute(
        """
        SELECT
            b.bundle_key,
            r.scoped_prediction_count,
            r.probability_ready_count,
            r.scoped_match_status,
            r.scoped_status_counts,
            r.p_observed_under_null,
            r.observed_log10_under_null,
            r.p_all_exact_under_null,
            r.all_exact_log10_under_null,
            r.p_all_near_or_better_under_null,
            r.all_near_or_better_log10_under_null,
            r.p_all_similar_or_better_under_null,
            r.all_similar_or_better_log10_under_null,
            r.p_all_miss_under_null,
            r.all_miss_log10_under_null,
            r.rollup_model_version,
            r.rollup_notes,
            r.rollup_meta
        FROM public.prediction_audit_bundle_rollups r
        JOIN public.prediction_audit_bundles b ON b.id = r.bundle_id
        WHERE r.rollup_run_id = %s
          AND r.event_family = 'earthquake'
        ORDER BY b.report_number, b.bundle_seq
        """,
        (runs.stage6_run_id,),
    )
    return {row["bundle_key"]: dict(row) for row in cur.fetchall()}


def observed_probability(prediction: dict[str, Any]) -> float | None:
    field_name = OBSERVED_PROBABILITY_FIELD.get(prediction["match_status"])
    if not field_name:
        return None
    value = prediction.get(field_name)
    return float(value) if value is not None else None


def scientific_from_log10(log10_sum: float) -> str:
    exponent = math.floor(log10_sum)
    mantissa = 10 ** (log10_sum - exponent)
    return f"{mantissa:.6f}e{exponent}"


def aggregate_probabilities(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "log10_sum": None,
            "ln_sum": None,
            "scientific_notation": None,
        }

    log10_sum = sum(math.log10(value) for value in values if value > 0)
    ln_sum = sum(math.log(value) for value in values if value > 0)
    return {
        "count": len(values),
        "log10_sum": round(log10_sum, 6),
        "ln_sum": round(ln_sum, 6),
        "scientific_notation": scientific_from_log10(log10_sum),
    }


def annotate_publication_timing(prediction: dict[str, Any]) -> None:
    public_date = prediction.get("earliest_provable_public_date")
    event_date = prediction.get("event_start_date")
    if not public_date or not event_date:
        prediction["observed_event_before_publication"] = None
        prediction["publication_lag_days_vs_event"] = None
        return
    lag_days = (event_date - public_date).days
    prediction["publication_lag_days_vs_event"] = lag_days
    prediction["observed_event_before_publication"] = lag_days < 0


def summarize_predictions(predictions: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts = Counter(prediction["match_status"] for prediction in predictions)
    stage2_counts = Counter(prediction["stage2_label"] for prediction in predictions)
    observed_rows = []
    hit_rows = []
    exact_rows = []

    for prediction in predictions:
        probability = prediction["observed_probability_under_null"]
        if probability is None:
            continue
        observed_rows.append(probability)
        if prediction["match_status"] in {"exact_hit", "near_hit", "similar_only"}:
            hit_rows.append(probability)
        if prediction["match_status"] == "exact_hit":
            exact_rows.append(probability)

    return {
        "scoped_prediction_count": len(predictions),
        "status_counts": dict(status_counts),
        "stage2_label_counts": dict(stage2_counts),
        "probability_ready_count": len(observed_rows),
        "combined_observed_probability": aggregate_probabilities(observed_rows),
        "combined_hit_probability": aggregate_probabilities(hit_rows),
        "combined_exact_hit_probability": aggregate_probabilities(exact_rows),
    }


def build_bundle_rows(
    predictions: list[dict[str, Any]],
    bundle_lookup: dict[str, dict[str, Any]],
    bundle_rollup_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for prediction in predictions:
        bundle_key = prediction.get("bundle_key")
        if bundle_key:
            grouped.setdefault(bundle_key, []).append(prediction)

    rows: list[dict[str, Any]] = []
    for bundle_key, children in sorted(grouped.items(), key=lambda item: (item[1][0]["report_number"], item[0])):
        bundle = bundle_lookup.get(bundle_key, {})
        rollup = bundle_rollup_lookup.get(bundle_key, {})
        child_status_counts = Counter(child["match_status"] for child in children)
        child_probabilities = [child["observed_probability_under_null"] for child in children if child["observed_probability_under_null"] is not None]
        rows.append(
            {
                "bundle_key": bundle_key,
                "report_number": bundle.get("report_number", children[0]["report_number"]),
                "claimed_contact_date": bundle.get("claimed_contact_date", children[0]["claimed_contact_date"]),
                "bundle_kind": bundle.get("bundle_kind"),
                "bundle_significant": bundle.get("bundle_significant"),
                "bundle_match_status": bundle.get("bundle_match_status"),
                "component_count": bundle.get("component_count", len(children)),
                "earthquake_child_count": len(children),
                "child_status_counts": rollup.get("scoped_status_counts", dict(child_status_counts)),
                "probability_ready_child_count": rollup.get("probability_ready_count", len(child_probabilities)),
                "scoped_match_status": rollup.get("scoped_match_status"),
                "child_combined_observed_probability": rollup.get("p_observed_under_null"),
                "child_combined_observed_probability_log10": rollup.get(
                    "observed_log10_under_null",
                    aggregate_probabilities(child_probabilities)["log10_sum"],
                ),
                "child_combined_observed_probability_scientific": aggregate_probabilities(child_probabilities)["scientific_notation"],
                "child_all_exact_probability": rollup.get("p_all_exact_under_null"),
                "child_all_exact_probability_log10": rollup.get("all_exact_log10_under_null"),
                "child_all_near_or_better_probability": rollup.get("p_all_near_or_better_under_null"),
                "child_all_near_or_better_probability_log10": rollup.get("all_near_or_better_log10_under_null"),
                "child_all_similar_or_better_probability": rollup.get("p_all_similar_or_better_under_null"),
                "child_all_similar_or_better_probability_log10": rollup.get("all_similar_or_better_log10_under_null"),
                "child_all_miss_probability": rollup.get("p_all_miss_under_null"),
                "child_all_miss_probability_log10": rollup.get("all_miss_log10_under_null"),
                "rollup_model_version": rollup.get("rollup_model_version"),
                "child_prediction_ids": [child["prediction_id"] for child in children],
            }
        )
    return rows


def csv_safe(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: csv_safe(row.get(field)) for field in fieldnames})


def main() -> int:
    args = parse_args()
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        print(f"Missing DSN env var: {args.dsn_env}", file=sys.stderr)
        return 2

    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            runs = resolve_run_set(cur, args)
            predictions = load_predictions(cur, runs)

            for prediction in predictions:
                prediction["observed_probability_under_null"] = observed_probability(prediction)
                probability = prediction["observed_probability_under_null"]
                prediction["observed_probability_log10"] = round(math.log10(probability), 6) if probability and probability > 0 else None
                annotate_publication_timing(prediction)

            bundle_keys = sorted({prediction["bundle_key"] for prediction in predictions if prediction.get("bundle_key")})
            bundle_lookup = load_bundles(cur, bundle_keys)
            bundle_rollup_lookup = load_bundle_rollups(cur, runs)

        bundle_rows = build_bundle_rows(predictions, bundle_lookup, bundle_rollup_lookup)
        unresolved_rows = [row for row in predictions if row["match_status"] == "unresolved"]
        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "script_version": SCRIPT_VERSION,
            "run_keys": {
                "stage2_run_key": runs.stage2_run_key,
                "stage3_run_key": runs.stage3_run_key,
                "stage4_run_key": runs.stage4_run_key,
                "stage5_run_key": runs.stage5_run_key,
                "stage6_run_key": runs.stage6_run_key,
                "stage7_run_key": runs.stage7_run_key,
            },
            "prediction_summary": summarize_predictions(predictions),
            "bundle_summary": {
                "bundle_count": len(bundle_rows),
                "bundle_status_counts": dict(Counter(row["bundle_match_status"] for row in bundle_rows)),
                "scoped_bundle_status_counts": dict(Counter(row["scoped_match_status"] for row in bundle_rows if row.get("scoped_match_status"))),
                "combined_probability_ready_bundle_count": sum(1 for row in bundle_rows if row["probability_ready_child_count"] == row["earthquake_child_count"]),
            },
            "final_status_counts": dict(Counter(row["final_status"] for row in predictions)),
            "earliest_public_date_populated_count": sum(1 for row in predictions if row["earliest_provable_public_date"] is not None),
            "observed_event_before_publication_count": sum(1 for row in predictions if row["observed_event_before_publication"] is True),
            "public_date_status_counts": dict(Counter(row["public_date_status"] for row in predictions)),
            "unresolved_prediction_count": len(unresolved_rows),
        }

        output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_ROOT / runs.stage5_run_key
        output_dir.mkdir(parents=True, exist_ok=True)

        summary_path = output_dir / "summary.json"
        predictions_path = output_dir / "predictions.csv"
        bundles_path = output_dir / "bundles.csv"
        unresolved_path = output_dir / "unresolved.csv"

        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        write_csv(
            predictions_path,
            predictions,
            [
                "prediction_id",
                "report_number",
                "candidate_seq",
                "bundle_key",
                "bundle_role",
                "bundle_component_seq",
                "bundle_component_count",
                "stage2_label",
                "significant",
                "claimed_contact_date",
                "earliest_provable_public_date",
                "public_date_basis",
                "provenance_score",
                "public_date_status",
                "public_date_reason",
                "claim_normalized",
                "source_quote",
                "target_name",
                "target_type",
                "target_lat",
                "target_lon",
                "target_radius_km",
                "magnitude_min",
                "magnitude_max",
                "severity_band",
                "time_window_start",
                "time_window_end",
                "match_status",
                "best_event_ledger_id",
                "external_event_id",
                "event_title",
                "event_start_date",
                "observed_location_name",
                "distance_km",
                "time_delta_days",
                "observed_magnitude_value",
                "review_confidence",
                "review_rationale",
                "observed_source_url",
                "p_exact_under_null",
                "p_near_under_null",
                "p_similar_under_null",
                "p_miss_under_null",
                "observed_probability_under_null",
                "observed_probability_log10",
                "observed_event_before_publication",
                "publication_lag_days_vs_event",
                "probability_model_version",
                "probability_notes",
                "probability_meta",
                "final_status",
                "final_reason",
                "final_meta",
                "review_meta",
            ],
        )
        write_csv(
            bundles_path,
            bundle_rows,
            [
                "bundle_key",
                "report_number",
                "claimed_contact_date",
                "bundle_kind",
                "bundle_significant",
                "bundle_match_status",
                "component_count",
                "earthquake_child_count",
                "probability_ready_child_count",
                "scoped_match_status",
                "child_status_counts",
                "child_combined_observed_probability",
                "child_combined_observed_probability_log10",
                "child_combined_observed_probability_scientific",
                "child_all_exact_probability",
                "child_all_exact_probability_log10",
                "child_all_near_or_better_probability",
                "child_all_near_or_better_probability_log10",
                "child_all_similar_or_better_probability",
                "child_all_similar_or_better_probability_log10",
                "child_all_miss_probability",
                "child_all_miss_probability_log10",
                "rollup_model_version",
                "child_prediction_ids",
            ],
        )
        write_csv(
            unresolved_path,
            unresolved_rows,
            [
                "prediction_id",
                "report_number",
                "candidate_seq",
                "bundle_key",
                "claim_normalized",
                "target_name",
                "target_type",
                "time_window_start",
                "time_window_end",
                "match_status",
                "final_status",
                "final_reason",
                "probability_notes",
                "review_rationale",
            ],
        )

        print(
            json.dumps(
                {
                    "summary_path": str(summary_path),
                    "predictions_path": str(predictions_path),
                    "bundles_path": str(bundles_path),
                    "unresolved_path": str(unresolved_path),
                    "run_keys": summary["run_keys"],
                    "prediction_summary": summary["prediction_summary"],
                    "bundle_summary": summary["bundle_summary"],
                },
                indent=2,
            )
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_batch


SCRIPT_VERSION = "stage0_provenance_claimed_date_seed_v1"
OUTPUT_ROOT = Path("data") / "exports" / "provenance"


@dataclass
class ReportRow:
    contact_report_id: int
    report_number: int
    claimed_contact_date: Any
    included_prediction_count: int
    scored_prediction_count: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed report-level provenance placeholders and export a provenance queue.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key. Defaults to latest completed Stage 2 run.")
    parser.add_argument("--run-key", help="Unique Stage 0 provenance run key. Defaults to a timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument(
        "--scope",
        choices=("included", "scored", "all_stage2"),
        default="included",
        help="Which report set to seed/export. Defaults to reports with included predictions.",
    )
    parser.add_argument("--output-dir", help="Output directory. Defaults to data/exports/provenance/<run_key>.")
    parser.add_argument("--dry-run", action="store_true", help="Compute output without writing DB rows.")
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage0-provenance-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def fetch_stage2_run(cur, run_key: str | None) -> tuple[int, str]:
    if run_key:
        cur.execute(
            """
            SELECT id, run_key
            FROM public.prediction_audit_runs
            WHERE stage = 'stage2_eligibility' AND run_key = %s
            """,
            (run_key,),
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
    return row["id"], row["run_key"]


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
        VALUES (%s, 'stage0_provenance', 'running', %s, %s, %s, %s, %s, %s, now())
        RETURNING id
        """,
        (
            run_key,
            SCRIPT_VERSION,
            "none",
            "public.contact_reports",
            Json(source_filter),
            notes or None,
            Json({"script_version": SCRIPT_VERSION, "seed_kind": "claimed_contact_date_only"}),
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


def scope_clause(scope: str) -> str:
    if scope == "included":
        return "p.final_status = 'included_in_statistics'"
    if scope == "scored":
        return "p.match_status IN ('exact_hit', 'near_hit', 'similar_only', 'miss')"
    return "TRUE"


def fetch_reports(cur, stage2_run_id: int, scope: str) -> list[ReportRow]:
    cur.execute(
        f"""
        SELECT
            c.id AS contact_report_id,
            c.report_number,
            c.report_date AS claimed_contact_date,
            count(*) FILTER (WHERE p.final_status = 'included_in_statistics')::integer AS included_prediction_count,
            count(*) FILTER (WHERE p.match_status IN ('exact_hit', 'near_hit', 'similar_only', 'miss'))::integer AS scored_prediction_count
        FROM public.contact_reports c
        JOIN public.prediction_audit_predictions p
          ON p.contact_report_id = c.id
        WHERE p.last_stage2_run_id = %s
          AND {scope_clause(scope)}
        GROUP BY c.id, c.report_number, c.report_date
        ORDER BY c.report_number
        """,
        (stage2_run_id,),
    )
    return [ReportRow(**row) for row in cur.fetchall()]


def fetch_prediction_rows(cur, stage2_run_id: int, scope: str) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
            p.report_number,
            p.candidate_seq,
            p.event_family_final,
            p.stage2_label,
            p.significant,
            p.claimed_contact_date,
            p.earliest_provable_public_date,
            p.public_date_basis,
            p.provenance_score,
            p.match_status,
            p.final_status,
            p.claim_normalized
        FROM public.prediction_audit_predictions p
        WHERE p.last_stage2_run_id = %s
          AND {scope_clause(scope)}
        ORDER BY p.event_family_final, p.report_number, p.candidate_seq
        """,
        (stage2_run_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def build_seed_rows(reports: list[ReportRow], run_id: int) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for report in reports:
        source_hash = hashlib.sha256(
            f"{report.report_number}|{report.claimed_contact_date}|claimed_contact_date_only".encode("utf-8")
        ).hexdigest()
        rows.append(
            (
                run_id,
                report.contact_report_id,
                report.report_number,
                report.claimed_contact_date,
                "claimed_contact_date_only",
                1,
                None,
                "contact_reports.report_date",
                "public.contact_reports.report_date",
                None,
                "english",
                None,
                None,
                source_hash,
                "Seed placeholder only. This is the claimed contact date from the corpus, not a provable public date.",
                Json(
                    {
                        "script_version": SCRIPT_VERSION,
                        "included_prediction_count": report.included_prediction_count,
                        "scored_prediction_count": report.scored_prediction_count,
                    }
                ),
            )
        )
    return rows


def insert_seed_rows(cur, rows: list[tuple[Any, ...]]) -> None:
    execute_batch(
        cur,
        """
        INSERT INTO public.prediction_audit_report_provenance (
            provenance_run_id,
            contact_report_id,
            report_number,
            claimed_contact_date,
            evidence_kind,
            evidence_quality,
            evidence_public_date,
            source_label,
            source_path,
            source_url,
            language,
            edition_or_translation,
            translator,
            source_hash,
            notes,
            raw_evidence
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
        page_size=200,
    )


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

    run_key = args.run_key or generate_run_key()
    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    conn.autocommit = False
    run_id: int | None = None

    try:
        with conn.cursor() as cur:
            stage2_run_id, stage2_run_key = fetch_stage2_run(cur, args.stage2_run_key)
            reports = fetch_reports(cur, stage2_run_id, args.scope)
            predictions = fetch_prediction_rows(cur, stage2_run_id, args.scope)

            if not args.dry_run:
                run_id = insert_run(
                    cur,
                    run_key,
                    {"stage2_run_key": stage2_run_key, "scope": args.scope, "family": "cross_family"},
                    args.notes,
                )
                conn.commit()

        seed_rows = build_seed_rows(reports, run_id or 0)

        if not args.dry_run and run_id is not None and seed_rows:
            with conn.cursor() as cur:
                insert_seed_rows(cur, seed_rows)
                update_run(
                    cur,
                    run_id,
                    "completed",
                    {
                        "stage2_run_key": stage2_run_key,
                        "scope": args.scope,
                        "seeded_report_count": len(reports),
                        "seeded_evidence_count": len(seed_rows),
                        "scoped_prediction_count": len(predictions),
                        "script_version": SCRIPT_VERSION,
                    },
                )
            conn.commit()

        report_rows = [
            {
                "report_number": report.report_number,
                "claimed_contact_date": report.claimed_contact_date,
                "included_prediction_count": report.included_prediction_count,
                "scored_prediction_count": report.scored_prediction_count,
                "seeded_evidence_kind": "claimed_contact_date_only",
                "seeded_evidence_quality": 1,
                "evidence_public_date": None,
                "notes": "Needs external/manual public-date evidence.",
            }
            for report in reports
        ]

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "script_version": SCRIPT_VERSION,
            "run_key": run_key,
            "stage2_run_key": stage2_run_key,
            "scope": args.scope,
            "report_count": len(reports),
            "prediction_count": len(predictions),
            "prediction_family_counts": dict(Counter(row["event_family_final"] for row in predictions)),
            "current_public_date_counts": {
                "with_earliest_provable_public_date": sum(1 for row in predictions if row["earliest_provable_public_date"] is not None),
                "claimed_only": sum(1 for row in predictions if row["public_date_basis"] == "claimed_contact_date_only"),
                "other_basis": sum(
                    1
                    for row in predictions
                    if row["public_date_basis"] not in (None, "claimed_contact_date_only")
                ),
            },
            "provenance_score_counts": dict(Counter(str(row["provenance_score"]) for row in predictions)),
        }

        output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_ROOT / run_key
        output_dir.mkdir(parents=True, exist_ok=True)
        summary_path = output_dir / "summary.json"
        report_queue_path = output_dir / "report_queue.csv"
        predictions_path = output_dir / "scoped_predictions.csv"

        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        write_csv(
            report_queue_path,
            report_rows,
            [
                "report_number",
                "claimed_contact_date",
                "included_prediction_count",
                "scored_prediction_count",
                "seeded_evidence_kind",
                "seeded_evidence_quality",
                "evidence_public_date",
                "notes",
            ],
        )
        write_csv(
            predictions_path,
            predictions,
            [
                "report_number",
                "candidate_seq",
                "event_family_final",
                "stage2_label",
                "significant",
                "claimed_contact_date",
                "earliest_provable_public_date",
                "public_date_basis",
                "provenance_score",
                "match_status",
                "final_status",
                "claim_normalized",
            ],
        )

        print(
            json.dumps(
                {
                    "run_key": run_key,
                    "dry_run": args.dry_run,
                    "stage2_run_key": stage2_run_key,
                    "scope": args.scope,
                    "report_count": len(reports),
                    "prediction_count": len(predictions),
                    "summary_path": str(summary_path),
                    "report_queue_path": str(report_queue_path),
                    "predictions_path": str(predictions_path),
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
        print(f"Stage 0 provenance seed failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

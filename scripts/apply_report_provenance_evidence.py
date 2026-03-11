#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_batch


SCRIPT_VERSION = "stage0_provenance_manual_evidence_v1"
REPO_ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply manual report-level provenance evidence and backfill prediction public-date fields.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument(
        "--evidence-path",
        default=str(REPO_ROOT / "data" / "report_provenance_manual_evidence.json"),
        help="Path to manual report provenance evidence JSON.",
    )
    parser.add_argument("--run-key", help="Unique Stage 0 provenance run key. Defaults to a timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument("--dry-run", action="store_true", help="Validate/import evidence without writing DB changes.")
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage0-provenance-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def load_evidence(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        raise RuntimeError("Manual provenance evidence file must be a JSON list.")
    return payload


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
            "public.prediction_audit_report_provenance",
            Json(source_filter),
            notes or None,
            Json({"script_version": SCRIPT_VERSION, "seed_kind": "manual_source_link"}),
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


def fetch_report_map(cur, report_numbers: list[int]) -> dict[int, dict[str, Any]]:
    cur.execute(
        """
        SELECT id, report_number, report_date
        FROM public.contact_reports
        WHERE report_number = ANY(%s)
        ORDER BY report_number
        """,
        (report_numbers,),
    )
    return {row["report_number"]: dict(row) for row in cur.fetchall()}


def fetch_existing_evidence_keys(cur, report_numbers: list[int]) -> set[tuple[Any, ...]]:
    cur.execute(
        """
        SELECT
            report_number,
            evidence_kind,
            evidence_public_date,
            COALESCE(source_label, '') AS source_label_norm,
            COALESCE(source_path, '') AS source_path_norm,
            COALESCE(source_url, '') AS source_url_norm,
            COALESCE(language, '') AS language_norm,
            COALESCE(edition_or_translation, '') AS edition_or_translation_norm,
            COALESCE(translator, '') AS translator_norm,
            COALESCE(source_hash, '') AS source_hash_norm,
            COALESCE(notes, '') AS notes_norm
        FROM public.prediction_audit_report_provenance
        WHERE report_number = ANY(%s)
        """,
        (report_numbers,),
    )
    keys: set[tuple[Any, ...]] = set()
    for row in cur.fetchall():
        if isinstance(row, dict):
            keys.add(
                (
                    row["report_number"],
                    row["evidence_kind"],
                    row["evidence_public_date"],
                    row["source_label_norm"],
                    row["source_path_norm"],
                    row["source_url_norm"],
                    row["language_norm"],
                    row["edition_or_translation_norm"],
                    row["translator_norm"],
                    row["source_hash_norm"],
                    row["notes_norm"],
                )
            )
        else:
            keys.add(tuple(row))
    return keys


def evidence_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(item["report_number"]),
        item["evidence_kind"],
        parse_iso_date(item.get("evidence_public_date")),
        item.get("source_label") or "",
        item.get("source_path") or "",
        item.get("source_url") or "",
        item.get("language") or "",
        item.get("edition_or_translation") or "",
        item.get("translator") or "",
        item.get("source_hash") or "",
        item.get("notes") or "",
    )


def build_insert_rows(
    evidence_rows: list[dict[str, Any]],
    report_map: dict[int, dict[str, Any]],
    run_id: int,
    existing_keys: set[tuple[Any, ...]],
) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for item in evidence_rows:
        if evidence_key(item) in existing_keys:
            continue
        report_number = int(item["report_number"])
        report = report_map.get(report_number)
        if not report:
            raise RuntimeError(f"Missing contact_reports row for report {report_number}.")

        rows.append(
            (
                run_id,
                report["id"],
                report_number,
                report["report_date"],
                item["evidence_kind"],
                int(item["evidence_quality"]),
                parse_iso_date(item.get("evidence_public_date")),
                item.get("source_label"),
                item.get("source_path"),
                item.get("source_url"),
                item.get("language"),
                item.get("edition_or_translation"),
                item.get("translator"),
                item.get("source_hash"),
                item.get("notes"),
                Json(
                    {
                        "script_version": SCRIPT_VERSION,
                        "source_url": item.get("source_url"),
                        "evidence_public_date": item.get("evidence_public_date"),
                    }
                ),
            )
        )
    return rows


def insert_evidence(cur, rows: list[tuple[Any, ...]]) -> None:
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


def fetch_rollups(cur, report_numbers: list[int]) -> list[dict[str, Any]]:
    cur.execute(
        """
        WITH ranked AS (
            SELECT
                rp.report_number,
                rp.evidence_public_date,
                rp.evidence_quality,
                rp.evidence_kind,
                rp.source_label,
                rp.source_url,
                row_number() OVER (
                    PARTITION BY rp.report_number
                    ORDER BY rp.evidence_public_date NULLS LAST, rp.evidence_quality DESC, rp.created_at ASC, rp.id ASC
                ) AS rn
            FROM public.prediction_audit_report_provenance rp
            WHERE rp.report_number = ANY(%s)
              AND rp.evidence_quality >= 1
        )
        SELECT
            report_number,
            evidence_public_date AS earliest_public_date,
            evidence_quality AS chosen_quality,
            evidence_kind AS chosen_kind,
            source_label AS chosen_source_label,
            source_url AS chosen_source_url
        FROM ranked
        WHERE rn = 1
        ORDER BY report_number
        """,
        (report_numbers,),
    )
    return [dict(row) for row in cur.fetchall()]


def build_prediction_updates(rollups: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    updates: list[tuple[Any, ...]] = []
    for row in rollups:
        basis = f"{row['chosen_kind']}:{row['chosen_source_label']}"
        updates.append(
            (
                row["earliest_public_date"],
                basis[:200] if basis else None,
                row["chosen_quality"],
                Json(
                    {
                        "script_version": SCRIPT_VERSION,
                        "source_label": row["chosen_source_label"],
                        "source_url": row["chosen_source_url"],
                    }
                ),
                row["report_number"],
            )
        )
    return updates


def update_predictions(cur, rows: list[tuple[Any, ...]]) -> None:
    execute_batch(
        cur,
        """
        UPDATE public.prediction_audit_predictions
        SET earliest_provable_public_date = %s,
            public_date_basis = %s,
            provenance_score = %s,
            stage2_meta = COALESCE(stage2_meta, '{}'::jsonb) || %s::jsonb
        WHERE report_number = %s
        """,
        rows,
        page_size=200,
    )


def main() -> int:
    args = parse_args()
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        print(f"Missing DSN env var: {args.dsn_env}", file=sys.stderr)
        return 2

    evidence_rows = load_evidence(args.evidence_path)
    report_numbers = sorted({int(item["report_number"]) for item in evidence_rows})
    run_key = args.run_key or generate_run_key()
    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    conn.autocommit = False
    run_id: int | None = None

    try:
        with conn.cursor() as cur:
            report_map = fetch_report_map(cur, report_numbers)
            existing_keys = fetch_existing_evidence_keys(cur, report_numbers)
            if not args.dry_run:
                run_id = insert_run(
                    cur,
                    run_key,
                    {
                        "evidence_path": args.evidence_path,
                        "report_count": len(report_numbers),
                        "family": "cross_family",
                    },
                    args.notes,
                )
                conn.commit()

        insert_rows = build_insert_rows(evidence_rows, report_map, run_id or 0, existing_keys)

        if not args.dry_run and run_id is not None:
            with conn.cursor() as cur:
                insert_evidence(cur, insert_rows)
                rollups = fetch_rollups(cur, report_numbers)
                update_predictions(cur, build_prediction_updates(rollups))
                update_run(
                    cur,
                    run_id,
                    "completed",
                    {
                        "report_count": len(report_numbers),
                        "evidence_count": len(insert_rows),
                        "updated_prediction_count": sum(
                            item["prediction_count"] for item in fetch_prediction_counts(cur, report_numbers)
                        ),
                        "script_version": SCRIPT_VERSION,
                    },
                )
            conn.commit()

        summary = {
            "run_key": run_key,
            "dry_run": args.dry_run,
            "report_count": len(report_numbers),
            "evidence_count": len(insert_rows),
            "script_version": SCRIPT_VERSION,
        }
        print(json.dumps(summary, indent=2))
        return 0
    except Exception as exc:
        conn.rollback()
        if not args.dry_run and run_id is not None:
            with conn.cursor() as cur:
                update_run(cur, run_id, "failed", {"error": str(exc)[:1000], "script_version": SCRIPT_VERSION})
            conn.commit()
        print(f"Stage 0 provenance evidence apply failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def fetch_prediction_counts(cur, report_numbers: list[int]) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT report_number, count(*) AS prediction_count
        FROM public.prediction_audit_predictions
        WHERE report_number = ANY(%s)
        GROUP BY report_number
        ORDER BY report_number
        """,
        (report_numbers,),
    )
    return [dict(row) for row in cur.fetchall()]


if __name__ == "__main__":
    raise SystemExit(main())

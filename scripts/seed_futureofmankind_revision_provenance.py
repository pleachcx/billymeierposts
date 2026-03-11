#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_batch


SCRIPT_VERSION = "stage0_provenance_fom_revision_v1"
FOM_API_URL = "https://www.futureofmankind.co.uk/w/api.php"
FOM_PAGE_URL = "https://www.futureofmankind.co.uk/Billy_Meier/Contact_Report_{report_number}"
REPORT_NUMBER_RE = re.compile(r"Contact Report (\d+)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed report-level provenance evidence from Future Of Mankind page creation timestamps.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", default="stage2-20260310T232950Z", help="Stage 2 run key used to scope included predictions.")
    parser.add_argument("--report-numbers", help="Optional comma-separated report numbers. Defaults to included scored reports in the Stage 2 run.")
    parser.add_argument("--run-key", help="Unique Stage 0 provenance run key. Defaults to a timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and validate evidence without writing DB changes.")
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage0-provenance-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def fetch_stage2_run(cur, run_key: str) -> dict[str, Any]:
    cur.execute(
        """
        SELECT id, run_key
        FROM public.prediction_audit_runs
        WHERE stage = 'stage2_eligibility' AND run_key = %s
        """,
        (run_key,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Missing Stage 2 run {run_key}.")
    return dict(row)


def fetch_scoped_report_numbers(cur, stage2_run_id: int) -> list[int]:
    cur.execute(
        """
        SELECT DISTINCT p.report_number
        FROM public.prediction_audit_predictions p
        WHERE p.last_stage2_run_id = %s
          AND p.final_status = 'included_in_statistics'
          AND p.match_status IN ('exact_hit', 'near_hit', 'similar_only', 'miss')
        ORDER BY p.report_number
        """,
        (stage2_run_id,),
    )
    return [int(row["report_number"]) for row in cur.fetchall()]


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
    return {int(row["report_number"]): dict(row) for row in cur.fetchall()}


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
    return keys


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
            Json({"script_version": SCRIPT_VERSION, "seed_kind": "futureofmankind_first_revision"}),
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


def fom_api_request(report_number: int) -> dict[str, Any]:
    title = f"Contact_Report_{report_number}"
    query = urllib.parse.urlencode(
        {
            "action": "query",
            "format": "json",
            "prop": "revisions",
            "rvlimit": "1",
            "rvdir": "newer",
            "rvprop": "timestamp|user|ids",
            "titles": title,
        },
    )
    request = urllib.request.Request(
        f"{FOM_API_URL}?{query}",
        headers={"User-Agent": "billymeierposts/1.0 provenance-audit"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def fetch_first_revision_rows(report_numbers: list[int]) -> tuple[list[dict[str, Any]], list[int]]:
    rows: list[dict[str, Any]] = []
    missing: list[int] = []
    for report_number in report_numbers:
        payload = fom_api_request(report_number)
        query = payload.get("query")
        if not query:
            missing.append(report_number)
            continue
        for page in query["pages"].values():
            title = page.get("title", "")
            match = REPORT_NUMBER_RE.search(title)
            if not match:
                continue
            report_number = int(match.group(1))
            if "missing" in page or not page.get("revisions"):
                missing.append(report_number)
                continue
            revision = page["revisions"][0]
            timestamp = revision.get("timestamp")
            if not timestamp:
                missing.append(report_number)
                continue
            rows.append(
                {
                    "report_number": report_number,
                    "page_title": title,
                    "page_url": FOM_PAGE_URL.format(report_number=report_number),
                    "api_url": f"{FOM_API_URL}?{urllib.parse.urlencode({'action': 'query', 'format': 'json', 'prop': 'revisions', 'rvlimit': '1', 'rvdir': 'newer', 'rvprop': 'timestamp|user|ids', 'titles': title.replace(' ', '_')})}",
                    "timestamp": timestamp,
                    "evidence_public_date": timestamp[:10],
                    "revid": revision.get("revid"),
                    "parentid": revision.get("parentid"),
                    "user": revision.get("user"),
                }
            )
    return sorted(rows, key=lambda row: row["report_number"]), sorted(set(missing))


def evidence_key(item: dict[str, Any]) -> tuple[Any, ...]:
    notes = f"MediaWiki first revision timestamp for {item['page_title']}."
    return (
        item["report_number"],
        "wiki_first_revision",
        parse_iso_date(item["evidence_public_date"]),
        f"Future Of Mankind {item['page_title']} first page revision",
        "",
        item["page_url"],
        "english",
        "English wiki page first revision timestamp",
        "",
        f"revid:{item['revid']}",
        notes,
    )


def build_insert_rows(
    evidence_rows: list[dict[str, Any]],
    report_map: dict[int, dict[str, Any]],
    run_id: int,
    existing_keys: set[tuple[Any, ...]],
) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for item in evidence_rows:
        key = evidence_key(item)
        if key in existing_keys:
            continue
        report = report_map.get(item["report_number"])
        if not report:
            raise RuntimeError(f"Missing contact_reports row for report {item['report_number']}.")
        rows.append(
            (
                run_id,
                report["id"],
                item["report_number"],
                report["report_date"],
                "wiki_first_revision",
                1,
                parse_iso_date(item["evidence_public_date"]),
                f"Future Of Mankind {item['page_title']} first page revision",
                None,
                item["page_url"],
                "english",
                "English wiki page first revision timestamp",
                None,
                f"revid:{item['revid']}",
                f"MediaWiki first revision timestamp for {item['page_title']}.",
                Json(
                    {
                        "script_version": SCRIPT_VERSION,
                        "page_title": item["page_title"],
                        "page_url": item["page_url"],
                        "api_url": item["api_url"],
                        "timestamp": item["timestamp"],
                        "revid": item["revid"],
                        "parentid": item["parentid"],
                        "user": item["user"],
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


def parse_report_numbers(value: str | None) -> list[int]:
    if not value:
        return []
    result = []
    for part in value.split(","):
        stripped = part.strip()
        if stripped:
            result.append(int(stripped))
    return sorted(set(result))


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
            stage2 = fetch_stage2_run(cur, args.stage2_run_key)
            report_numbers = parse_report_numbers(args.report_numbers) or fetch_scoped_report_numbers(cur, stage2["id"])
            report_map = fetch_report_map(cur, report_numbers)
            existing_keys = fetch_existing_evidence_keys(cur, report_numbers)
            if not args.dry_run:
                run_id = insert_run(
                    cur,
                    run_key,
                    {
                        "stage2_run_key": stage2["run_key"],
                        "report_count": len(report_numbers),
                        "report_numbers": report_numbers,
                        "family": "cross_family",
                    },
                    args.notes,
                )
                conn.commit()

        fetched_rows, missing_reports = fetch_first_revision_rows(report_numbers)
        insert_rows = build_insert_rows(fetched_rows, report_map, run_id or 0, existing_keys)

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
                        "stage2_run_key": args.stage2_run_key,
                        "report_count": len(report_numbers),
                        "fetched_count": len(fetched_rows),
                        "evidence_count": len(insert_rows),
                        "missing_report_numbers": missing_reports,
                    },
                )
                conn.commit()

        summary = {
            "run_key": run_key,
            "script_version": SCRIPT_VERSION,
            "stage2_run_key": args.stage2_run_key,
            "report_count": len(report_numbers),
            "fetched_count": len(fetched_rows),
            "evidence_count": len(insert_rows),
            "missing_report_numbers": missing_reports,
            "earliest_dates_sample": [
                {
                    "report_number": row["report_number"],
                    "evidence_public_date": row["evidence_public_date"],
                    "revid": row["revid"],
                }
                for row in fetched_rows[:10]
            ],
        }
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    except Exception as exc:  # pragma: no cover
        if run_id is not None and not args.dry_run:
            conn.rollback()
            with conn.cursor() as cur:
                update_run(cur, run_id, "failed", {"error": str(exc)})
                conn.commit()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

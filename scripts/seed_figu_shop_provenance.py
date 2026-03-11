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
from typing import Any

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_batch

from provenance_export_helpers import resolve_stage2_run


SCRIPT_VERSION = "stage0_provenance_figu_shop_v1"
FOM_REPORT_URL = "https://www.futureofmankind.co.uk/Billy_Meier/Contact_Report_{report_number}"
SOURCE_LINK_RE = re.compile(r'<a rel="nofollow" class="external text" href="([^"]+)">Source</a>')
SHOP_PUBLISHED_RE = re.compile(r'article:published_time" content="([^"]+)"')
SHOP_SCHEMA_PUBLISHED_RE = re.compile(r'"datePublished":"([^"]+)"')
SHOP_TITLE_RE = re.compile(r'<h1 class="product_title entry-title">([^<]+)</h1>')
BLOCK_RE = re.compile(r"block[- ](\d+)(?:-(\d+))?", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed official FIGU shop publication evidence from Future Of Mankind source links.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--stage2-run-key", help="Stage 2 run key used to scope included predictions. Defaults to latest completed Stage 2 run.")
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
    return date.fromisoformat(value[:10])


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
            Json({"script_version": SCRIPT_VERSION, "seed_kind": "figu_shop_source_link"}),
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


def http_get_text(url: str) -> tuple[str, str]:
    request = urllib.request.Request(url, headers={"User-Agent": "billymeierposts/1.0 provenance-audit"})
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.read().decode("utf-8", "ignore"), response.geturl()


def fetch_fom_source_url(report_number: int) -> str | None:
    html, _ = http_get_text(FOM_REPORT_URL.format(report_number=report_number))
    match = SOURCE_LINK_RE.search(html)
    return match.group(1) if match else None


def normalize_shop_url(source_url: str) -> str:
    parsed = urllib.parse.urlparse(source_url)
    path = parsed.path
    if "/produkt/" not in path:
        path = path.replace("/b%C3%BCcher/", "/produkt/")
        path = path.replace("/bücher/", "/produkt/")
    path = path.replace("kontakberichte", "kontaktberichte")
    if not path.endswith("/"):
        path = path + "/"
    return urllib.parse.urlunparse((parsed.scheme or "https", parsed.netloc, path, "", "", ""))


def extract_block_from_text(text: str) -> int | None:
    match = BLOCK_RE.search(text)
    return int(match.group(1)) if match else None


def fetch_shop_metadata(source_url: str, cache: dict[str, dict[str, Any]]) -> dict[str, Any]:
    requested_url = normalize_shop_url(source_url)
    cached = cache.get(requested_url)
    if cached is not None:
        return dict(cached)
    html, final_url = http_get_text(requested_url)
    article_published = SHOP_PUBLISHED_RE.search(html)
    schema_published = SHOP_SCHEMA_PUBLISHED_RE.search(html)
    title_match = SHOP_TITLE_RE.search(html)
    metadata = {
        "requested_url": requested_url,
        "final_url": final_url,
        "article_published_time": article_published.group(1) if article_published else None,
        "schema_date_published": schema_published.group(1) if schema_published else None,
        "title": title_match.group(1).strip() if title_match else None,
    }
    cache[requested_url] = dict(metadata)
    return metadata


def build_evidence_items(report_numbers: list[int]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    evidence_items: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    shop_cache: dict[str, dict[str, Any]] = {}
    for report_number in report_numbers:
        try:
            source_url = fetch_fom_source_url(report_number)
            if not source_url:
                skipped.append({"report_number": report_number, "reason": "missing_fom_source_link"})
                continue
            expected_block = extract_block_from_text(source_url)
            shop = fetch_shop_metadata(source_url, shop_cache)
            final_block = extract_block_from_text(shop["final_url"] or "") or extract_block_from_text(shop["title"] or "")
            if not shop["article_published_time"] and not shop["schema_date_published"]:
                skipped.append({"report_number": report_number, "reason": "missing_shop_date", "source_url": source_url})
                continue
            if expected_block is not None and final_block is not None and expected_block != final_block:
                skipped.append(
                    {
                        "report_number": report_number,
                        "reason": "shop_block_mismatch",
                        "source_url": source_url,
                        "final_url": shop["final_url"],
                        "expected_block": expected_block,
                        "final_block": final_block,
                    }
                )
                continue
            evidence_public_date = parse_iso_date(shop["schema_date_published"] or shop["article_published_time"])
            if evidence_public_date is None:
                skipped.append({"report_number": report_number, "reason": "undated_shop_page", "source_url": source_url})
                continue
            title = shop["title"] or f"FIGU shop product page for block {expected_block or final_block or 'unknown'}"
            notes = (
                f"The FIGU shop product page linked from Future Of Mankind exposes article:published_time/datePublished "
                f"as {(shop['schema_date_published'] or shop['article_published_time'])}; "
                f"Future Of Mankind source URL was {source_url}."
            )
            evidence_items.append(
                {
                    "report_number": report_number,
                    "evidence_kind": "publication_snapshot",
                    "evidence_quality": 2,
                    "evidence_public_date": evidence_public_date,
                    "source_label": f"FIGU shop product page for {title}",
                    "source_url": shop["final_url"] or shop["requested_url"],
                    "language": "german",
                    "edition_or_translation": "official FIGU shop product page",
                    "notes": notes,
                    "raw_evidence": {
                        "script_version": SCRIPT_VERSION,
                        "fom_source_url": source_url,
                        "shop_requested_url": shop["requested_url"],
                        "shop_final_url": shop["final_url"],
                        "article_published_time": shop["article_published_time"],
                        "schema_date_published": shop["schema_date_published"],
                        "shop_title": shop["title"],
                    },
                }
            )
        except Exception as exc:
            skipped.append({"report_number": report_number, "reason": "fetch_error", "error": str(exc)[:500]})
    return evidence_items, skipped


def evidence_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(item["report_number"]),
        item["evidence_kind"],
        item["evidence_public_date"],
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
    evidence_items: list[dict[str, Any]],
    report_map: dict[int, dict[str, Any]],
    run_id: int,
    existing_keys: set[tuple[Any, ...]],
) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for item in evidence_items:
        if evidence_key(item) in existing_keys:
            continue
        report = report_map[int(item["report_number"])]
        rows.append(
            (
                run_id,
                report["id"],
                int(item["report_number"]),
                report["report_date"],
                item["evidence_kind"],
                int(item["evidence_quality"]),
                item["evidence_public_date"],
                item.get("source_label"),
                item.get("source_path"),
                item.get("source_url"),
                item.get("language"),
                item.get("edition_or_translation"),
                item.get("translator"),
                item.get("source_hash"),
                item.get("notes"),
                Json(item.get("raw_evidence") or {}),
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
            evidence_kind AS public_date_basis,
            source_label,
            source_url
        FROM ranked
        WHERE rn = 1
        ORDER BY report_number
        """,
        (report_numbers,),
    )
    return [dict(row) for row in cur.fetchall()]


def update_predictions_from_rollups(cur, rollups: list[dict[str, Any]]) -> None:
    params = []
    for row in rollups:
        earliest_public_date = row["earliest_public_date"]
        provenance_score = 2 if earliest_public_date else 1
        params.append(
            (
                earliest_public_date,
                row["public_date_basis"],
                provenance_score,
                int(row["report_number"]),
            )
        )
    execute_batch(
        cur,
        """
        UPDATE public.prediction_audit_predictions
        SET earliest_provable_public_date = %s,
            public_date_basis = %s,
            provenance_score = %s
        WHERE report_number = %s
        """,
        params,
        page_size=200,
    )


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
            stage2 = resolve_stage2_run(cur, args.stage2_run_key)
            if args.report_numbers:
                report_numbers = sorted({int(value.strip()) for value in args.report_numbers.split(",") if value.strip()})
            else:
                report_numbers = fetch_scoped_report_numbers(cur, int(stage2["id"]))
            report_map = fetch_report_map(cur, report_numbers)
            existing_keys = fetch_existing_evidence_keys(cur, report_numbers)
            if not args.dry_run:
                run_id = insert_run(
                    cur,
                    run_key,
                    {"stage2_run_key": stage2["run_key"], "seed_kind": "figu_shop_source_link", "report_count": len(report_numbers)},
                    args.notes,
                )
                conn.commit()

        evidence_items, skipped = build_evidence_items(report_numbers)
        insert_rows = build_insert_rows(evidence_items, report_map, run_id or 0, existing_keys)

        if not args.dry_run and run_id is not None:
            with conn.cursor() as cur:
                insert_evidence(cur, insert_rows)
                rollups = fetch_rollups(cur, report_numbers)
                update_predictions_from_rollups(cur, rollups)
                update_run(
                    cur,
                    run_id,
                    "completed",
                    {
                        "stage2_run_key": stage2["run_key"],
                        "report_count": len(report_numbers),
                        "evidence_count": len(insert_rows),
                        "skipped_count": len(skipped),
                        "script_version": SCRIPT_VERSION,
                    },
                )
            conn.commit()

        print(
            json.dumps(
                {
                    "run_key": run_key,
                    "dry_run": args.dry_run,
                    "stage2_run_key": stage2["run_key"],
                    "report_count": len(report_numbers),
                    "evidence_count": len(insert_rows),
                    "skipped_count": len(skipped),
                    "skipped_preview": skipped[:10],
                    "script_version": SCRIPT_VERSION,
                },
                indent=2,
                default=str,
            )
        )
        return 0
    except Exception as exc:
        conn.rollback()
        if not args.dry_run and run_id is not None:
            with conn.cursor() as cur:
                update_run(cur, run_id, "failed", {"error": str(exc)[:1000], "script_version": SCRIPT_VERSION})
            conn.commit()
        print(f"FIGU shop provenance seed failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

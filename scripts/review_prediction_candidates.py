#!/usr/bin/env python3

from __future__ import annotations

import argparse
import calendar
import json
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import psycopg2
from psycopg2.extras import Json, execute_batch

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.parse_contact_report_predictions import (
    ACTOR_PATTERN,
    FUTURE_MARKER_PATTERN,
    HEDGE_PATTERN,
    LOCATION_PATTERN,
    STATE_CHANGE_WORDS,
    TIME_PATTERN,
    TREND_WORDS,
    normalize_claim_text,
    provisional_event_family,
)


REVIEW_VERSION = "stage2_rules_v2"
STOPWORDS = {
    "about",
    "after",
    "again",
    "also",
    "already",
    "another",
    "around",
    "because",
    "become",
    "being",
    "bring",
    "coming",
    "does",
    "earth",
    "earthhuman",
    "even",
    "fact",
    "first",
    "from",
    "future",
    "have",
    "human",
    "humankind",
    "humanity",
    "humans",
    "into",
    "just",
    "later",
    "longer",
    "many",
    "more",
    "must",
    "next",
    "only",
    "other",
    "people",
    "rather",
    "said",
    "same",
    "shall",
    "should",
    "soon",
    "some",
    "than",
    "that",
    "their",
    "them",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "today",
    "very",
    "what",
    "when",
    "where",
    "which",
    "while",
    "will",
    "with",
    "world",
    "would",
    "years",
}
META_FUTURE_PATTERN = re.compile(
    r"""
    \b(
        contact\ report|conversation|greeting|greetings|interest\ you|letter|question|questions|
        tell\ you|visit\ you|meet\ again|write|written|reading|reported
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)
GENERIC_FUTURE_PATTERN = re.compile(
    r"""
    \b(
        human\ beings?\s+will|
        humanity\s+will|
        people\s+will|
        governments?\s+will|
        there\s+will\s+be|
        the\ world\s+will
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)
CONSEQUENTIAL_PATTERN = re.compile(
    r"""
    \b(
        assassination|assassinated|attack|catastrophe|collapse|crash|death|destroy|destruction|
        earthquake|epidemic|eruption|explode|exploded|famine|fall\ to\ earth|flood|hurricane|
        hydrazine|kill|losses|pandemic|poison|referendum|satellite|shot\ down|storm|terror|
        toxic|tuberculosis|virus|war
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)
MECHANISM_PATTERN = re.compile(
    r"""
    \b(
        by\ poison|heart\ attack|voting\ period|conclave|magnitude|richter|due\ to|as\ a\ result\ of|
        carried\ out|exploded|fuel\ content|rocket|shot\ down|spread|sweep\ the\ planet|toxic\ fuel
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)
LOCATION_GENERIC_VALUES = {
    "Earth",
    "world",
    "World",
    "Earth human",
    "Earth-human",
    "human beings",
}
MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


@dataclass
class Stage2Result:
    prediction_id: int
    bundle_key: str | None
    bundle_role: str
    bundle_component_count: int | None
    stage2_label: str
    meaningfulness_score: int | None
    measurability_score: int | None
    provenance_score: int | None
    event_family_final: str | None
    time_window_start: date | None
    time_window_end: date | None
    public_date_basis: str | None
    target_type: str | None
    target_name: str | None
    actor_name: str | None
    magnitude_min: float | None
    magnitude_max: float | None
    severity_band: str | None
    prediction_family_key: str | None
    duplicate_of_prediction_id: int | None
    rulebook_version: str | None
    eligible: bool
    significant: bool
    match_status: str
    review_notes: str
    stage2_meta: dict[str, object]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 2 review/normalization for prediction candidates.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--parse-run-key", help="Stage 1 parse run key. Defaults to latest completed Stage 1 run.")
    parser.add_argument("--run-key", help="Unique Stage 2 run key. Defaults to an auto-generated timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument("--limit", type=int, help="Limit number of candidate rows reviewed.")
    parser.add_argument("--only-pending", action="store_true", help="Review only rows with stage2_label='pending_review'.")
    parser.add_argument("--dry-run", action="store_true", help="Score without writing updates.")
    parser.add_argument("--batch-size", type=int, default=200, help="DB update batch size.")
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage2-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def fetch_stage1_run(cur, parse_run_key: str | None) -> tuple[int, str]:
    if parse_run_key:
        cur.execute(
            """
            SELECT id, run_key
            FROM public.prediction_audit_runs
            WHERE run_key = %s AND stage = 'stage1_candidate_extraction'
            """,
            (parse_run_key,),
        )
    else:
        cur.execute(
            """
            SELECT id, run_key
            FROM public.prediction_audit_runs
            WHERE stage = 'stage1_candidate_extraction' AND status = 'completed'
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("No completed Stage 1 parse run found.")
    return row[0], row[1]


def insert_run(cur, run_key: str, source_filter: dict[str, object], notes: str | None) -> int:
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
        VALUES (%s, 'stage2_eligibility', 'running', %s, %s, %s, %s, %s, %s, now())
        RETURNING id
        """,
        (
            run_key,
            REVIEW_VERSION,
            "none",
            "public.prediction_audit_predictions",
            Json(source_filter),
            notes or None,
            Json({"mode": "stage2_rule_review"}),
        ),
    )
    return cur.fetchone()[0]


def update_run(cur, run_id: int, status: str, run_meta: dict[str, object], notes: str | None = None) -> None:
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


def fetch_candidates(cur, parse_run_id: int, only_pending: bool, limit: int | None) -> list[tuple]:
    where_clauses = ["parse_run_id = %s"]
    params: list[object] = [parse_run_id]
    if only_pending:
        where_clauses.append("stage2_label = 'pending_review'")
    sql = f"""
        SELECT
            id,
            bundle_key,
            bundle_role,
            bundle_component_count,
            report_number,
            candidate_seq,
            claimed_contact_date,
            earliest_provable_public_date,
            public_date_basis,
            candidate_class,
            claim_normalized,
            event_family_provisional,
            time_text,
            location_text,
            actor_text,
            magnitude_text,
            conditionality,
            ambiguity_flags,
            extractor_confidence,
            future_claim_present
        FROM public.prediction_audit_predictions
        WHERE {' AND '.join(where_clauses)}
        ORDER BY claimed_contact_date, report_number, candidate_seq, id
    """
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    cur.execute(sql, params)
    return cur.fetchall()


def infer_event_family(claim: str, provisional: str | None) -> str | None:
    return provisional or provisional_event_family(claim)


def parse_month_day_variant(text: str, base_year: int, claimed_date: date) -> tuple[date, str] | None:
    patterns = [
        (r"\bon\s+the\s+(\d{1,2})(?:st|nd|rd|th)?\s+of\s+([A-Za-z]+)(?:,\s*(\d{4}))?", "exact_month_day"),
        (r"\bon\s+(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)(?:,\s*(\d{4}))?", "exact_month_day"),
        (r"\buntil\s+the\s+(\d{1,2})(?:st|nd|rd|th)?\s+of\s+([A-Za-z]+)(?:,\s*(\d{4}))?", "until_month_day"),
        (r"\buntil\s+(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)(?:,\s*(\d{4}))?", "until_month_day"),
    ]
    for pattern, basis in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        day = int(match.group(1))
        month_name = match.group(2).lower()
        year = int(match.group(3)) if match.group(3) else base_year
        month = MONTHS.get(month_name)
        if not month:
            return None
        candidate = date(year, month, day)
        if match.group(3) is None and candidate < claimed_date:
            candidate = date(year + 1, month, day)
        return candidate, basis
    return None


def add_months(source: date, months: int) -> date:
    year = source.year + (source.month - 1 + months) // 12
    month = (source.month - 1 + months) % 12 + 1
    day = min(source.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def normalize_time_window(claimed_date: date, claim: str, time_text: str | None) -> tuple[date | None, date | None, str | None]:
    lower_claim = claim.lower()
    exact = parse_month_day_variant(claim, claimed_date.year, claimed_date)
    if exact:
        if exact[1] == "until_month_day":
            return claimed_date, exact[0], "until_month_day"
        return exact[0], exact[0], exact[1]

    full_date = re.search(r"\bon\s+([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})\b", claim, re.IGNORECASE)
    if full_date:
        month = MONTHS.get(full_date.group(1).lower())
        if month:
            candidate = date(int(full_date.group(3)), month, int(full_date.group(2)))
            return candidate, candidate, "exact_full_date"

    months_range = re.search(r"\b(\d{1,2})\s+to\s+(\d{1,2})\s+months\s+from\s+today\b", lower_claim)
    if months_range:
        start = add_months(claimed_date, int(months_range.group(1)))
        end = add_months(claimed_date, int(months_range.group(2)))
        return start, end, "relative_month_range"

    single_months = re.search(r"\bin\s+(\d{1,2})\s+months\b", lower_claim)
    if single_months:
        target = add_months(claimed_date, int(single_months.group(1)))
        return target, target, "relative_months"

    weeks = re.search(r"\bin\s+(\d{1,2})\s+weeks\b", lower_claim)
    if weeks:
        target = claimed_date + timedelta(weeks=int(weeks.group(1)))
        return target, target, "relative_weeks"

    if "tomorrow" in lower_claim:
        target = claimed_date + timedelta(days=1)
        return target, target, "tomorrow"
    if "next week" in lower_claim:
        return claimed_date, claimed_date + timedelta(days=7), "next_week"
    if "next month" in lower_claim:
        start = add_months(claimed_date, 1)
        end = add_months(claimed_date, 2) - timedelta(days=1)
        return start, end, "next_month"
    if "next year" in lower_claim:
        start = date(claimed_date.year + 1, 1, 1)
        end = date(claimed_date.year + 1, 12, 31)
        return start, end, "next_year"
    if "later this year" in lower_claim or "middle of this year" in lower_claim or "by the middle of this year" in lower_claim:
        return claimed_date, date(claimed_date.year, 12, 31), "later_this_year"

    year_match = re.search(r"\b(?:in|by)\s+(\d{4})\b", lower_claim)
    if year_match:
        year = int(year_match.group(1))
        start = claimed_date if lower_claim.startswith("by ") or f"by {year}" in lower_claim else date(year, 1, 1)
        end = date(year, 12, 31)
        return start, end, "calendar_year"

    if "turn of the millennium" in lower_claim or "turn of the century" in lower_claim:
        return date(1998, 1, 1), date(2002, 12, 31), "turn_of_millennium"
    if "new millennium" in lower_claim:
        return date(2000, 1, 1), date(2009, 12, 31), "new_millennium"

    phrase = (time_text or "").lower()
    if phrase == "soon":
        return claimed_date, add_months(claimed_date, 24), "soon_0_2y"
    if phrase == "before long":
        return claimed_date, add_months(claimed_date, 36), "before_long_0_3y"
    if phrase == "in the near future":
        return claimed_date, add_months(claimed_date, 60), "near_future_0_5y"
    if phrase == "in the coming years":
        return claimed_date, add_months(claimed_date, 120), "coming_years_0_10y"
    if phrase == "one day":
        return None, None, "one_day_vague"

    return None, None, None


def clean_location(location_text: str | None) -> str | None:
    if not location_text:
        return None
    value = normalize_claim_text(location_text).strip(",.")
    if value in LOCATION_GENERIC_VALUES:
        return None
    return value


def clean_actor(actor_text: str | None) -> str | None:
    if not actor_text:
        return None
    value = normalize_claim_text(actor_text).strip(",.")
    if value.lower() in {"i", "we", "you", "they", "he", "she", "it", "what", "then"}:
        return None
    return value


def parse_magnitude(magnitude_text: str | None, claim: str) -> tuple[float | None, float | None, str | None]:
    text = magnitude_text or claim
    numeric = re.search(r"\bmagnitude(?:\s+of\s+about)?\s+(\d+(?:\.\d+)?)", text, re.IGNORECASE)
    if numeric:
        value = float(numeric.group(1))
        return value, value, severity_band_from_text(text)
    return None, None, severity_band_from_text(text)


def severity_band_from_text(text: str | None) -> str | None:
    if not text:
        return None
    lower = text.lower()
    if "devastating" in lower or "gigantic" in lower:
        return "devastating"
    if "severe" in lower or "very heavy" in lower:
        return "severe"
    if "strong" in lower or "heavy" in lower:
        return "strong"
    return None


def score_meaningfulness(claim: str, family: str | None, time_window: tuple[date | None, date | None], location: str | None, actor: str | None, magnitude_text: str | None) -> tuple[int, list[str]]:
    reasons: list[str] = []
    raw_score = 0
    if family:
        raw_score += 1
        reasons.append("event_family")
    if location or actor:
        raw_score += 1
        reasons.append("target")
    if magnitude_text or CONSEQUENTIAL_PATTERN.search(claim):
        raw_score += 1
        reasons.append("severity_or_consequence")
    if time_window[0] and time_window[1]:
        raw_score += 1
        reasons.append("time_window")

    if GENERIC_FUTURE_PATTERN.search(claim) and raw_score <= 1:
        return 0, ["generic_broad_claim"]
    if META_FUTURE_PATTERN.search(claim) and raw_score <= 1:
        return 0, ["meta_conversation"]
    if HEDGE_PATTERN.search(claim) and raw_score <= 2:
        raw_score -= 1
        reasons.append("hedged_penalty")
    if len(claim.split()) < 6 and raw_score <= 1:
        return 0, ["too_short"]

    if raw_score <= 0:
        return 0, reasons or ["no_specificity"]
    if raw_score == 1:
        return 1, reasons
    if raw_score in (2, 3):
        return 2, reasons
    return 3, reasons


def score_measurability(claim: str, family: str | None, time_window: tuple[date | None, date | None], location: str | None, actor: str | None, magnitude_text: str | None) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    if family:
        score += 1
        reasons.append("event_family")
    if time_window[0] and time_window[1]:
        score += 1
        reasons.append("time")
    if location or actor:
        score += 1
        reasons.append("target")
    if magnitude_text or MECHANISM_PATTERN.search(claim) or severity_band_from_text(claim):
        score += 1
        reasons.append("severity_or_mechanism")
    return score, reasons


def score_provenance(claimed_date: date | None, earliest_public_date: date | None) -> tuple[int, str]:
    if earliest_public_date is not None:
        return 2, "earliest_public_date_recorded"
    if claimed_date is not None:
        return 1, "claimed_contact_date_only"
    return 0, "missing_dates"


def build_family_key(family: str | None, location: str | None, actor: str | None, start: date | None, end: date | None, claim: str) -> str | None:
    anchor = location or actor
    if not family and not anchor and not (start and end):
        return None
    tokens = []
    for token in re.findall(r"[a-z0-9']+", claim.lower()):
        if token in STOPWORDS or len(token) < 4:
            continue
        tokens.append(token)
    fingerprint = "-".join(tokens[:8]) if tokens else "generic"
    time_bucket = f"{start.isoformat() if start else 'na'}_{end.isoformat() if end else 'na'}"
    anchor_norm = re.sub(r"[^a-z0-9]+", "-", (anchor or "na").lower()).strip("-")
    family_norm = family or "unknown"
    return "|".join([family_norm, anchor_norm or "na", time_bucket, fingerprint])


def build_result(row: tuple) -> Stage2Result:
    (
        prediction_id,
        bundle_key,
        bundle_role,
        bundle_component_count,
        report_number,
        candidate_seq,
        claimed_contact_date,
        earliest_public_date,
        public_date_basis,
        candidate_class,
        claim_normalized,
        event_family_provisional,
        time_text,
        location_text,
        actor_text,
        magnitude_text,
        conditionality,
        ambiguity_flags,
        extractor_confidence,
        future_claim_present,
    ) = row

    claim = normalize_claim_text(claim_normalized)
    family = infer_event_family(claim, event_family_provisional)
    location = clean_location(location_text)
    actor = clean_actor(actor_text)
    time_start, time_end, time_basis = normalize_time_window(claimed_contact_date, claim, time_text)
    magnitude_min, magnitude_max, severity_band = parse_magnitude(magnitude_text, claim)
    meaningfulness_score, meaning_reasons = score_meaningfulness(claim, family, (time_start, time_end), location, actor, magnitude_text)
    measurability_score, measure_reasons = score_measurability(claim, family, (time_start, time_end), location, actor, magnitude_text)
    provenance_score, derived_public_basis = score_provenance(claimed_contact_date, earliest_public_date)

    if not future_claim_present and not FUTURE_MARKER_PATTERN.search(claim):
        label = "not_a_prediction"
    elif meaningfulness_score < 1 and measurability_score < 1:
        label = "not_a_prediction"
    elif provenance_score < 1:
        label = "prediction_with_weak_provenance"
    elif meaningfulness_score >= 2 and measurability_score >= 3:
        label = "significant_prediction"
    elif meaningfulness_score >= 1 and measurability_score >= 2:
        label = "eligible_prediction"
    elif meaningfulness_score < 2:
        label = "prediction_but_not_meaningful"
    else:
        label = "prediction_but_not_measurable"

    eligible = label in {"eligible_prediction", "significant_prediction"}
    significant = label == "significant_prediction"
    target_type = "none"
    target_name = None
    if location:
        target_type = "region"
        target_name = location
    elif actor:
        target_type = "actor"
        target_name = actor

    family_key = build_family_key(family, location, actor, time_start, time_end, claim)
    rulebook_version = f"{family}_v1" if family else "generic_v1"
    match_status = "unreviewed" if eligible else "excluded"
    notes = f"meaning={meaningfulness_score}; measurable={measurability_score}; provenance={provenance_score}"
    stage2_meta = {
        "review_version": REVIEW_VERSION,
        "report_number": report_number,
        "candidate_seq": candidate_seq,
        "time_basis": time_basis,
        "meaning_reasons": meaning_reasons,
        "measurability_reasons": measure_reasons,
        "conditionality": conditionality,
        "bundle_key": bundle_key,
        "bundle_role": bundle_role,
        "bundle_component_count": bundle_component_count,
        "ambiguity_flags": ambiguity_flags or [],
        "extractor_confidence": float(extractor_confidence) if extractor_confidence is not None else None,
        "family_key_inputs": {
            "event_family_final": family,
            "location": location,
            "actor": actor,
            "time_start": time_start.isoformat() if time_start else None,
            "time_end": time_end.isoformat() if time_end else None,
        },
    }
    return Stage2Result(
        prediction_id=prediction_id,
        bundle_key=bundle_key,
        bundle_role=bundle_role,
        bundle_component_count=bundle_component_count,
        stage2_label=label,
        meaningfulness_score=meaningfulness_score,
        measurability_score=measurability_score,
        provenance_score=provenance_score,
        event_family_final=family,
        time_window_start=time_start,
        time_window_end=time_end,
        public_date_basis=public_date_basis or derived_public_basis,
        target_type=target_type,
        target_name=target_name,
        actor_name=actor,
        magnitude_min=magnitude_min,
        magnitude_max=magnitude_max,
        severity_band=severity_band,
        prediction_family_key=family_key,
        duplicate_of_prediction_id=None,
        rulebook_version=rulebook_version,
        eligible=eligible,
        significant=significant,
        match_status=match_status,
        review_notes=notes,
        stage2_meta=stage2_meta,
    )


def apply_duplicates(results: list[Stage2Result]) -> None:
    first_by_key: dict[str, int] = {}
    for result in results:
        key = result.prediction_family_key
        if not key:
            continue
        if not result.eligible:
            continue
        if key in first_by_key:
            result.duplicate_of_prediction_id = first_by_key[key]
            result.stage2_label = "duplicate_restating_prior_prediction"
            result.eligible = False
            result.significant = False
            result.match_status = "excluded"
            result.stage2_meta["duplicate_reason"] = "matching_prediction_family_key"
            result.stage2_meta["duplicate_of_prediction_id"] = first_by_key[key]
        else:
            first_by_key[key] = result.prediction_id


def update_predictions(cur, stage2_run_id: int, results: Iterable[Stage2Result], batch_size: int) -> None:
    params = [
        (
            stage2_run_id,
            Json(result.stage2_meta),
            result.stage2_label,
            result.meaningfulness_score,
            result.measurability_score,
            result.provenance_score,
            result.event_family_final,
            result.time_window_start,
            result.time_window_end,
            result.public_date_basis,
            result.target_type,
            result.target_name,
            result.actor_name,
            result.magnitude_min,
            result.magnitude_max,
            result.severity_band,
            result.prediction_family_key,
            result.duplicate_of_prediction_id,
            result.rulebook_version,
            result.eligible,
            result.significant,
            result.match_status,
            result.review_notes,
            result.prediction_id,
        )
        for result in results
    ]
    execute_batch(
        cur,
        """
        UPDATE public.prediction_audit_predictions
        SET last_stage2_run_id = %s,
            stage2_reviewed_at = now(),
            stage2_meta = %s,
            stage2_label = %s,
            meaningfulness_score = %s,
            measurability_score = %s,
            provenance_score = %s,
            event_family_final = %s,
            time_window_start = %s,
            time_window_end = %s,
            public_date_basis = %s,
            target_type = %s,
            target_name = %s,
            actor_name = %s,
            magnitude_min = %s,
            magnitude_max = %s,
            severity_band = %s,
            prediction_family_key = %s,
            duplicate_of_prediction_id = %s,
            rulebook_version = %s,
            eligible = %s,
            significant = %s,
            match_status = %s,
            review_notes = %s
        WHERE id = %s
        """,
        params,
        page_size=batch_size,
    )


def summarize(results: list[Stage2Result]) -> dict[str, object]:
    label_counts = Counter(result.stage2_label for result in results)
    significant_by_family = Counter(result.event_family_final for result in results if result.significant and result.event_family_final)
    bundle_count = len({result.bundle_key for result in results if result.bundle_key})
    return {
        "reviewed_rows": len(results),
        "bundle_rows": bundle_count,
        "label_counts": dict(sorted(label_counts.items())),
        "significant_by_family": dict(significant_by_family.most_common(20)),
    }


def update_bundles(cur, parse_run_id: int, stage2_run_id: int) -> None:
    cur.execute(
        """
        WITH bundle_rollup AS (
            SELECT
                p.bundle_key,
                count(*) AS child_count,
                count(*) FILTER (WHERE p.eligible) AS eligible_child_count,
                count(*) FILTER (WHERE p.significant) AS significant_child_count
            FROM public.prediction_audit_predictions p
            WHERE p.parse_run_id = %s
              AND p.bundle_key IS NOT NULL
            GROUP BY p.bundle_key
        )
        UPDATE public.prediction_audit_bundles b
        SET bundle_significant = (r.significant_child_count = r.child_count AND r.child_count >= 2),
            bundle_meta = COALESCE(b.bundle_meta, '{}'::jsonb) || jsonb_build_object(
                'last_stage2_run_id', %s,
                'child_count', r.child_count,
                'eligible_child_count', r.eligible_child_count,
                'significant_child_count', r.significant_child_count
            )
        FROM bundle_rollup r
        WHERE b.bundle_key = r.bundle_key
        """,
        (parse_run_id, stage2_run_id),
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
    stage2_run_id: int | None = None

    try:
        with conn.cursor() as cur:
            parse_run_id, resolved_parse_run_key = fetch_stage1_run(cur, args.parse_run_key)
            candidates = fetch_candidates(cur, parse_run_id, args.only_pending, args.limit)

            if not args.dry_run:
                stage2_run_id = insert_run(
                    cur,
                    run_key,
                    {
                        "parse_run_key": resolved_parse_run_key,
                        "only_pending": args.only_pending,
                        "limit": args.limit,
                    },
                    args.notes,
                )
                conn.commit()

        results = [build_result(row) for row in candidates]
        apply_duplicates(results)
        summary = summarize(results)

        if not args.dry_run and stage2_run_id is not None:
            for index in range(0, len(results), args.batch_size):
                batch = results[index : index + args.batch_size]
                with conn.cursor() as cur:
                    update_predictions(cur, stage2_run_id, batch, args.batch_size)
                conn.commit()

            with conn.cursor() as cur:
                update_bundles(cur, parse_run_id, stage2_run_id)
            conn.commit()

            with conn.cursor() as cur:
                update_run(
                    cur,
                    stage2_run_id,
                    "completed",
                    {
                        "parse_run_key": resolved_parse_run_key,
                        "reviewed_rows": len(results),
                        "label_counts": summary["label_counts"],
                        "significant_by_family": summary["significant_by_family"],
                        "review_version": REVIEW_VERSION,
                    },
                )
            conn.commit()

        output = {
            "run_key": run_key,
            "dry_run": args.dry_run,
            "parse_run_key": resolved_parse_run_key,
            **summary,
        }
        print(json.dumps(output, indent=2))
        return 0
    except Exception as exc:
        conn.rollback()
        if not args.dry_run and stage2_run_id is not None:
            with conn.cursor() as cur:
                update_run(
                    cur,
                    stage2_run_id,
                    "failed",
                    {"error": str(exc)[:1000], "review_version": REVIEW_VERSION},
                )
            conn.commit()
        print(f"Stage 2 review failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

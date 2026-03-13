#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import psycopg2
from psycopg2.extras import Json, execute_values


PARSER_VERSION = "stage1_regex_v4"
PROMPT_VERSION = "none"
SOURCE_CORPUS = "public.contact_reports.english_content"
FUTURE_MARKER_PATTERN = re.compile(
    r"""
    \b(
        will|
        shall|
        is\ going\ to|
        are\ going\ to|
        can\ be\ expected\ to|
        is\ expected\ to|
        are\ expected\ to|
        one\ day|
        in\ the\ near\ future|
        in\ the\ coming\ years|
        before\ long|
        sooner\ or\ later|
        from\ now\ on|
        next\ year|
        next\ month|
        next\ week|
        tomorrow
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)
HEDGE_PATTERN = re.compile(r"\b(might|may|could|possibly|perhaps|one day|eventually)\b", re.IGNORECASE)
TIME_PATTERN = re.compile(
    r"""
    (
        on\s+the\s+\d{1,2}(?:st|nd|rd|th)?\s+of\s+[A-Z][a-z]+|
        on\s+\d{1,2}(?:st|nd|rd|th)?\s+[A-Z][a-z]+|
        on\s+[A-Z][a-z]+\s+\d{1,2},?\s+\d{4}|
        until\s+the\s+\d{1,2}(?:st|nd|rd|th)?\s+of\s+[A-Z][a-z]+|
        until\s+\d{1,2}(?:st|nd|rd|th)?\s+[A-Z][a-z]+|
        at\s+about\s+the\s+same\s+time|
        at\s+the\s+same\s+time|
        by\s+\d{4}|
        in\s+\d{4}|
        around\s+the\s+turn\s+of\s+the\s+millennium|
        around\s+the\s+turn\s+of\s+the\s+century|
        in\s+the\s+new\s+millennium|
        in\s+the\s+coming\s+years|
        in\s+the\s+near\s+future|
        before\s+long|
        one\s+day|
        tomorrow|
        next\s+(?:day|week|month|year)|
        soon
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)
LOCATION_PATTERN = re.compile(
    r"""
    \b(?:in|at|near|around|off|from)\s+
    (
        [A-Z][A-Za-z'`.-]+
        (?:\s+[A-Z][A-Za-z'`.-]+){0,5}
    )
    """,
    re.VERBOSE,
)
ACTOR_PATTERN = re.compile(
    r"""
    \b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})\b
    \s+
    (?:will|shall|is\ going\ to|are\ going\ to|officially\s+resigns?|resigns?|is\ declared|declares?|apologi[sz]es?)
    """,
    re.VERBOSE,
)
DECLARATIVE_PREDICTIVE_PREFIX_PATTERN = re.compile(
    r"""
    ^\s*
    (?:
        then|
        and\ then|
        at\s+about\s+the\s+same\s+time|
        at\s+the\s+same\s+time|
        on\s+(?:the\s+)?\d{1,2}(?:st|nd|rd|th)?(?:\s+of)?\s+[A-Z][a-z]+|
        until\s+(?:the\s+)?\d{1,2}(?:st|nd|rd|th)?(?:\s+of)?\s+[A-Z][a-z]+
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)
DECLARATIVE_TIME_PATTERN = re.compile(
    r"""
    (
        on\s+(?:the\s+)?\d{1,2}(?:st|nd|rd|th)?(?:\s+of)?\s+[A-Z][a-z]+|
        until\s+(?:the\s+)?\d{1,2}(?:st|nd|rd|th)?(?:\s+of)?\s+[A-Z][a-z]+|
        at\s+about\s+the\s+same\s+time|
        at\s+the\s+same\s+time
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)
DECLARATIVE_EVENT_PATTERN = re.compile(
    r"""
    \b(
        is\ declared|
        declared\ independent|
        resigns?|
        is\ shot\ down|
        shot\ down|
        exploded|
        apologi[sz]es?|
        recogni[sz]es?|
        is\ recogni[sz]ed|
        occurs?|
        takes\ place
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)
DECLARATIVE_ACTOR_PATTERN = re.compile(
    r"""
    (?:
        on\s+(?:the\s+)?\d{1,2}(?:st|nd|rd|th)?(?:\s+of)?\s+[A-Z][a-z]+\s+
    )?
    \b([A-Z][A-Za-z'`.-]+(?:\s+[A-Z][A-Za-z'`.-]+){0,3})\b
    \s+
    (?:is\ declared|declared\ independent|officially\s+resigns?|resigns?|apologi[sz]es?)
    """,
    re.VERBOSE,
)
PRIVATE_FUTURE_PREFIX_PATTERN = re.compile(
    r"""
    ^\s*
    (?:
        and\s+|
        but\s+|
        of\ course\s+|
        then\s+
    )*
    (?:i|we|you)\s+
    (?:will|shall|am\ going\ to|are\ going\ to)\b
    """,
    re.IGNORECASE | re.VERBOSE,
)
PRIVATE_ACTION_VERB_PATTERN = re.compile(
    r"""
    \b(
        answer|ask|bring|call|come|convey|give|get|go|greet|hear|leave|meet|order|pour|
        read|record|remove|reply|say|see|speak|take\ care|tell|visit|wait|write
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)
WORLD_SCOPE_PATTERN = re.compile(
    r"""
    \b(
        asteroid|birth|black\ hole|bloodshed|catastrophe|climate|collapse|comet|conflict|crash|
        dna|disease|earth|earthquake|economy|election|epidemic|eruption|eu|famine|flood|
        food|gene|genetic|government|human\ beings?|humanity|immigration|land|migration|
        nation|overpopulation|pandemic|people|planet|pope|population|president|quake|
        raw\ materials|referendum|resources?|russia|serbia|solar\ system|storm|transplant|
        un|virus|volcano|war|water|world|yeltsin
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)
META_CONVERSATION_PATTERN = re.compile(
    r"""
    \b(
        contact\ report|conversation|greeting|greetings|interview\ report|letter|
        question|questions|visit|writing|written
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)
SPLIT_PATTERN = re.compile(
    r"""
    \s*(?:;|(?<!\d)\.\s+|,\s+(?=(?:and|but)\s+[^,;:.!?]{0,120}\b(?:will|shall|is\ going\ to|are\ going\ to)\b))
    """,
    re.IGNORECASE | re.VERBOSE,
)
COMPOUND_SPLITTER_PATTERNS = [
    re.compile(r"\s*,\s*while\s+on\s+the\s+same\s+day,?\s*", re.IGNORECASE),
    re.compile(r"\s*,\s*while\s+", re.IGNORECASE),
    re.compile(r"\s*,\s*after\s+which\s+", re.IGNORECASE),
    re.compile(r"\s*,\s*followed\s+by\s+", re.IGNORECASE),
    re.compile(r"\s*,\s*as\s+well\s+as\s+", re.IGNORECASE),
    re.compile(r"\s*,\s*and\s+in\s+", re.IGNORECASE),
    re.compile(r"\s*,\s*and\s+another\s+", re.IGNORECASE),
    re.compile(r"\s*,\s*and\s+on\s+the\s+", re.IGNORECASE),
]
EVENTISH_PATTERN = re.compile(
    r"""
    \b(
        earthquake|seaquake|quake|volcano|eruption|storm|hurricane|flood|war|attack|
        epidemic|pandemic|disease|virus|election|pope|president|comet|asteroid|
        satellite|rocket|independence|independent|resign|apology
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)
LINE_PREFIX_PATTERN = re.compile(r"^\s*\d+\.\s*")
SKIP_LINE_PATTERNS = [
    re.compile(r"^\s*English Translation\s*$", re.IGNORECASE),
    re.compile(r"^\s*[A-Z][A-Za-z -]+Contact\s*$"),
    re.compile(r"^\s*(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),", re.IGNORECASE),
    re.compile(r"^\s*[A-Z][A-Za-z]+:\s*$"),
]

EVENT_FAMILY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("earthquake", re.compile(r"\b(earthquake|seaquake|quake|richter)\b", re.IGNORECASE)),
    ("volcano", re.compile(r"\b(volcano|eruption|erupt)\b", re.IGNORECASE)),
    ("storm", re.compile(r"\b(storm|hurricane|cyclone|typhoon|tornado|flood)\b", re.IGNORECASE)),
    ("war_conflict", re.compile(r"\b(war|civil war|conflict|attack|invasion|terror|terrorist)\b", re.IGNORECASE)),
    ("epidemic", re.compile(r"\b(epidemic|pandemic|disease|virus|plague|infection)\b", re.IGNORECASE)),
    ("aviation_space", re.compile(r"\b(space|spacecraft|satellite|comet|asteroid|rocket|aircraft|plane crash)\b", re.IGNORECASE)),
    ("politics_election", re.compile(r"\b(election|vote|referendum|government|president|chancellor|pope|prime minister|resign(?:s|ed|ation)?|independence|independent|recogni[sz](?:e|es|ed)|apolog(?:y|ise|ises|ized|ize))\b", re.IGNORECASE)),
    ("economy", re.compile(r"\b(economy|economic|inflation|recession|market crash|collapse)\b", re.IGNORECASE)),
    ("climate_environment", re.compile(r"\b(climate|warming|environment|overpopulation|pollution|resource shortage)\b", re.IGNORECASE)),
    ("science_technology", re.compile(r"\b(genetic|genetics|dna|gene|technology|artificial intelligence|robot)\b", re.IGNORECASE)),
]

TREND_WORDS = re.compile(r"\b(increase|decrease|rise|fall|grow|decline|deteriorat|spread|expand)\w*\b", re.IGNORECASE)
STATE_CHANGE_WORDS = re.compile(
    r"\b(collapse|unification|regulation|restriction|ban|shortage|chaos|awakening|change|reform)\b",
    re.IGNORECASE,
)


@dataclass
class Candidate:
    source_quote: str
    source_start_offset: int
    source_end_offset: int
    future_claim_present: bool
    candidate_class: str
    claim_normalized: str
    event_family_provisional: str | None
    time_text: str | None
    location_text: str | None
    actor_text: str | None
    magnitude_text: str | None
    conditionality: str
    ambiguity_flags: list[str]
    extractor_confidence: float
    extractor_meta: dict[str, object]
    bundle_role: str
    bundle_source_quote: str | None
    bundle_source_start_offset: int | None
    bundle_source_end_offset: int | None
    bundle_component_seq: int | None
    bundle_component_count: int | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 1 prediction candidate extractor for Billy Meier contact reports.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument("--run-key", help="Unique run key. Defaults to an auto-generated timestamped key.")
    parser.add_argument("--notes", default="", help="Free-form run notes.")
    parser.add_argument("--report-min", type=int, help="Minimum report_number to include.")
    parser.add_argument("--report-max", type=int, help="Maximum report_number to include.")
    parser.add_argument("--limit", type=int, help="Limit number of reports processed after filters.")
    parser.add_argument("--batch-size", type=int, default=200, help="Bulk insert batch size.")
    parser.add_argument("--dry-run", action="store_true", help="Parse without writing a run or prediction rows.")
    return parser.parse_args()


def generate_run_key() -> str:
    return "stage1-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_report_query(args: argparse.Namespace) -> tuple[str, list[object]]:
    clauses = ["english_content IS NOT NULL", "english_content <> ''"]
    params: list[object] = []
    if args.report_min is not None:
        clauses.append("report_number >= %s")
        params.append(args.report_min)
    if args.report_max is not None:
        clauses.append("report_number <= %s")
        params.append(args.report_max)
    sql = f"""
        SELECT id, report_number, report_date, english_content
        FROM public.contact_reports
        WHERE {' AND '.join(clauses)}
        ORDER BY report_number, id
    """
    if args.limit is not None:
        sql += " LIMIT %s"
        params.append(args.limit)
    return sql, params


def should_skip_line(line: str) -> bool:
    if not line.strip():
        return True
    return any(pattern.search(line) for pattern in SKIP_LINE_PATTERNS)


def normalize_claim_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip(" -:;,.")


def provisional_event_family(text: str) -> str | None:
    for family, pattern in EVENT_FAMILY_PATTERNS:
        if pattern.search(text):
            return family
    return None


def looks_like_world_prediction(text: str) -> bool:
    if text.endswith("?"):
        return False

    family = provisional_event_family(text)
    has_time = TIME_PATTERN.search(text) is not None
    has_location = LOCATION_PATTERN.search(text) is not None
    has_scope = WORLD_SCOPE_PATTERN.search(text) is not None
    has_state_signal = TREND_WORDS.search(text) is not None or STATE_CHANGE_WORDS.search(text) is not None

    if PRIVATE_FUTURE_PREFIX_PATTERN.search(text) and PRIVATE_ACTION_VERB_PATTERN.search(text) and not (family or has_scope):
        return False
    if META_CONVERSATION_PATTERN.search(text) and not (family or has_scope or has_state_signal):
        return False
    if len(text.split()) < 6 and not (family or has_time):
        return False
    return bool(family or has_time or has_location or has_scope or has_state_signal)


def has_predictive_signal(text: str) -> bool:
    normalized = normalize_claim_text(text)
    if FUTURE_MARKER_PATTERN.search(normalized):
        return True
    if DECLARATIVE_TIME_PATTERN.search(normalized) and (
        has_event_anchor(normalized) or DECLARATIVE_EVENT_PATTERN.search(normalized) or STATE_CHANGE_WORDS.search(normalized)
    ):
        return True
    if DECLARATIVE_PREDICTIVE_PREFIX_PATTERN.search(normalized) and (
        has_event_anchor(normalized) or DECLARATIVE_EVENT_PATTERN.search(normalized) or STATE_CHANGE_WORDS.search(normalized)
    ):
        return True
    return False


def is_atomic_prediction_text(text: str, inherited_future: bool = False) -> bool:
    normalized = normalize_claim_text(text)
    if not normalized:
        return False
    if has_predictive_signal(normalized):
        return looks_like_world_prediction(normalized)
    if not inherited_future:
        return False
    family = provisional_event_family(normalized)
    has_time = TIME_PATTERN.search(normalized) is not None
    has_location = LOCATION_PATTERN.search(normalized) is not None
    has_scope = WORLD_SCOPE_PATTERN.search(normalized) is not None
    has_event = EVENTISH_PATTERN.search(normalized) is not None
    return bool(family or has_time or has_location or has_scope or has_event)


def has_event_anchor(text: str) -> bool:
    normalized = normalize_claim_text(text)
    return bool(
        provisional_event_family(normalized)
        or EVENTISH_PATTERN.search(normalized)
        or re.search(r"\bmagnitude\s+\d", normalized, re.IGNORECASE)
    )


def is_compound_rhs_candidate(text: str) -> bool:
    normalized = normalize_claim_text(text)
    if has_predictive_signal(normalized):
        return has_event_anchor(normalized)
    return bool(
        has_event_anchor(normalized)
        and re.match(
            r"""
            ^(
                namely\s+on\s+the|
                on\s+the|
                in\s+the|
                at\s+the\s+same|
                there\s+will|
                another\b|
                a\s+new\b|
                an?\s+(?:earthquake|seaquake|quake|volcano|eruption|storm|hurricane|epidemic|pandemic|attack)
            )
            """,
            normalized,
            re.IGNORECASE | re.VERBOSE,
        )
    )


def split_compound_claim(segment: str) -> list[tuple[str, bool]]:
    parts: list[tuple[str, bool]] = [(segment, False)]
    changed = True
    while changed:
        changed = False
        next_parts: list[tuple[str, bool]] = []
        for text, inherited_future in parts:
            applied = False
            for pattern in COMPOUND_SPLITTER_PATTERNS:
                split = pattern.split(text, maxsplit=1)
                if len(split) != 2:
                    continue
                left = normalize_claim_text(split[0])
                right = normalize_claim_text(split[1])
                if not left or not right:
                    continue
                if not has_event_anchor(left) or not has_event_anchor(right):
                    continue
                if not is_compound_rhs_candidate(right):
                    continue
                if not is_atomic_prediction_text(left, inherited_future):
                    continue
                if not is_atomic_prediction_text(right, True):
                    continue
                next_parts.append((left, inherited_future))
                next_parts.append((right, True))
                applied = True
                changed = True
                break
            if not applied:
                next_parts.append((text, inherited_future))
        parts = next_parts
    return parts


def split_line_into_claims(line: str) -> list[str]:
    cleaned = LINE_PREFIX_PATTERN.sub("", line.strip())
    if not has_predictive_signal(cleaned):
        return []

    parts = [cleaned]
    splitter = re.compile(r"\s+(?:and|but)\s+(?=[^,;:.!?]{0,120}\b(?:will|shall|is going to|are going to)\b)", re.IGNORECASE)
    changed = True
    while changed:
        changed = False
        next_parts: list[str] = []
        for part in parts:
            split = splitter.split(part, maxsplit=1)
            if len(split) != 2:
                next_parts.append(part)
                continue
            left = normalize_claim_text(split[0])
            right = normalize_claim_text(split[1])
            if left and right and is_atomic_prediction_text(left) and is_atomic_prediction_text(right):
                next_parts.extend([left, right])
                changed = True
                continue
            next_parts.append(part)
        parts = next_parts

    claims: list[str] = []
    for part in parts:
        for subpart in SPLIT_PATTERN.split(part):
            text = normalize_claim_text(subpart)
            if text and has_predictive_signal(text) and looks_like_world_prediction(text):
                claims.append(text)
    return claims


def extract_time_text(text: str) -> str | None:
    match = TIME_PATTERN.search(text)
    return normalize_claim_text(match.group(1)) if match else None


def extract_location_text(text: str) -> str | None:
    match = LOCATION_PATTERN.search(text)
    return normalize_claim_text(match.group(1)) if match else None


def extract_actor_text(text: str) -> str | None:
    match = ACTOR_PATTERN.search(text)
    if match:
        actor = normalize_claim_text(match.group(1))
        if actor.split()[0].lower() not in {
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        }:
            return actor
    match = DECLARATIVE_ACTOR_PATTERN.search(text)
    return normalize_claim_text(match.group(1)) if match else None


def extract_magnitude_text(text: str) -> str | None:
    match = re.search(
        r"\b(magnitude\s+of\s+about\s+\d+(?:\.\d+)?|magnitude\s+\d+(?:\.\d+)?|severe|strong|devastating|gigantic|great)\b",
        text,
        re.IGNORECASE,
    )
    return normalize_claim_text(match.group(1)) if match else None


def determine_candidate_class(text: str, conditionality: str, family: str | None) -> str:
    if conditionality != "none":
        return "conditional_future_claim"
    if family in {"earthquake", "volcano", "storm", "war_conflict", "epidemic", "aviation_space", "politics_election"}:
        return "discrete_event"
    if TREND_WORDS.search(text):
        return "trend_claim"
    if STATE_CHANGE_WORDS.search(text):
        return "state_change"
    return "ambiguous_future_claim"


def build_ambiguity_flags(text: str, time_text: str | None, location_text: str | None, actor_text: str | None, conditionality: str) -> list[str]:
    flags: list[str] = []
    if time_text is None:
        flags.append("vague_time")
    if location_text is None:
        flags.append("vague_location")
    if actor_text is None:
        flags.append("vague_actor")
    if conditionality != "none":
        flags.append("conditional")
    if HEDGE_PATTERN.search(text):
        flags.append("hedged_language")
    if len(text.split()) < 6:
        flags.append("short_claim")
    return flags


def estimate_confidence(text: str, family: str | None, time_text: str | None, location_text: str | None) -> float:
    score = 0.55
    if has_predictive_signal(text):
        score += 0.12
    if family:
        score += 0.08
    if time_text:
        score += 0.08
    if location_text:
        score += 0.07
    if HEDGE_PATTERN.search(text):
        score -= 0.08
    if len(text.split()) < 6:
        score -= 0.05
    return round(max(0.05, min(score, 0.98)), 4)


def extract_candidates_from_report(text: str) -> list[Candidate]:
    candidates: list[Candidate] = []
    seen_offsets: set[tuple[int, int]] = set()

    for line_match in re.finditer(r"[^\n]+", text):
        original_line = line_match.group(0)
        if should_skip_line(original_line):
            continue

        line_start = line_match.start()
        line_clean = LINE_PREFIX_PATTERN.sub("", original_line)
        line_prefix_len = len(original_line) - len(line_clean)
        segments = split_line_into_claims(original_line)
        search_pos = 0

        for segment in segments:
            bundle_source_quote = None
            bundle_source_start = None
            bundle_source_end = None
            component_group = split_compound_claim(segment)
            if len(component_group) > 1:
                bundle_relative_start = line_clean.lower().find(segment.lower(), search_pos)
                if bundle_relative_start < 0:
                    bundle_relative_start = line_clean.lower().find(segment.lower())
                if bundle_relative_start >= 0:
                    bundle_source_start = line_start + line_prefix_len + bundle_relative_start
                    bundle_source_end = bundle_source_start + len(segment)
                    bundle_source_quote = segment

            if len(component_group) == 1:
                iterable = [(segment, False, 1, 1)]
            else:
                iterable = [
                    (component_text, inherited_future, component_index, len(component_group))
                    for component_index, (component_text, inherited_future) in enumerate(component_group, start=1)
                ]

            for component_text, inherited_future, component_index, component_count in iterable:
                relative_start = line_clean.lower().find(component_text.lower(), search_pos)
                if relative_start < 0:
                    relative_start = line_clean.lower().find(component_text.lower())
                if relative_start < 0 and bundle_source_start is not None:
                    relative_start = max(0, bundle_source_start - line_start - line_prefix_len)
                if relative_start < 0:
                    continue

                search_pos = relative_start + len(component_text)
                source_start = line_start + line_prefix_len + relative_start
                source_end = source_start + len(component_text)
                offset_key = (source_start, source_end)
                if offset_key in seen_offsets:
                    continue
                seen_offsets.add(offset_key)

                conditionality = "if_then" if re.search(r"\b(if|unless|provided that|in case)\b", component_text, re.IGNORECASE) else "none"
                family = provisional_event_family(component_text)
                time_text = extract_time_text(component_text)
                location_text = extract_location_text(component_text)
                actor_text = extract_actor_text(component_text)
                magnitude_text = extract_magnitude_text(component_text)
                candidate_class = determine_candidate_class(component_text, conditionality, family)
                ambiguity_flags = build_ambiguity_flags(component_text, time_text, location_text, actor_text, conditionality)
                if inherited_future:
                    ambiguity_flags.append("inherited_future")
                confidence = estimate_confidence(component_text, family, time_text, location_text)

                candidates.append(
                    Candidate(
                        source_quote=component_text,
                        source_start_offset=source_start,
                        source_end_offset=source_end,
                        future_claim_present=True,
                        candidate_class=candidate_class,
                        claim_normalized=component_text if component_text.endswith(".") else component_text + ".",
                        event_family_provisional=family,
                        time_text=time_text,
                        location_text=location_text,
                        actor_text=actor_text,
                        magnitude_text=magnitude_text,
                        conditionality=conditionality,
                        ambiguity_flags=sorted(set(ambiguity_flags)),
                        extractor_confidence=confidence,
                        extractor_meta={
                            "parser_version": PARSER_VERSION,
                            "future_marker_found": bool(has_predictive_signal(component_text)) or inherited_future,
                            "inherited_future_marker": inherited_future,
                            "line_fragment": normalize_claim_text(original_line)[:240],
                        },
                        bundle_role="compound_child" if component_count > 1 else "standalone",
                        bundle_source_quote=bundle_source_quote,
                        bundle_source_start_offset=bundle_source_start,
                        bundle_source_end_offset=bundle_source_end,
                        bundle_component_seq=component_index if component_count > 1 else None,
                        bundle_component_count=component_count if component_count > 1 else None,
                    )
                )

    return candidates


def insert_run(cur, run_key: str, args: argparse.Namespace, source_filter: dict[str, object]) -> int:
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
        VALUES (%s, 'stage1_candidate_extraction', 'running', %s, %s, %s, %s, %s, %s, now())
        RETURNING id
        """,
        (
            run_key,
            PARSER_VERSION,
            PROMPT_VERSION,
            SOURCE_CORPUS,
            Json(source_filter),
            args.notes or None,
            Json({"mode": "stage1_regex_extraction"}),
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


def build_stage1_rows(run_id: int, report_row: tuple[object, ...], candidates: Iterable[Candidate]) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]]]:
    report_id, report_number, report_date, english_content = report_row
    source_hash = hashlib.sha256(english_content.encode("utf-8")).hexdigest()
    prediction_rows: list[tuple[object, ...]] = []
    bundle_rows: list[tuple[object, ...]] = []
    bundle_defs: dict[str, dict[str, object]] = {}

    for idx, candidate in enumerate(candidates, start=1):
        bundle_key = None
        if candidate.bundle_role == "compound_child":
            bundle_key = f"run:{run_id}:report:{report_id}:bundle:{candidate.bundle_source_start_offset}:{candidate.bundle_source_end_offset}"
            if bundle_key not in bundle_defs:
                bundle_defs[bundle_key] = {
                    "bundle_seq": len(bundle_defs) + 1,
                    "source_quote": candidate.bundle_source_quote,
                    "source_start_offset": candidate.bundle_source_start_offset,
                    "source_end_offset": candidate.bundle_source_end_offset,
                    "component_count": candidate.bundle_component_count,
                    "event_families": set(),
                }
            if candidate.event_family_provisional:
                bundle_defs[bundle_key]["event_families"].add(candidate.event_family_provisional)

        prediction_rows.append(
            (
                run_id,
                report_id,
                report_number,
                report_date,
                None,
                None,
                "english",
                source_hash,
                idx,
                bundle_key,
                candidate.bundle_component_seq,
                candidate.bundle_component_count,
                candidate.bundle_role,
                candidate.source_quote,
                candidate.source_start_offset,
                candidate.source_end_offset,
                candidate.future_claim_present,
                candidate.candidate_class,
                candidate.claim_normalized,
                candidate.event_family_provisional,
                candidate.time_text,
                candidate.location_text,
                candidate.actor_text,
                candidate.magnitude_text,
                candidate.conditionality,
                Json(candidate.ambiguity_flags),
                candidate.extractor_confidence,
                PARSER_VERSION,
                Json(candidate.extractor_meta),
            )
        )

    for bundle_key, payload in bundle_defs.items():
        families = sorted(payload["event_families"])
        event_family_hint = families[0] if len(families) == 1 else "mixed"
        bundle_rows.append(
            (
                run_id,
                report_id,
                report_number,
                report_date,
                bundle_key,
                payload["bundle_seq"],
                "compound_multi_event",
                payload["source_quote"],
                payload["source_start_offset"],
                payload["source_end_offset"],
                payload["component_count"],
                event_family_hint,
                Json({"parser_version": PARSER_VERSION}),
            )
        )

    return prediction_rows, bundle_rows


def main() -> int:
    args = parse_args()
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        print(f"Missing DSN env var: {args.dsn_env}", file=sys.stderr)
        return 2

    run_key = args.run_key or generate_run_key()
    source_filter = {
        "report_min": args.report_min,
        "report_max": args.report_max,
        "limit": args.limit,
        "source_language": "english",
    }

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    report_sql, report_params = build_report_query(args)

    total_reports = 0
    total_candidates = 0
    total_bundles = 0
    run_id: int | None = None

    try:
        with conn.cursor() as cur:
            if not args.dry_run:
                run_id = insert_run(cur, run_key, args, source_filter)
                conn.commit()

        with conn.cursor() as cur:
            cur.execute(report_sql, report_params)
            report_rows = cur.fetchall()

        pending_prediction_rows: list[tuple[object, ...]] = []
        pending_bundle_rows: list[tuple[object, ...]] = []

        for report_row in report_rows:
            total_reports += 1
            candidates = extract_candidates_from_report(report_row[3])
            total_candidates += len(candidates)
            if args.dry_run:
                continue

            prediction_rows, bundle_rows = build_stage1_rows(run_id, report_row, candidates)
            total_bundles += len(bundle_rows)
            pending_prediction_rows.extend(prediction_rows)
            pending_bundle_rows.extend(bundle_rows)

            if len(pending_bundle_rows) >= args.batch_size:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO public.prediction_audit_bundles (
                            parse_run_id,
                            contact_report_id,
                            report_number,
                            claimed_contact_date,
                            bundle_key,
                            bundle_seq,
                            bundle_kind,
                            source_quote,
                            source_start_offset,
                            source_end_offset,
                            component_count,
                            event_family_hint,
                            bundle_meta
                        )
                        VALUES %s
                        """,
                        pending_bundle_rows,
                    )
                conn.commit()
                pending_bundle_rows.clear()

            if len(pending_prediction_rows) >= args.batch_size:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO public.prediction_audit_predictions (
                            parse_run_id,
                            contact_report_id,
                            report_number,
                            claimed_contact_date,
                            earliest_provable_public_date,
                            public_date_basis,
                            source_language,
                            source_text_hash,
                            candidate_seq,
                            bundle_key,
                            bundle_component_seq,
                            bundle_component_count,
                            bundle_role,
                            source_quote,
                            source_start_offset,
                            source_end_offset,
                            future_claim_present,
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
                            extractor_model,
                            extractor_meta
                        )
                        VALUES %s
                        """,
                        pending_prediction_rows,
                    )
                conn.commit()
                pending_prediction_rows.clear()

        if pending_bundle_rows:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO public.prediction_audit_bundles (
                        parse_run_id,
                        contact_report_id,
                        report_number,
                        claimed_contact_date,
                        bundle_key,
                        bundle_seq,
                        bundle_kind,
                        source_quote,
                        source_start_offset,
                        source_end_offset,
                        component_count,
                        event_family_hint,
                        bundle_meta
                    )
                    VALUES %s
                    """,
                    pending_bundle_rows,
                )
            conn.commit()

        if pending_prediction_rows:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO public.prediction_audit_predictions (
                        parse_run_id,
                        contact_report_id,
                        report_number,
                        claimed_contact_date,
                        earliest_provable_public_date,
                        public_date_basis,
                        source_language,
                        source_text_hash,
                        candidate_seq,
                        bundle_key,
                        bundle_component_seq,
                        bundle_component_count,
                        bundle_role,
                        source_quote,
                        source_start_offset,
                        source_end_offset,
                        future_claim_present,
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
                        extractor_model,
                        extractor_meta
                    )
                    VALUES %s
                    """,
                    pending_prediction_rows,
                )
            conn.commit()

        if not args.dry_run and run_id is not None:
            with conn.cursor() as cur:
                update_run(
                    cur,
                    run_id,
                    "completed",
                    {
                        "total_reports": total_reports,
                        "total_candidates": total_candidates,
                        "total_bundles": total_bundles,
                        "parser_version": PARSER_VERSION,
                    },
                )
            conn.commit()

        print(
            json.dumps(
                {
                    "run_key": run_key,
                    "dry_run": args.dry_run,
                    "total_reports": total_reports,
                    "total_candidates": total_candidates,
                    "total_bundles": total_bundles,
                    "parser_version": PARSER_VERSION,
                },
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        conn.rollback()
        if not args.dry_run and run_id is not None:
            with conn.cursor() as cur:
                update_run(cur, run_id, "failed", {"error": str(exc)[:1000], "parser_version": PARSER_VERSION})
            conn.commit()
        print(f"Parser failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

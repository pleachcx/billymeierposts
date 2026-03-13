#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCRIPT_VERSION = "prediction_audit_release_report_v1"
OUTPUT_ROOT = Path("data") / "exports" / "release"

BRANCH_EXPORT_SPECS = [
    {
        "name": "overview",
        "script": Path("scripts") / "export_prediction_audit_overview.py",
        "root": Path("data") / "exports" / "overview",
        "dir_prefix": "prediction-audit-overview-",
    },
    {
        "name": "publication_timing",
        "script": Path("scripts") / "export_publication_timing_audit.py",
        "root": Path("data") / "exports" / "provenance",
        "dir_prefix": "publication-timing-audit-",
    },
    {
        "name": "cohort_comparison",
        "script": Path("scripts") / "export_cohort_comparison.py",
        "root": Path("data") / "exports" / "provenance",
        "dir_prefix": "cohort-comparison-",
    },
    {
        "name": "research_queue",
        "script": Path("scripts") / "export_public_date_research_queue.py",
        "root": Path("data") / "exports" / "provenance",
        "dir_prefix": "public-date-research-queue-",
    },
    {
        "name": "unscored_queue",
        "script": Path("scripts") / "export_unscored_prediction_queue.py",
        "root": Path("data") / "exports" / "unscored",
        "dir_prefix": "unscored-prediction-queue-",
    },
]

FAMILY_EXPORT_SPECS = [
    {"family": "aviation_space", "root": Path("data") / "exports" / "aviation_space"},
    {"family": "earthquake", "root": Path("data") / "exports" / "earthquake"},
    {"family": "epidemic", "root": Path("data") / "exports" / "epidemic"},
    {"family": "politics_election", "root": Path("data") / "exports" / "politics"},
    {"family": "storm", "root": Path("data") / "exports" / "storm"},
    {"family": "volcano", "root": Path("data") / "exports" / "volcano"},
    {"family": "war_conflict", "root": Path("data") / "exports" / "war_conflict"},
]

MATCH_STATUS_ORDER = {
    "exact_hit": 0,
    "near_hit": 1,
    "similar_only": 2,
    "miss": 3,
}

PUBLIC_DATE_COHORT_LABELS = {
    "included_in_current_public_date_cohort": "public-date clean",
    "excluded_currently_unrescued": "publication conflict",
    "pending_more_public_evidence": "pending evidence",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh aligned export surfaces and build a release-grade prediction-audit summary bundle."
    )
    parser.add_argument("--stage2-run-key", help="Stage 2 run key to scope the release bundle.")
    parser.add_argument("--dsn-env", default="DatabaseURL", help="Environment variable containing the PostgreSQL DSN.")
    parser.add_argument(
        "--dotenv-path",
        default=".env",
        help="Optional dotenv file to load when the DSN env var is not already set. Defaults to .env.",
    )
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Reuse the latest aligned export surfaces instead of rerunning the branch-level exporters first.",
    )
    parser.add_argument(
        "--top-exact-limit",
        type=int,
        default=15,
        help="How many top exact-hit rows to include in the release bundle.",
    )
    parser.add_argument(
        "--conflict-limit",
        type=int,
        default=15,
        help="How many top publication-conflict rows to include in the release bundle.",
    )
    parser.add_argument(
        "--unscored-limit",
        type=int,
        default=15,
        help="How many top unscored queue rows to include in the release bundle.",
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory. Defaults to data/exports/release/prediction-audit-release-<timestamp>.",
    )
    return parser.parse_args()


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if value and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ[key] = value


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: csv_safe(row.get(field)) for field in fieldnames})


def csv_safe(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return value


def parse_generated_at(value: str | None, fallback_path: Path) -> tuple[str, str]:
    if value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return (parsed.astimezone(timezone.utc).isoformat(), str(fallback_path))
        except ValueError:
            pass
    return ("", str(fallback_path))


def discover_export(
    root: Path,
    stage2_run_key: str,
    *,
    family: str | None = None,
    dir_prefix: str | None = None,
) -> dict[str, Any]:
    candidates: list[tuple[tuple[str, str], Path, dict[str, Any]]] = []
    for summary_path in root.glob("*/summary.json"):
        if dir_prefix and not summary_path.parent.name.startswith(dir_prefix):
            continue
        summary = load_json(summary_path)
        if family is None:
            summary_stage2 = summary.get("stage2_run_key")
        else:
            summary_stage2 = (summary.get("run_keys") or {}).get("stage2_run_key")
        if summary_stage2 != stage2_run_key:
            continue
        candidates.append((parse_generated_at(summary.get("generated_at"), summary_path.parent), summary_path, summary))

    if not candidates:
        label = family or root.name
        raise RuntimeError(f"No aligned export summary found for {label} on Stage 2 run {stage2_run_key}.")

    _, summary_path, summary = sorted(candidates)[-1]
    return {
        "summary_path": str(summary_path),
        "output_dir": str(summary_path.parent),
        "summary": summary,
    }


def parse_export_stdout(stdout: str) -> dict[str, Any]:
    text = stdout.strip()
    if not text:
        raise RuntimeError("Expected JSON output from export script, but stdout was empty.")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse exporter JSON output: {exc}") from exc


def run_export(spec: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    command = [
        sys.executable,
        str(spec["script"]),
        "--stage2-run-key",
        args.stage2_run_key,
        "--dsn-env",
        args.dsn_env,
    ]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{spec['script']} failed with exit code {completed.returncode}.\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )

    payload = parse_export_stdout(completed.stdout)
    summary_path = payload.get("summary_path")
    output_dir = payload.get("output_dir")
    if summary_path and not output_dir:
        output_dir = str(Path(summary_path).parent)
    if not summary_path or not output_dir:
        raise RuntimeError(f"{spec['script']} did not report summary_path/output_dir in its JSON payload.")

    return {
        "summary_path": str(summary_path),
        "output_dir": str(output_dir),
        "summary": load_json(Path(summary_path)),
    }


def normalize_key(report_number: Any, candidate_seq: Any) -> str:
    return f"{str(report_number).strip()}:{str(candidate_seq).strip()}"


def parse_int(value: Any) -> int:
    if value in (None, "", "null"):
        return 0
    return int(value)


def parse_float(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    return float(value)


def cohort_snapshot(name: str, cohort_summary: dict[str, Any]) -> dict[str, Any]:
    match_counts = cohort_summary.get("match_status_counts") or {}
    exact_hit_count = parse_int(match_counts.get("exact_hit"))
    near_hit_count = parse_int(match_counts.get("near_hit"))
    similar_only_count = parse_int(match_counts.get("similar_only"))
    miss_count = parse_int(match_counts.get("miss"))
    return {
        "cohort_name": name,
        "prediction_count": parse_int(cohort_summary.get("prediction_count")),
        "hit_count": exact_hit_count + near_hit_count + similar_only_count,
        "exact_hit_count": exact_hit_count,
        "near_hit_count": near_hit_count,
        "similar_only_count": similar_only_count,
        "miss_count": miss_count,
        "combined_observed_probability": cohort_summary.get("combined_observed_probability") or {},
        "family_counts": cohort_summary.get("family_counts") or {},
        "public_date_status_counts": cohort_summary.get("public_date_status_counts") or {},
    }


def format_probability(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.6g}"


def format_cohort_label(value: str | None) -> str:
    if not value:
        return "unknown"
    return PUBLIC_DATE_COHORT_LABELS.get(value, value.replace("_", " "))


def markdown_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    if not rows:
        return "_None._"
    header = "| " + " | ".join(label for _, label in columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body = [
        "| " + " | ".join(str(row.get(key, "")) for key, _ in columns) + " |"
        for row in rows
    ]
    return "\n".join([header, separator, *body])


def family_sort_key(path_text: str) -> tuple[str, str]:
    return ("", path_text)


def build_release_markdown(
    *,
    generated_at: str,
    stage2_run_key: str,
    parse_counts: dict[str, Any],
    release_counts: dict[str, Any],
    family_rows: list[dict[str, Any]],
    top_exact_hits: list[dict[str, Any]],
    conflict_rows: list[dict[str, Any]],
    unscored_rows: list[dict[str, Any]],
    source_exports: dict[str, Any],
) -> str:
    claimed = release_counts["claimed_date_baseline"]
    clean = release_counts["public_date_clean"]
    pending = release_counts["pending_more_public_evidence"]
    conflicts = release_counts["public_date_currently_unrescued"]

    source_lines = [
        f"- `overview`: `{source_exports['overview']['output_dir']}`",
        f"- `publication_timing`: `{source_exports['publication_timing']['output_dir']}`",
        f"- `cohort_comparison`: `{source_exports['cohort_comparison']['output_dir']}`",
        f"- `research_queue`: `{source_exports['research_queue']['output_dir']}`",
        f"- `unscored_queue`: `{source_exports['unscored_queue']['output_dir']}`",
    ]
    for family_name in sorted(source_exports["family_exports"]):
        source_lines.append(f"- `family/{family_name}`: `{source_exports['family_exports'][family_name]['output_dir']}`")

    family_table_rows = [
        {
            "family": row["event_family_final"],
            "claimed_scored": row["claimed_scored_count"],
            "claimed_exact": row["claimed_exact_hit_count"],
            "public_clean": row["public_date_clean_count"],
            "clean_exact": row["public_date_clean_exact_hit_count"],
            "pending": row["pending_more_public_evidence_count"],
            "conflicts": row["public_date_currently_unrescued_count"],
        }
        for row in family_rows
    ]
    top_exact_table_rows = [
        {
            "report_candidate": row["report_candidate"],
            "family": row["event_family_final"],
            "cohort": row["public_date_cohort_label"],
            "p_obs": row["observed_probability_under_null"],
            "event": row["event_title"],
        }
        for row in top_exact_hits
    ]
    conflict_table_rows = [
        {
            "rank": row["priority_rank"],
            "report_candidate": row["report_candidate"],
            "family": row["event_family_final"],
            "surprisal": row["surprisal_log10"],
            "gap": row["publication_conflict_gap_bucket"],
            "event": row["event_title"],
        }
        for row in conflict_rows
    ]
    unscored_table_rows = [
        {
            "rank": row["priority_rank"],
            "report_candidate": row["report_candidate"],
            "family": row["family_guess"],
            "stage2": row["stage2_label"],
            "bucket": row["recovery_bucket"],
        }
        for row in unscored_rows
    ]

    return "\n".join(
        [
            "# Prediction Audit Release Snapshot",
            "",
            f"- Generated at: `{generated_at}`",
            f"- Stage 2 baseline: `{stage2_run_key}`",
            f"- Script version: `{SCRIPT_VERSION}`",
            "",
            "## Claimed-Date Baseline",
            "",
            f"- Candidate predictions parsed: `{parse_counts['candidate_count']}`",
            f"- Eligible predictions: `{parse_counts['eligible_count']}`",
            f"- Significant predictions: `{parse_counts['significant_count']}`",
            f"- Included scored rows: `{claimed['prediction_count']}`",
            f"- Hits: `{claimed['hit_count']}` (`{claimed['exact_hit_count']}` exact, `{claimed['near_hit_count']}` near, `{claimed['similar_only_count']}` similar-only)",
            f"- Misses: `{claimed['miss_count']}`",
            f"- Combined observed log10 probability sum: `{claimed['combined_observed_probability'].get('log10_sum')}`",
            "",
            "## Public-Date Separation",
            "",
            markdown_table(
                [
                    {
                        "cohort": "public-date clean",
                        "rows": clean["prediction_count"],
                        "hits": clean["hit_count"],
                        "exact": clean["exact_hit_count"],
                        "misses": clean["miss_count"],
                        "log10_sum": clean["combined_observed_probability"].get("log10_sum"),
                    },
                    {
                        "cohort": "pending evidence",
                        "rows": pending["prediction_count"],
                        "hits": pending["hit_count"],
                        "exact": pending["exact_hit_count"],
                        "misses": pending["miss_count"],
                        "log10_sum": pending["combined_observed_probability"].get("log10_sum"),
                    },
                    {
                        "cohort": "publication conflicts",
                        "rows": conflicts["prediction_count"],
                        "hits": conflicts["hit_count"],
                        "exact": conflicts["exact_hit_count"],
                        "misses": conflicts["miss_count"],
                        "log10_sum": conflicts["combined_observed_probability"].get("log10_sum"),
                    },
                ],
                [
                    ("cohort", "Cohort"),
                    ("rows", "Rows"),
                    ("hits", "Hits"),
                    ("exact", "Exact"),
                    ("misses", "Misses"),
                    ("log10_sum", "log10(sum p_obs)"),
                ],
            ),
            "",
            "## Family Summary",
            "",
            markdown_table(
                family_table_rows,
                [
                    ("family", "Family"),
                    ("claimed_scored", "Claimed Scored"),
                    ("claimed_exact", "Claimed Exact"),
                    ("public_clean", "Public Clean"),
                    ("clean_exact", "Clean Exact"),
                    ("pending", "Pending"),
                    ("conflicts", "Conflicts"),
                ],
            ),
            "",
            "## Top Exact Hits",
            "",
            "_Ranked by observed probability under each family's current null model. These family-specific nulls are not directly interchangeable._",
            "",
            markdown_table(
                top_exact_table_rows,
                [
                    ("report_candidate", "Report/Candidate"),
                    ("family", "Family"),
                    ("cohort", "Public-Date Status"),
                    ("p_obs", "p_obs"),
                    ("event", "Observed Event"),
                ],
            ),
            "",
            "## Top Publication-Conflict Rows",
            "",
            markdown_table(
                conflict_table_rows,
                [
                    ("rank", "Rank"),
                    ("report_candidate", "Report/Candidate"),
                    ("family", "Family"),
                    ("surprisal", "Surprisal"),
                    ("gap", "Gap Bucket"),
                    ("event", "Observed Event"),
                ],
            ),
            "",
            "## Open Queues",
            "",
            f"- Research queue rows: `{conflicts['prediction_count']}`. Current aligned export: `{source_exports['research_queue']['output_dir']}`",
            f"- Unscored queue rows: `{source_exports['unscored_queue']['summary']['queue_summary']['prediction_count']}`. Current aligned export: `{source_exports['unscored_queue']['output_dir']}`",
            "",
            markdown_table(
                unscored_table_rows,
                [
                    ("rank", "Rank"),
                    ("report_candidate", "Report/Candidate"),
                    ("family", "Family"),
                    ("stage2", "Stage 2 Label"),
                    ("bucket", "Recovery Bucket"),
                ],
            ),
            "",
            "## Source Exports",
            "",
            *source_lines,
            "",
        ]
    )


def main() -> int:
    args = parse_args()
    dotenv_path = Path(args.dotenv_path)
    if args.dsn_env not in os.environ and dotenv_path.exists():
        load_dotenv(dotenv_path)

    if not args.skip_refresh and args.dsn_env not in os.environ:
        print(
            f"Missing DSN env var `{args.dsn_env}` and could not load it from `{dotenv_path}`.",
            file=sys.stderr,
        )
        return 2

    branch_exports: dict[str, Any] = {}
    for spec in BRANCH_EXPORT_SPECS:
        if args.skip_refresh:
            branch_exports[spec["name"]] = discover_export(
                spec["root"],
                args.stage2_run_key,
                dir_prefix=spec.get("dir_prefix"),
            )
        else:
            branch_exports[spec["name"]] = run_export(spec, args)

    family_exports: dict[str, Any] = {}
    for spec in FAMILY_EXPORT_SPECS:
        family_exports[spec["family"]] = discover_export(spec["root"], args.stage2_run_key, family=spec["family"])

    overview_summary = branch_exports["overview"]["summary"]
    timing_summary = branch_exports["publication_timing"]["summary"]
    cohort_summary = branch_exports["cohort_comparison"]["summary"]
    research_summary = branch_exports["research_queue"]["summary"]
    unscored_summary = branch_exports["unscored_queue"]["summary"]

    overview_family_rows = load_csv(Path(branch_exports["overview"]["output_dir"]) / "family_summary.csv")
    timing_rows = load_csv(Path(branch_exports["publication_timing"]["output_dir"]) / "timing_audit.csv")
    research_rows = load_csv(Path(branch_exports["research_queue"]["output_dir"]) / "research_queue.csv")
    unscored_rows = load_csv(Path(branch_exports["unscored_queue"]["output_dir"]) / "queue.csv")

    probability_index: dict[str, dict[str, Any]] = {}
    for family_name, export_info in family_exports.items():
        predictions_path = Path(export_info["output_dir"]) / "predictions.csv"
        if not predictions_path.exists():
            raise RuntimeError(f"Missing predictions.csv for family export {family_name}: {predictions_path}")
        for row in load_csv(predictions_path):
            key = normalize_key(row.get("report_number"), row.get("candidate_seq"))
            probability_index[key] = {
                "event_family_final": row.get("event_family_final") or family_name,
                "observed_probability_under_null": parse_float(row.get("observed_probability_under_null")),
                "probability_model_version": row.get("probability_model_version"),
                "event_title": row.get("event_title"),
                "event_start_date": row.get("event_start_date"),
                "source_name": row.get("source_name"),
                "source_url": row.get("source_url"),
            }

    enriched_timing_rows: list[dict[str, Any]] = []
    for row in timing_rows:
        key = normalize_key(row.get("report_number"), row.get("candidate_seq"))
        probability_row = probability_index.get(key, {})
        observed_probability = probability_row.get("observed_probability_under_null")
        enriched_timing_rows.append(
            {
                **row,
                "report_candidate": f"{row['report_number']}/{row['candidate_seq']}",
                "event_family_final": row["event_family_final"],
                "observed_probability_under_null": observed_probability,
                "observed_probability_label": format_probability(observed_probability),
                "observed_probability_log10": round(math.log10(observed_probability), 6)
                if observed_probability and observed_probability > 0
                else None,
                "probability_model_version": probability_row.get("probability_model_version"),
                "event_title": probability_row.get("event_title") or row.get("event_title") or "",
                "event_start_date": probability_row.get("event_start_date") or row.get("event_start_date") or "",
                "source_name": probability_row.get("source_name") or "",
                "source_url": probability_row.get("source_url") or "",
                "public_date_cohort_label": format_cohort_label(row.get("public_date_cohort_status")),
            }
        )

    top_exact_hits = sorted(
        [
            {
                "report_candidate": row["report_candidate"],
                "event_family_final": row["event_family_final"],
                "public_date_status": row["public_date_status"],
                "public_date_cohort_status": row["public_date_cohort_status"],
                "public_date_cohort_label": row["public_date_cohort_label"],
                "observed_probability_under_null": row["observed_probability_label"],
                "observed_probability_sort": row["observed_probability_under_null"] or float("inf"),
                "event_title": row["event_title"],
                "event_start_date": row["event_start_date"],
                "claim_normalized": row["claim_normalized"],
                "publication_lag_days_vs_event": row.get("publication_lag_days_vs_event"),
                "current_public_source_tier": row.get("current_public_source_tier"),
            }
            for row in enriched_timing_rows
            if row["match_status"] == "exact_hit" and row["observed_probability_under_null"] is not None
        ],
        key=lambda row: (row["observed_probability_sort"], row["event_family_final"], row["report_candidate"]),
    )[: args.top_exact_limit]
    for row in top_exact_hits:
        row.pop("observed_probability_sort", None)

    public_date_clean_rows = sorted(
        [
            {
                "report_candidate": row["report_candidate"],
                "event_family_final": row["event_family_final"],
                "match_status": row["match_status"],
                "observed_probability_under_null": row["observed_probability_label"],
                "event_title": row["event_title"],
                "current_public_source_tier": row.get("current_public_source_tier"),
                "claim_normalized": row["claim_normalized"],
            }
            for row in enriched_timing_rows
            if row["public_date_cohort_status"] == "included_in_current_public_date_cohort"
        ],
        key=lambda row: (
            MATCH_STATUS_ORDER.get(row["match_status"], 9),
            parse_float(row["observed_probability_under_null"]) if row["observed_probability_under_null"] else float("inf"),
            row["event_family_final"],
            row["report_candidate"],
        ),
    )

    conflict_rows = [
        {
            **row,
            "report_candidate": f"{row['report_number']}/{row['candidate_seq']}",
            "surprisal_log10": row["surprisal_log10"],
        }
        for row in research_rows[: args.conflict_limit]
    ]

    top_unscored_rows = [
        {
            **row,
            "report_candidate": f"{row['report_number']}/{row['candidate_seq']}",
        }
        for row in unscored_rows[: args.unscored_limit]
    ]

    overview_family_index = {row["event_family_final"]: row for row in overview_family_rows}
    cohort_by_family = cohort_summary.get("cohorts_by_family") or {}
    public_clean_by_family = cohort_by_family.get("public_date_strict_clean") or {}
    pending_by_family = cohort_by_family.get("public_date_pending_evidence") or {}
    unrescued_by_family = cohort_by_family.get("public_date_currently_unrescued") or {}

    family_rows: list[dict[str, Any]] = []
    for family_name in sorted(overview_summary["family_counts"]):
        overview_row = overview_family_index.get(family_name, {})
        public_clean_summary = public_clean_by_family.get(family_name, {})
        pending_summary = pending_by_family.get(family_name, {})
        unrescued_summary = unrescued_by_family.get(family_name, {})
        unrescued_match_counts = unrescued_summary.get("match_status_counts") or {}

        family_rows.append(
            {
                "event_family_final": family_name,
                "claimed_scored_count": parse_int(overview_row.get("included_scored_count")),
                "claimed_hit_count": parse_int(overview_row.get("claimed_hit_count")),
                "claimed_exact_hit_count": parse_int(overview_row.get("claimed_exact_hit_count")),
                "public_date_clean_count": parse_int(overview_row.get("public_date_clean_count")),
                "public_date_clean_exact_hit_count": parse_int(overview_row.get("public_date_clean_exact_hit_count")),
                "public_date_pending_clean_count": parse_int(public_clean_summary.get("prediction_count")),
                "pending_more_public_evidence_count": parse_int(pending_summary.get("prediction_count")),
                "public_date_currently_unrescued_count": parse_int(unrescued_summary.get("prediction_count")),
                "public_date_currently_unrescued_exact_hit_count": parse_int(unrescued_match_counts.get("exact_hit")),
                "public_date_currently_unrescued_hit_count": parse_int(unrescued_match_counts.get("exact_hit"))
                + parse_int(unrescued_match_counts.get("near_hit"))
                + parse_int(unrescued_match_counts.get("similar_only")),
            }
        )

    release_counts = {
        "claimed_date_baseline": cohort_snapshot(
            "claimed_date_baseline",
            cohort_summary["cohorts"]["claimed_date_baseline"],
        ),
        "public_date_clean": cohort_snapshot(
            "public_date_clean",
            cohort_summary["cohorts"]["public_date_strict_clean"],
        ),
        "pending_more_public_evidence": cohort_snapshot(
            "pending_more_public_evidence",
            cohort_summary["cohorts"]["public_date_pending_evidence"],
        ),
        "public_date_currently_unrescued": cohort_snapshot(
            "public_date_currently_unrescued",
            cohort_summary["cohorts"]["public_date_currently_unrescued"],
        ),
    }

    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else OUTPUT_ROOT / ("prediction-audit-release-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    source_exports = {
        **branch_exports,
        "family_exports": family_exports,
    }

    generated_at = datetime.now(timezone.utc).isoformat()
    summary = {
        "generated_at": generated_at,
        "script_version": SCRIPT_VERSION,
        "stage2_run_key": args.stage2_run_key,
        "refresh_mode": "reuse_existing_exports" if args.skip_refresh else "refresh_branch_exports",
        "parse_counts": overview_summary["parse_counts"],
        "release_counts": release_counts,
        "family_summary": family_rows,
        "top_exact_hits": top_exact_hits,
        "public_date_clean_rows": public_date_clean_rows,
        "top_publication_conflicts": conflict_rows,
        "unscored_queue_summary": unscored_summary["queue_summary"],
        "top_unscored_rows": top_unscored_rows,
        "source_exports": {
            key: {
                "summary_path": value["summary_path"],
                "output_dir": value["output_dir"],
            }
            for key, value in branch_exports.items()
        },
        "family_exports": {
            key: {
                "summary_path": value["summary_path"],
                "output_dir": value["output_dir"],
            }
            for key, value in family_exports.items()
        },
    }

    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_csv(
        output_dir / "family_summary.csv",
        family_rows,
        [
            "event_family_final",
            "claimed_scored_count",
            "claimed_hit_count",
            "claimed_exact_hit_count",
            "public_date_clean_count",
            "public_date_clean_exact_hit_count",
            "pending_more_public_evidence_count",
            "public_date_currently_unrescued_count",
            "public_date_currently_unrescued_hit_count",
            "public_date_currently_unrescued_exact_hit_count",
        ],
    )
    write_csv(
        output_dir / "top_exact_hits.csv",
        top_exact_hits,
        [
            "report_candidate",
            "event_family_final",
            "public_date_status",
            "public_date_cohort_status",
            "public_date_cohort_label",
            "observed_probability_under_null",
            "publication_lag_days_vs_event",
            "event_start_date",
            "event_title",
            "current_public_source_tier",
            "claim_normalized",
        ],
    )
    write_csv(
        output_dir / "public_date_clean_rows.csv",
        public_date_clean_rows,
        [
            "report_candidate",
            "event_family_final",
            "match_status",
            "observed_probability_under_null",
            "event_title",
            "current_public_source_tier",
            "claim_normalized",
        ],
    )
    write_csv(
        output_dir / "top_publication_conflicts.csv",
        conflict_rows,
        [
            "priority_rank",
            "report_candidate",
            "event_family_final",
            "match_status",
            "surprisal_log10",
            "publication_conflict_gap_bucket",
            "publication_lag_days_vs_event",
            "current_public_source_tier",
            "event_start_date",
            "event_title",
            "claim_normalized",
            "current_public_source_url",
        ],
    )
    write_csv(
        output_dir / "top_unscored_queue.csv",
        top_unscored_rows,
        [
            "priority_rank",
            "report_candidate",
            "family_guess",
            "stage2_label",
            "significant",
            "recovery_bucket",
            "recovery_rationale",
            "claim_normalized",
        ],
    )

    markdown = build_release_markdown(
        generated_at=generated_at,
        stage2_run_key=args.stage2_run_key,
        parse_counts=overview_summary["parse_counts"],
        release_counts=release_counts,
        family_rows=family_rows,
        top_exact_hits=top_exact_hits,
        conflict_rows=conflict_rows,
        unscored_rows=top_unscored_rows,
        source_exports=source_exports,
    )
    markdown_path = output_dir / "release_summary.md"
    markdown_path.write_text(markdown + "\n", encoding="utf-8")

    payload = {
        "generated_at": generated_at,
        "output_dir": str(output_dir),
        "summary_path": str(summary_path),
        "markdown_path": str(markdown_path),
        "stage2_run_key": args.stage2_run_key,
        "refresh_mode": summary["refresh_mode"],
        "release_counts": release_counts,
        "source_exports": {
            key: value["output_dir"] for key, value in branch_exports.items()
        },
        "family_exports": {
            key: value["output_dir"] for key, value in family_exports.items()
        },
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

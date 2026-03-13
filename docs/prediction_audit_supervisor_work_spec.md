---
doc_type: supervisor_work_spec_v1
project: billymeierposts
workstream: prediction_audit
branch_name: feat/prediction-audit-outstanding
recommended_doc_spec:
  - docs/prediction_audit_supervisor_work_spec.md
  - billy_meier_staged_protocol.md
baseline_stage2_run_key: stage2-20260313T024045Z
baseline_exports:
  overview: data/exports/overview/prediction-audit-overview-20260313T035032Z/summary.json
  publication_timing: data/exports/provenance/publication-timing-audit-20260313T035031Z/summary.json
  cohort_comparison: data/exports/provenance/cohort-comparison-20260313T035031Z/summary.json
  research_queue: data/exports/provenance/public-date-research-queue-20260313T035032Z/summary.json
  unscored_queue: data/exports/unscored/unscored-prediction-queue-20260313T035033Z/summary.json
  release_bundle: data/exports/release/prediction-audit-release-20260313T040828Z/summary.json
current_state:
  candidate_count: 6797
  eligible_count: 1226
  significant_count: 316
  included_scored_count: 110
  claimed_hit_count: 103
  claimed_exact_hit_count: 77
  public_date_clean_count: 8
  public_date_clean_exact_hit_count: 1
  public_date_pending_more_evidence_count: 19
  public_date_currently_unrescued_count: 83
  family_counts:
    aviation_space: 1
    earthquake: 52
    epidemic: 15
    politics_election: 17
    storm: 6
    volcano: 19
global_assumptions:
  - The prediction audit remains anchored to the fresh parse tables and ignores legacy JSON prediction fields.
  - The current aligned mainline baseline is stage2-20260313T024045Z unless a pack explicitly reruns Stage 1 or Stage 2.
  - Existing earthquake, epidemic, politics, aviation_space, storm, volcano, provenance, and overview scripts remain the primary execution surface.
global_constraints:
  - Do not mutate contact_reports.predictions or accurate_predictions.
  - Use prediction_audit_* tables and existing staged scripts.
  - Keep run keys and exported artifacts aligned to one Stage 2 baseline at a time.
  - When replaying a family, run Stage 7 before Stage 8, then Stage 9, then refresh exports.
  - Reject curated rows whose matched event predates claimed_contact_date.
  - Use only defensible public-date evidence with explicit URLs, dates, and source notes; do not use undated mirrors.
  - Keep family slices narrow and source-backed; retire rows explicitly instead of stretching rulebooks.
  - Keep repo committed before and after each work bundle.
global_validators:
  - python3 -m py_compile scripts/*.py
  - python3 scripts/export_prediction_audit_overview.py --stage2-run-key stage2-20260313T024045Z
definition_of_done:
  - All ready packs are done or explicitly retired.
  - Overview, provenance, and touched family exports are refreshed on one aligned baseline.
  - The public-date research queue is refreshed.
  - Outstanding rescue attempts are documented with explicit evidence or explicit dead-end notes.
  - Repo is clean.
packs:
  - pack_id: P1
    title: Small-gap provenance rescue
    status: ready
    depends_on: []
    objective: Try to flip the small and medium publication-gap exact hits with earlier defensible public evidence.
    acceptance:
      - Each target row is either rescued or documented as a dead end with source notes.
      - Stage 8 and Stage 9 rerun after any evidence import.
      - Publication timing, cohort comparison, research queue, and overview exports are refreshed.
    commit_strategy: One commit per coherent evidence batch, keeping evidence additions and reruns together.
    slices:
      - slice_id: P1.S1
        title: Rescue Libya storm row 863/6
        status: ready
        target_rows: ["863/6"]
        target_scope: storm
        allowed_files:
          - data/report_provenance_manual_evidence.json
          - scripts/apply_report_provenance_evidence.py
          - scripts/seed_futureofmankind_revision_provenance.py
          - scripts/seed_figu_shop_provenance.py
          - scripts/review_publication_timing.py
          - scripts/finalize_public_date_cohort.py
          - scripts/export_publication_timing_audit.py
          - scripts/export_cohort_comparison.py
          - scripts/export_public_date_research_queue.py
          - scripts/export_prediction_audit_overview.py
          - docs/agent_notes.md
        validators:
          - python3 scripts/review_publication_timing.py --stage2-run-key stage2-20260311T093233Z
          - python3 scripts/finalize_public_date_cohort.py --stage2-run-key stage2-20260311T093233Z
          - python3 scripts/export_publication_timing_audit.py --stage2-run-key stage2-20260311T093233Z
        stop_conditions:
          - No earlier dated official or archived source is found.
          - Candidate source lacks an explicit publication date.
          - Evidence conflicts with existing provenance rules.
      - slice_id: P1.S2
        title: Rescue earthquake row 663/15
        status: ready
        target_rows: ["663/15"]
        target_scope: earthquake
        allowed_files:
          - data/report_provenance_manual_evidence.json
          - scripts/apply_report_provenance_evidence.py
          - scripts/seed_futureofmankind_revision_provenance.py
          - scripts/seed_figu_shop_provenance.py
          - docs/agent_notes.md
        validators:
          - python3 scripts/review_publication_timing.py --stage2-run-key stage2-20260311T093233Z
        stop_conditions:
          - No earlier dated official or archived source is found.
          - Only later mirror evidence is available.
      - slice_id: P1.S3
        title: Rescue corona row 729/12
        status: ready
        target_rows: ["729/12"]
        target_scope: epidemic
        allowed_files:
          - data/report_provenance_manual_evidence.json
          - scripts/apply_report_provenance_evidence.py
          - scripts/seed_futureofmankind_revision_provenance.py
          - scripts/seed_figu_shop_provenance.py
          - docs/agent_notes.md
        validators:
          - python3 scripts/review_publication_timing.py --stage2-run-key stage2-20260311T093233Z
        stop_conditions:
          - No earlier dated official or archived source is found.
          - Source depends on undated snippet-only evidence.
      - slice_id: P1.S4
        title: Rescue earthquake row 725/1
        status: ready
        target_rows: ["725/1"]
        target_scope: earthquake
        allowed_files:
          - data/report_provenance_manual_evidence.json
          - scripts/apply_report_provenance_evidence.py
          - scripts/seed_futureofmankind_revision_provenance.py
          - scripts/seed_figu_shop_provenance.py
          - docs/agent_notes.md
        validators:
          - python3 scripts/review_publication_timing.py --stage2-run-key stage2-20260311T093233Z
        stop_conditions:
          - No earlier dated official or archived source is found.
          - Only indirect references exist without dated provenance.
      - slice_id: P1.S5
        title: Refresh public-date exports
        status: ready
        target_scope: provenance
        allowed_files:
          - data/exports/provenance/**
          - data/exports/overview/**
          - docs/agent_notes.md
        validators:
          - python3 scripts/finalize_public_date_cohort.py --stage2-run-key stage2-20260311T093233Z
          - python3 scripts/export_publication_timing_audit.py --stage2-run-key stage2-20260311T093233Z
          - python3 scripts/export_cohort_comparison.py --stage2-run-key stage2-20260311T093233Z
          - python3 scripts/export_public_date_research_queue.py --stage2-run-key stage2-20260311T093233Z
          - python3 scripts/export_prediction_audit_overview.py --stage2-run-key stage2-20260311T093233Z
        stop_conditions:
          - Stop only after all provenance exports align to one Stage 2 baseline.
  - pack_id: P2
    title: Deep-archive provenance triage
    status: ready
    depends_on: [P1]
    objective: Record structured rescue attempts for the highest-surprisal deep-archive conflicts and stop re-trying dead paths blindly.
    acceptance:
      - A structured research log exists for the top deep-archive targets.
      - Each target has source attempts, outcome, and next hypothesis or explicit dead end.
      - If any new evidence is imported, Stage 8 and Stage 9 are rerun and exports refreshed.
    commit_strategy: One commit per coherent research-log batch; separate evidence imports from note-only dead-end recording where practical.
    slices:
      - slice_id: P2.S1
        title: Create structured provenance research log
        status: ready
        target_scope: provenance
        allowed_files:
          - data/report_provenance_research_log.json
          - docs/agent_notes.md
        validators:
          - python3 -c "import json, pathlib; json.loads(pathlib.Path('data/report_provenance_research_log.json').read_text()); print('ok')"
        stop_conditions:
          - Stop when the log schema can hold target rows, attempted sources, tier, outcome, and next hypothesis.
      - slice_id: P2.S2
        title: Triage top high-surprisal deep-archive reports
        status: ready
        target_rows: ["465/6", "446/6", "136/88", "400/7", "395/4", "459/5", "442/3", "246/23", "427/1", "402/5"]
        target_scope: cross_family
        allowed_files:
          - data/report_provenance_research_log.json
          - data/report_provenance_manual_evidence.json
          - docs/agent_notes.md
        validators:
          - python3 -c "import json, pathlib, sys; data = json.loads(pathlib.Path('data/report_provenance_research_log.json').read_text()); targets = {'465/6','446/6','136/88','400/7','395/4','459/5','442/3','246/23','427/1','402/5'}; covered = {row['prediction_key'] for row in data.get('entries', [])}; missing = sorted(targets - covered); print('missing=' + ','.join(missing)); sys.exit(1 if missing else 0)"
        stop_conditions:
          - Stop when every listed row has either a rescue source, a dead end, or a next-hypothesis entry.
  - pack_id: P3
    title: Unscored measurable-row recovery
    status: ready
    depends_on: []
    objective: Surface and recover measurable predictions that are still trapped in Stage 1 or Stage 2 limbo instead of the scored families.
    acceptance:
      - A reusable queue export exists for unscored eligible or significant rows.
      - At least five measurable rows are either promoted into a scored family or explicitly retired with rationale.
      - If Stage 1 or Stage 2 reruns occur, the active baseline and downstream exports are updated deliberately.
    commit_strategy: Split queue tooling, parser review fixes, and family replays into separate commits inside the same pack.
    slices:
      - slice_id: P3.S1
        title: Export unscored measurable queue
        status: ready
        target_scope: parser_stage2_gap
        allowed_files:
          - scripts/export_unscored_prediction_queue.py
          - data/exports/unscored/**
          - docs/agent_notes.md
        validators:
          - python3 scripts/export_unscored_prediction_queue.py --stage2-run-key stage2-20260311T093233Z
        stop_conditions:
          - Stop when the queue includes report number, candidate seq, current stage label, family guess, and recovery rationale.
      - slice_id: P3.S2
        title: Recover report 383 disease rows
        status: ready
        target_scope: epidemic
        target_rows: ["383:marburg_candidate", "383/32_review_context"]
        allowed_files:
          - scripts/parse_contact_report_predictions.py
          - scripts/review_prediction_candidates.py
          - scripts/export_unscored_prediction_queue.py
          - data/epidemic_prediction_overrides.json
          - data/epidemic_official_events.json
          - docs/agent_notes.md
        validators:
          - python3 -m py_compile scripts/parse_contact_report_predictions.py scripts/review_prediction_candidates.py
        stop_conditions:
          - Stop when the Marburg-style row is either extracted and routed or explicitly documented as not recoverable from the current text.
      - slice_id: P3.S3
        title: Recover next measurable rows from queue
        status: ready
        target_scope: cross_family
        allowed_files:
          - scripts/**
          - data/*_prediction_overrides.json
          - data/*_official_events.json
          - data/*_probability_baselines.json
          - docs/agent_notes.md
        validators:
          - python3 scripts/export_prediction_audit_overview.py --stage2-run-key stage2-20260311T093233Z
        stop_conditions:
          - Stop after five queue rows are either scored or explicitly retired.
          - If a Stage 1 or Stage 2 rerun becomes necessary, stop after the rerun and rewrite the workflow files around the new baseline.
  - pack_id: P4
    title: Widen scored families with existing scaffolding
    status: ready
    depends_on: [P3]
    objective: Keep growing the measurable scored set using the already-built family pipelines before inventing new complex rulebooks.
    acceptance:
      - At least ten additional rows are either scored or explicitly retired across epidemic, volcano, storm, politics_election, and aviation_space.
      - Every touched family has refreshed Stage 4+ export output.
      - Stage 8, Stage 9, and overview exports are refreshed after the family batch.
    commit_strategy: One commit per family batch or coherent cross-family replay; do not combine unrelated families into one opaque commit.
    slices:
      - slice_id: P4.S1
        title: Expand epidemic exact-hit catalog
        status: ready
        target_scope: epidemic
        allowed_files:
          - scripts/build_epidemic_event_ledger.py
          - scripts/score_epidemic_matches.py
          - scripts/assign_epidemic_probabilities.py
          - scripts/finalize_epidemic_predictions.py
          - scripts/export_epidemic_analysis.py
          - data/epidemic_prediction_overrides.json
          - data/epidemic_official_events.json
          - docs/agent_notes.md
        validators:
          - python3 -m py_compile scripts/build_epidemic_event_ledger.py scripts/score_epidemic_matches.py scripts/assign_epidemic_probabilities.py scripts/finalize_epidemic_predictions.py scripts/export_epidemic_analysis.py
        stop_conditions:
          - Stop when the next three source-backed disease-specific rows are scored or explicitly retired.
      - slice_id: P4.S2
        title: Expand volcano and storm backlogs
        status: ready
        target_scope: volcano_storm
        allowed_files:
          - scripts/build_volcano_event_ledger.py
          - scripts/score_volcano_matches.py
          - scripts/assign_volcano_probabilities.py
          - scripts/finalize_volcano_predictions.py
          - scripts/export_volcano_analysis.py
          - scripts/build_storm_event_ledger.py
          - scripts/score_storm_matches.py
          - scripts/assign_storm_probabilities.py
          - scripts/finalize_storm_predictions.py
          - scripts/export_storm_analysis.py
          - data/volcano_prediction_overrides.json
          - data/volcano_official_events.json
          - data/volcano_probability_baselines.json
          - data/storm_prediction_overrides.json
          - data/storm_official_events.json
          - data/storm_probability_baselines.json
          - docs/agent_notes.md
        validators:
          - python3 -m py_compile scripts/build_volcano_event_ledger.py scripts/score_volcano_matches.py scripts/build_storm_event_ledger.py scripts/score_storm_matches.py
        stop_conditions:
          - Stop when at least four new volcano or storm rows are scored or retired.
      - slice_id: P4.S3
        title: Expand politics and aviation-space backlogs
        status: ready
        target_scope: politics_aviation
        allowed_files:
          - scripts/build_politics_event_ledger.py
          - scripts/score_politics_matches.py
          - scripts/assign_politics_probabilities.py
          - scripts/finalize_politics_predictions.py
          - scripts/export_politics_analysis.py
          - scripts/build_aviation_space_event_ledger.py
          - scripts/score_aviation_space_matches.py
          - scripts/assign_aviation_space_probabilities.py
          - scripts/finalize_aviation_space_predictions.py
          - scripts/export_aviation_space_analysis.py
          - data/politics_prediction_overrides.json
          - data/politics_official_events.json
          - data/politics_probability_baselines.json
          - data/aviation_space_prediction_overrides.json
          - data/aviation_space_official_events.json
          - data/aviation_space_probability_baselines.json
          - docs/agent_notes.md
        validators:
          - python3 -m py_compile scripts/build_politics_event_ledger.py scripts/score_politics_matches.py scripts/build_aviation_space_event_ledger.py scripts/score_aviation_space_matches.py
        stop_conditions:
          - Stop when at least three new politics_election or aviation_space rows are scored or retired.
  - pack_id: P5
    title: Release-grade audit outputs
    status: in_progress
    depends_on: [P1, P4]
    objective: Produce one operator-friendly and Python-friendly reporting bundle from the aligned mainline.
    acceptance:
      - One command refreshes a consolidated audit report.
      - Output includes claimed-date counts, public-date-clean counts, family summaries, top exact hits, and rescue queue references.
      - The report clearly separates semantic hits from currently unrescued public-date conflicts.
    commit_strategy: Keep P5 split across at least three coherent commits: release-report implementation plus first bundle, rerun/documentation updates, and a final refreshed release snapshot or closeout verification.
    slices:
      - slice_id: P5.S1
        title: Add consolidated audit report script
        status: in_progress
        target_scope: reporting
        allowed_files:
          - scripts/export_prediction_audit_release_report.py
          - data/exports/release/**
          - docs/agent_notes.md
        validators:
          - python3 scripts/export_prediction_audit_release_report.py --stage2-run-key stage2-20260313T024045Z
        stop_conditions:
          - Stop when the script emits both machine-readable JSON and a short human-readable Markdown summary.
      - slice_id: P5.S2
        title: Document rerun workflow
        status: in_progress
        target_scope: documentation
        allowed_files:
          - README.md
          - docs/prediction_audit_supervisor_work_spec.md
          - docs/agent_notes.md
        validators:
          - rg -n "prediction audit|public-date|release report|stage2-20260313T024045Z" README.md docs/prediction_audit_supervisor_work_spec.md
        stop_conditions:
          - Stop when an operator can see how to rerun the aligned exports and where to find the current release summary.
  - pack_id: P6
    title: Next complex family bootstrap
    status: blocked
    depends_on: [P3, P4]
    objective: Start the first narrow war_conflict or climate_environment slice only after the current simpler families and recovery queue are under control.
    blocked_reason: Matching rules and source catalogs are not yet constrained enough; starting this now risks subjective event matching and diffused effort.
    acceptance:
      - A narrow family rulebook exists with concrete sources, measurable dimensions, and explicit miss conditions.
    commit_strategy: Do not start this pack until the blocker is removed.
    slices:
      - slice_id: P6.S1
        title: Draft narrow war_conflict rulebook
        status: blocked
        target_scope: war_conflict
        stop_conditions:
          - Stop unless exact event families, source catalog, and miss conditions are written first.
---

# Prediction Audit Supervisor Work Spec

This document is the operator-facing backlog for the Billy Meier prediction audit. It is meant to be consumed by a supervisor or supervisor-loop workflow, not as a narrative project plan.

## Recommended doc set

Use this work spec together with:

- `docs/prediction_audit_supervisor_work_spec.md`
- `billy_meier_staged_protocol.md`

The staged protocol defines the evaluation rules. This work spec defines what is still outstanding on the current mainline baseline.

## Current baseline summary

- Current aligned Stage 2 baseline: `stage2-20260313T024045Z`
- Current scored set: `110` predictions
- Claimed-date hits: `103`
- Claimed-date exact hits: `77`
- Current public-date-clean cohort: `8`
- Current public-date-clean exact hits: `1`
- Current public-date-pending-evidence cohort: `19`
- Current public-date-currently-unrescued cohort: `83`
- Current checked-in release snapshot: `data/exports/release/prediction-audit-release-20260313T040828Z/release_summary.md`

## Release bundle refresh

- Refresh the aligned release bundle with `python3 scripts/export_prediction_audit_release_report.py --stage2-run-key stage2-20260313T024045Z`.
- The script refreshes the aligned overview, provenance, research-queue, and unscored export surfaces before composing `data/exports/release/prediction-audit-release-<timestamp>/`.
- For a low-churn recomposition from already-aligned exports, use `python3 scripts/export_prediction_audit_release_report.py --stage2-run-key stage2-20260313T024045Z --skip-refresh`.
- Current checked-in machine-readable release snapshot: `data/exports/release/prediction-audit-release-20260313T040828Z/summary.json`.

## Supervisor guidance

- Do not run supervised implementation work on `main`. Use the branch named in the front matter unless the operator explicitly chooses another feature branch.
- Prefer larger coherent packs over tiny one-row handoffs.
- If a Stage 1 or Stage 2 rerun changes the aligned baseline, rewrite the workflow files before continuing. Do not let later packs silently mix run keys.
- When rescue attempts fail, record the dead end explicitly instead of re-trying the same path in later cycles.
- Do not let publication rescue work “upgrade” a row without explicit dated evidence.
- Do not create new family rulebooks when an existing family pipeline can score the row cleanly.

## Retirement rules

- A row can be retired from active work if it is:
  - permanently too vague to measure,
  - already covered by a better-scoped sibling row,
  - blocked on a missing primary source with no credible next path,
  - or dependent on a new complex family rulebook that has not yet been defined.

- Retirement must still leave a note in either:
  - `data/report_provenance_research_log.json`, or
  - `docs/agent_notes.md`

## Suggested first execution order

1. `P1` Small-gap provenance rescue
2. `P2` Deep-archive provenance triage
3. `P3` Unscored measurable-row recovery
4. `P4` Widen scored families with existing scaffolding
5. `P5` Release-grade audit outputs
6. `P6` stays blocked until the above packs settle

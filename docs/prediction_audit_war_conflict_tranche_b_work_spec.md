---
doc_type: supervisor_work_spec_v1
project: billymeierposts
workstream: prediction_audit
branch_name: supervisor/loop2
recommended_doc_spec:
  - docs/prediction_audit_war_conflict_tranche_b_work_spec.md
  - docs/war_conflict_dated_attack_rulebook.md
  - billy_meier_staged_protocol.md
baseline_stage2_run_key: stage2-20260313T092015Z
baseline_exports:
  overview: data/exports/overview/prediction-audit-overview-20260313T101813Z/summary.json
  publication_timing: data/exports/provenance/publication-timing-audit-20260313T101813Z/summary.json
  cohort_comparison: data/exports/provenance/cohort-comparison-20260313T101813Z/summary.json
  research_queue: data/exports/provenance/public-date-research-queue-20260313T101813Z/summary.json
  unscored_queue: data/exports/unscored/unscored-prediction-queue-20260313T101814Z/summary.json
  release_bundle: data/exports/release/prediction-audit-release-20260313T101821Z/summary.json
  war_conflict_family: data/exports/war_conflict/stage4-war-conflict-20260313T101724Z/summary.json
current_state:
  candidate_count: 6797
  eligible_count: 1289
  significant_count: 340
  included_scored_count: 145
  claimed_hit_count: 134
  claimed_exact_hit_count: 105
  public_date_clean_count: 10
  public_date_pending_more_evidence_count: 29
  public_date_currently_unrescued_count: 106
  war_conflict_included_scored_count: 7
  war_conflict_exact_hit_count: 7
  unscored_prediction_count: 1877
  supported_family_unscored_count: 1381
  war_conflict_promote_bucket_count: 347
  war_conflict_stage2_revisit_count: 274
global_assumptions:
  - `supervisor/loop2` remains the active non-`main` execution branch for this follow-on slice.
  - The dated-attack `war_conflict` rulebook in `docs/war_conflict_dated_attack_rulebook.md` remains the scope boundary; this doc does not authorize a broader war, terrorism-trend, or geopolitical rulebook.
  - The current aligned baseline stays `stage2-20260313T092015Z`; this slice should reuse the checked-in `war_conflict` family pipeline rather than rerunning Stage 1 or Stage 2 unless a blocker forces a replan.
global_constraints:
  - Do not implement on `main`.
  - Do not create or switch branches mid-cycle without caller direction.
  - Do not mutate `contact_reports.predictions` or `accurate_predictions`.
  - Keep commits logically coherent and keep the repo clean at pack boundaries.
  - Run `war_conflict` finalization before aligned Stage 8, then Stage 9, then touched family and aligned exports sequentially when outcomes change.
  - Reject curated rows whose matched event predates `claimed_contact_date`.
  - Do not widen the `war_conflict` rulebook beyond atomic dated attacks and bombings; if a target row needs a broader theme, retire or defer it explicitly instead of stretching the matcher.
global_validators:
  - python3 -m py_compile scripts/build_war_conflict_event_ledger.py scripts/score_war_conflict_matches.py scripts/assign_war_conflict_probabilities.py scripts/finalize_war_conflict_predictions.py scripts/export_war_conflict_analysis.py
  - python3 scripts/export_unscored_prediction_queue.py --stage2-run-key stage2-20260313T092015Z
  - python3 scripts/export_prediction_audit_overview.py --stage2-run-key stage2-20260313T092015Z
definition_of_done:
  - At least twelve targeted `war_conflict` rows are either scored or explicitly retired under the existing dated-attack rulebook.
  - The refreshed `war_conflict` family export, aligned overview, provenance, research-queue, unscored, and release outputs all reconcile on `stage2-20260313T092015Z`.
  - `0/20` targeted keys remain in the refreshed unscored queue.
  - Repo is clean.
packs:
  - pack_id: P1
    title: War-conflict dated-attack tranche B
    status: ready
    depends_on: []
    objective: Reuse the new `war_conflict` pipeline to widen the same narrow dated-attack slice before opening any broader war-conflict rulebook.
    acceptance:
      - At least twelve targeted rows are either scored or explicitly retired.
      - The tranche stays inside the existing dated-attack rulebook without adding new match dimensions.
      - The refreshed queue proves movement by dropping all listed target keys.
      - `war_conflict` family export plus aligned provenance, research-queue, overview, unscored, and release artifacts are refreshed on the same baseline.
    commit_strategy: Keep this as one coherent pack with multiple commits: curate event/override additions first, replay the tranche second, and refresh aligned exports plus closeout evidence third.
    slices:
      - slice_id: P1.S1
        title: Curate first dated-attack target batch
        status: ready
        target_rows: ["136:251", "182:57", "201:12", "216:33", "231:4", "231:5", "238:68", "241:16", "264:2", "385:1"]
        target_scope: war_conflict
        allowed_files:
          - docs/war_conflict_dated_attack_rulebook.md
          - data/war_conflict_official_events.json
          - data/war_conflict_prediction_overrides.json
          - data/war_conflict_probability_baselines.json
          - data/war_conflict_final_adjudications.json
          - docs/agent_notes.md
        validators:
          - python3 -m py_compile scripts/build_war_conflict_event_ledger.py scripts/score_war_conflict_matches.py scripts/assign_war_conflict_probabilities.py scripts/finalize_war_conflict_predictions.py
        stop_conditions:
          - Stop when every listed row has either a curated in-scope event candidate or an explicit defer/retire rationale.
          - Stop early if more than three listed rows require a broader rulebook than dated attacks and bombings.
      - slice_id: P1.S2
        title: Curate second dated-attack target batch
        status: ready
        target_rows: ["388:8", "441:5", "589:8", "622:22", "718:30", "726:6", "795:6", "830:3", "866:9", "898:9"]
        target_scope: war_conflict
        allowed_files:
          - docs/war_conflict_dated_attack_rulebook.md
          - data/war_conflict_official_events.json
          - data/war_conflict_prediction_overrides.json
          - data/war_conflict_probability_baselines.json
          - data/war_conflict_final_adjudications.json
          - docs/agent_notes.md
        validators:
          - python3 -m py_compile scripts/build_war_conflict_event_ledger.py scripts/score_war_conflict_matches.py scripts/assign_war_conflict_probabilities.py scripts/finalize_war_conflict_predictions.py
        stop_conditions:
          - Stop when every listed row has either a curated in-scope event candidate or an explicit defer/retire rationale.
          - Stop early if the batch materially drifts into open-ended war, ideology, or consequence claims.
      - slice_id: P1.S3
        title: Replay tranche and refresh aligned exports
        status: ready
        target_rows: ["136:251", "182:57", "201:12", "216:33", "231:4", "231:5", "238:68", "241:16", "264:2", "385:1", "388:8", "441:5", "589:8", "622:22", "718:30", "726:6", "795:6", "830:3", "866:9", "898:9"]
        target_scope: war_conflict
        allowed_files:
          - scripts/build_war_conflict_event_ledger.py
          - scripts/score_war_conflict_matches.py
          - scripts/assign_war_conflict_probabilities.py
          - scripts/finalize_war_conflict_predictions.py
          - scripts/export_war_conflict_analysis.py
          - scripts/review_publication_timing.py
          - scripts/finalize_public_date_cohort.py
          - scripts/export_publication_timing_audit.py
          - scripts/export_cohort_comparison.py
          - scripts/export_public_date_research_queue.py
          - scripts/export_unscored_prediction_queue.py
          - scripts/export_prediction_audit_overview.py
          - scripts/export_prediction_audit_release_report.py
          - data/war_conflict_official_events.json
          - data/war_conflict_prediction_overrides.json
          - data/war_conflict_probability_baselines.json
          - data/war_conflict_final_adjudications.json
          - data/exports/war_conflict/**
          - data/exports/provenance/**
          - data/exports/unscored/**
          - data/exports/overview/**
          - data/exports/release/**
          - docs/agent_notes.md
        validators:
          - python3 -m py_compile scripts/*.py
          - python3 scripts/export_prediction_audit_overview.py --stage2-run-key stage2-20260313T092015Z
          - python3 scripts/export_unscored_prediction_queue.py --stage2-run-key stage2-20260313T092015Z
        stop_conditions:
          - Stop when at least twelve listed keys are scored or retired and the refreshed queue shows `0/20` remaining targets.
          - Stop early and return for supervisor review if the tranche cannot reach twelve rows without revising the rulebook.

---
doc_type: supervisor_work_spec_v1
project: billymeierposts
workstream: prediction_audit
branch_name: feat/prediction-audit-supported-backlog-wave1
recommended_doc_spec:
  - docs/prediction_audit_next_volume_work_spec.md
  - billy_meier_staged_protocol.md
baseline_stage2_run_key: stage2-20260313T024045Z
baseline_exports:
  overview: data/exports/overview/prediction-audit-overview-20260313T041049Z/summary.json
  publication_timing: data/exports/provenance/publication-timing-audit-20260313T041049Z/summary.json
  cohort_comparison: data/exports/provenance/cohort-comparison-20260313T041049Z/summary.json
  research_queue: data/exports/provenance/public-date-research-queue-20260313T041049Z/summary.json
  unscored_queue_summary: data/exports/unscored/unscored-prediction-queue-20260313T041050Z/summary.json
  unscored_queue_csv: data/exports/unscored/unscored-prediction-queue-20260313T041050Z/queue.csv
  release_bundle: data/exports/release/prediction-audit-release-20260313T041050Z/summary.json
current_state:
  candidate_count: 6797
  eligible_count: 1226
  significant_count: 316
  included_scored_count: 110
  claimed_hit_count: 103
  claimed_exact_hit_count: 77
  public_date_clean_count: 8
  public_date_pending_more_evidence_count: 19
  public_date_currently_unrescued_count: 83
  unscored_prediction_count: 1627
  supported_family_unscored_count: 738
  recovery_bucket_counts:
    promote_via_existing_family_pipeline: 528
    stage2_revisit_in_supported_family: 191
    existing_pipeline_outside_current_p3_scope: 81
    needs_parser_or_stage2_family_resolution: 279
    outside_current_rulebook_scope: 496
    retire_past_event_reference: 52
  queue_family_guess_counts:
    aviation_space: 94
    climate_environment: 121
    earthquake: 81
    economy: 41
    epidemic: 324
    politics_election: 220
    science_technology: 37
    storm: 71
    unknown: 296
    volcano: 29
    war_conflict: 313
  scored_family_counts:
    aviation_space: 1
    earthquake: 52
    epidemic: 15
    politics_election: 17
    storm: 6
    volcano: 19
global_assumptions:
  - The closed `supervisor/loop` branch remains the accepted release baseline and should not be reopened for normal backlog work.
  - The next implementation cycle starts from `stage2-20260313T024045Z` unless a pack explicitly reruns Stage 1 or Stage 2.
  - Existing family pipelines for aviation_space, earthquake, epidemic, politics_election, storm, and volcano remain the fastest path for backlog reduction.
  - New-family work is lower priority than draining supported-family backlog already marked `promote_via_existing_family_pipeline`, `stage2_revisit_in_supported_family`, or `existing_pipeline_outside_current_p3_scope`.
global_constraints:
  - Do not mutate contact_reports.predictions or accurate_predictions.
  - Use prediction_audit_* tables and existing staged scripts.
  - Keep run keys and exported artifacts aligned to one Stage 2 baseline at a time.
  - When replaying a family, run Stage 7 before Stage 8, then Stage 9, then refresh exports.
  - Reject curated rows whose matched event predates claimed_contact_date.
  - Keep family slices narrow and source-backed; retire rows explicitly instead of stretching rulebooks.
  - Treat `.workflow/*` from the closed branch as historical context only; create fresh workflow state for the new cycle.
  - Keep repo committed before and after each work bundle.
global_validators:
  - python3 -m py_compile scripts/*.py
  - python3 scripts/export_unscored_prediction_queue.py --stage2-run-key stage2-20260313T024045Z
  - python3 scripts/export_prediction_audit_overview.py --stage2-run-key stage2-20260313T024045Z
definition_of_done:
  - At least thirty supported-family backlog rows are either scored or explicitly retired with rationale.
  - The unscored queue is refreshed and materially reduced in the `promote_via_existing_family_pipeline`, `stage2_revisit_in_supported_family`, or `existing_pipeline_outside_current_p3_scope` buckets.
  - All touched family exports plus aligned overview, provenance, and research queue exports are refreshed on one baseline.
  - Any Stage 1 or Stage 2 rerun is deliberate, documented, and followed by rewritten workflow state around the new baseline.
  - Repo is clean.
packs:
  - pack_id: P1
    title: Supported-family promotion tranche
    status: ready
    depends_on: []
    objective: Drain the highest-priority backlog already tagged `promote_via_existing_family_pipeline` using existing family pipelines before inventing new rulebooks.
    acceptance:
      - At least fifteen rows are either scored or explicitly retired across politics_election, epidemic, aviation_space, volcano, and storm.
      - Every touched family has refreshed Stage 4+ export output.
      - Stage 8, Stage 9, overview, provenance, and unscored exports are refreshed after the tranche.
    commit_strategy: One commit per coherent family batch; keep replay and export refresh commits grouped with their family changes.
    slices:
      - slice_id: P1.S1
        title: Politics significant-row promotion batch
        status: ready
        target_rows: ["136:136", "136:138", "136:139", "136:148", "136:161", "136:163", "136:211", "136:221", "150:105", "225:7"]
        target_scope: politics_election
        allowed_files:
          - scripts/build_politics_event_ledger.py
          - scripts/score_politics_matches.py
          - scripts/assign_politics_probabilities.py
          - scripts/finalize_politics_predictions.py
          - scripts/export_politics_analysis.py
          - data/politics_prediction_overrides.json
          - data/politics_probability_baselines.json
          - data/exports/politics/**
          - docs/agent_notes.md
        validators:
          - python3 scripts/build_politics_event_ledger.py --stage2-run-key stage2-20260313T024045Z
          - python3 scripts/export_politics_analysis.py --stage2-run-key stage2-20260313T024045Z
        stop_conditions:
          - Stop when at least eight target rows are scored or explicitly retired.
          - Stop early if the target rows require a Stage 1 or Stage 2 rerun instead of a family replay.
      - slice_id: P1.S2
        title: Aviation and volcano significant-row promotion batch
        status: ready
        target_rows: ["72:1", "202:5", "214:26", "238:41", "258:11", "366:27", "383:13", "481:4", "537:3", "688:25", "779:464"]
        target_scope: aviation_space_plus_volcano
        allowed_files:
          - scripts/build_aviation_event_ledger.py
          - scripts/score_aviation_matches.py
          - scripts/assign_aviation_probabilities.py
          - scripts/finalize_aviation_predictions.py
          - scripts/export_aviation_analysis.py
          - scripts/build_volcano_event_ledger.py
          - scripts/score_volcano_matches.py
          - scripts/assign_volcano_probabilities.py
          - scripts/finalize_volcano_predictions.py
          - scripts/export_volcano_analysis.py
          - data/aviation_prediction_overrides.json
          - data/aviation_probability_baselines.json
          - data/volcano_prediction_overrides.json
          - data/volcano_probability_baselines.json
          - data/exports/aviation/**
          - data/exports/volcano/**
          - docs/agent_notes.md
        validators:
          - python3 -m py_compile scripts/build_volcano_event_ledger.py scripts/score_volcano_matches.py scripts/finalize_volcano_predictions.py
        stop_conditions:
          - Stop when both families have either replay-ready scoring rows or explicit retirements for the listed targets.
          - Stop early if aviation tooling gaps prove the family is not yet replay-ready on this baseline.
      - slice_id: P1.S3
        title: Epidemic and storm significant-row promotion batch
        status: ready
        target_rows: ["369:2", "369:13", "395:6", "721:6", "731:2", "732:2", "734:1", "767:28", "779:233"]
        target_scope: epidemic_plus_storm
        allowed_files:
          - scripts/build_epidemic_event_ledger.py
          - scripts/score_epidemic_matches.py
          - scripts/assign_epidemic_probabilities.py
          - scripts/finalize_epidemic_predictions.py
          - scripts/export_epidemic_analysis.py
          - scripts/build_storm_event_ledger.py
          - scripts/score_storm_matches.py
          - scripts/assign_storm_probabilities.py
          - scripts/finalize_storm_predictions.py
          - scripts/export_storm_analysis.py
          - data/epidemic_prediction_overrides.json
          - data/epidemic_probability_baselines.json
          - data/storm_prediction_overrides.json
          - data/storm_probability_baselines.json
          - data/exports/epidemic/**
          - data/exports/storm/**
          - docs/agent_notes.md
        validators:
          - python3 -m py_compile scripts/build_epidemic_event_ledger.py scripts/build_storm_event_ledger.py
        stop_conditions:
          - Stop when both families have either replay-ready scoring rows or explicit retirements for the listed targets.
          - Stop early if one family requires parser or Stage 2 changes instead of family replay work.
  - pack_id: P2
    title: Supported-family Stage 2 revisit tranche
    status: ready
    depends_on: [P1]
    objective: Revisit prediction rows already mapped to supported families but still labeled `prediction_but_not_measurable`, turning the best candidates into scored rows or explicit retirements.
    acceptance:
      - At least ten rows are either promoted or retired across epidemic, politics_election, storm, aviation_space, and volcano.
      - Any rule or override change is reflected in touched family exports.
      - The unscored queue is refreshed and the touched rows disappear from the `stage2_revisit_in_supported_family` bucket.
    commit_strategy: One commit per family or paired-family revisit batch; keep Stage 2 review changes separate from downstream family replays.
    slices:
      - slice_id: P2.S1
        title: Revisit politics and volcano borderline rows
        status: ready
        target_rows: ["79:15", "113:17", "136:29", "136:32", "136:118", "136:137", "136:149", "136:162", "136:206", "136:215", "214:10", "214:27"]
        target_scope: politics_election_plus_volcano
        allowed_files:
          - scripts/review_prediction_candidates.py
          - scripts/export_unscored_prediction_queue.py
          - scripts/build_politics_event_ledger.py
          - scripts/score_politics_matches.py
          - scripts/assign_politics_probabilities.py
          - scripts/finalize_politics_predictions.py
          - scripts/build_volcano_event_ledger.py
          - scripts/score_volcano_matches.py
          - scripts/assign_volcano_probabilities.py
          - scripts/finalize_volcano_predictions.py
          - data/politics_prediction_overrides.json
          - data/politics_probability_baselines.json
          - data/volcano_prediction_overrides.json
          - data/volcano_probability_baselines.json
          - data/exports/unscored/**
          - docs/agent_notes.md
        validators:
          - python3 scripts/export_unscored_prediction_queue.py --stage2-run-key stage2-20260313T024045Z
        stop_conditions:
          - Stop when at least six listed rows are promoted or retired.
          - Stop early if a Stage 1 or Stage 2 rerun becomes necessary.
      - slice_id: P2.S2
        title: Revisit epidemic, storm, and aviation borderline rows
        status: ready
        target_rows: ["52:1", "79:13", "115:11", "131:1", "150:73", "150:74", "150:80", "150:137", "166:4", "166:5", "182:28", "182:29", "182:32", "182:37", "182:38", "215:72", "218:6", "220:15", "220:17", "221:1", "229:55", "229:56", "229:115", "230:53"]
        target_scope: epidemic_plus_storm_plus_aviation
        allowed_files:
          - scripts/review_prediction_candidates.py
          - scripts/export_unscored_prediction_queue.py
          - scripts/build_epidemic_event_ledger.py
          - scripts/score_epidemic_matches.py
          - scripts/assign_epidemic_probabilities.py
          - scripts/finalize_epidemic_predictions.py
          - scripts/build_storm_event_ledger.py
          - scripts/score_storm_matches.py
          - scripts/assign_storm_probabilities.py
          - scripts/finalize_storm_predictions.py
          - scripts/build_aviation_event_ledger.py
          - scripts/score_aviation_matches.py
          - scripts/assign_aviation_probabilities.py
          - scripts/finalize_aviation_predictions.py
          - data/epidemic_prediction_overrides.json
          - data/epidemic_probability_baselines.json
          - data/storm_prediction_overrides.json
          - data/storm_probability_baselines.json
          - data/aviation_prediction_overrides.json
          - data/aviation_probability_baselines.json
          - data/exports/unscored/**
          - docs/agent_notes.md
        validators:
          - python3 scripts/export_unscored_prediction_queue.py --stage2-run-key stage2-20260313T024045Z
        stop_conditions:
          - Stop when at least eight listed rows are promoted or retired.
          - Stop early if multiple families require parser-stage fixes instead of family review changes.
  - pack_id: P3
    title: Earthquake backlog replay tranche
    status: ready
    depends_on: [P1]
    objective: Clear the earthquake queue already tagged `existing_pipeline_outside_current_p3_scope` using the mature earthquake rulebook instead of leaving those rows parked.
    acceptance:
      - At least twelve earthquake rows are either scored or explicitly retired.
      - Earthquake exports are refreshed and aligned exports are rerun after the tranche.
      - The earthquake rows addressed in this pack leave the `existing_pipeline_outside_current_p3_scope` bucket.
    commit_strategy: Split override/remap preparation, earthquake replay, and aligned export refresh into separate commits when all are needed.
    slices:
      - slice_id: P3.S1
        title: Earthquake significant-row replay batch
        status: ready
        target_rows: ["136:26", "136:28", "136:44", "136:86", "136:172", "136:174", "136:180", "150:142", "238:30", "238:210", "241:57", "241:67", "369:22", "377:23", "392:2", "392:4", "395:3", "400:6", "400:9", "400:12", "401:3", "401:12", "416:1", "420:2", "436:3", "446:9", "453:16", "453:22", "519:4", "663:14"]
        target_scope: earthquake
        allowed_files:
          - scripts/build_earthquake_event_ledger.py
          - scripts/score_earthquake_matches.py
          - scripts/assign_earthquake_probabilities.py
          - scripts/rollup_earthquake_bundles.py
          - scripts/finalize_earthquake_predictions.py
          - scripts/export_earthquake_analysis.py
          - data/earthquake_prediction_overrides.json
          - data/earthquake_probability_baselines.json
          - data/earthquake_final_adjudications.json
          - data/exports/earthquake/**
          - docs/agent_notes.md
        validators:
          - python3 -m py_compile scripts/build_earthquake_event_ledger.py scripts/score_earthquake_matches.py scripts/finalize_earthquake_predictions.py
        stop_conditions:
          - Stop when at least twelve rows are scored or retired.
          - Stop early if override-key drift or remap ambiguity requires a Stage 1 or Stage 2 rerun.
      - slice_id: P3.S2
        title: Earthquake eligible-row follow-on batch
        status: ready
        target_rows: ["106:2", "136:24", "136:35", "136:54", "136:57", "136:87", "150:6", "150:39", "150:135", "182:53"]
        target_scope: earthquake
        allowed_files:
          - scripts/build_earthquake_event_ledger.py
          - scripts/score_earthquake_matches.py
          - scripts/assign_earthquake_probabilities.py
          - scripts/rollup_earthquake_bundles.py
          - scripts/finalize_earthquake_predictions.py
          - scripts/export_earthquake_analysis.py
          - data/earthquake_prediction_overrides.json
          - data/earthquake_probability_baselines.json
          - data/earthquake_final_adjudications.json
          - data/exports/earthquake/**
          - docs/agent_notes.md
        validators:
          - python3 scripts/export_earthquake_analysis.py --stage2-run-key stage2-20260313T024045Z
        stop_conditions:
          - Stop when the tranche either scores or retires at least six of the listed rows.
          - Stop early if the significant-row batch already consumes the safe work budget for this pack.
  - pack_id: P4
    title: Parser and Stage 2 family-resolution tranche
    status: ready
    depends_on: [P1, P2]
    objective: Reduce the `unknown` queue that still needs parser or Stage 2 family resolution without silently mutating baseline assumptions.
    acceptance:
      - At least ten high-priority `unknown` rows are either assigned to a supported family, retired, or explicitly documented as unresolved.
      - If Stage 1 or Stage 2 reruns happen, the active baseline, queue export, overview, and touched family exports are updated deliberately.
      - The new workflow state records the new baseline if one is created.
    commit_strategy: Keep parser fixes, Stage 2 review rules, and downstream family replays in separate commits.
    slices:
      - slice_id: P4.S1
        title: Resolve top unknown-family candidates
        status: ready
        target_scope: parser_stage2_gap
        allowed_files:
          - scripts/parse_contact_report_predictions.py
          - scripts/review_prediction_candidates.py
          - scripts/export_unscored_prediction_queue.py
          - data/*_prediction_overrides.json
          - data/*_official_events.json
          - data/*_probability_baselines.json
          - data/exports/unscored/**
          - docs/agent_notes.md
        validators:
          - python3 -m py_compile scripts/parse_contact_report_predictions.py scripts/review_prediction_candidates.py scripts/export_unscored_prediction_queue.py
        stop_conditions:
          - Stop after ten high-priority unknown rows are resolved, retired, or documented as unresolved.
          - If a full-corpus rerun becomes necessary, stop after the rerun and rewrite the workflow files around the new baseline.
  - pack_id: P5
    title: New family bootstrap after backlog drain
    status: blocked
    depends_on: [P1, P2, P3, P4]
    objective: Start the first narrow war_conflict or climate_environment rulebook only after the supported-family backlog is materially reduced.
    acceptance:
      - A narrow rulebook exists with explicit sources, miss conditions, and target rows.
      - The first slice stays inside one family and one sub-theme.
      - The new family exports reconcile with the aligned baseline and refreshed queue.
    commit_strategy: One commit for rulebook/data setup, one for replay/scoring, one for export refresh.
    slices:
      - slice_id: P5.S1
        title: Draft a narrow rulebook for war_conflict or climate_environment
        status: blocked
        target_scope: new_family_bootstrap
        allowed_files:
          - docs/**
          - scripts/**
          - data/**
        validators:
          - python3 -m py_compile scripts/*.py
        stop_conditions:
          - Stop unless the supervisor explicitly reauthorizes the family and names the exact sub-theme to pursue.

# Agent Notes

- Keep this file evergreen; remove dated run keys, one-off corpus counts, and completion logs.
- Use `contact_reports.english_content` as the extraction corpus; ignore legacy `predictions` JSON for new pipeline work.
- Keep `prediction_audit_runs`, `prediction_audit_predictions`, and `prediction_audit_bundles` separated by responsibility.
- Validate schema changes with committed SQL plus catalog checks before rerunning the pipeline.
- Run the pipeline in ordered stages: parse, review, family ledger, scoring, probability assignment, export, and final review.
- Use local override and adjudication JSON files for target resolution and final-review decisions instead of hardcoding exceptions.
- Keep per-row outcome probabilities mutually exclusive and summing to `1`.
- Scope bundle rollups and exports to the same cohort used by the earlier ledger and scoring stages.
- Only sweep unrelated repo dirt into a commit when the user explicitly asks for a full-repo commit.
- Keep repo branch workflow linear: branch from `main`, merge back to `main`, delete merged branches, then start the next bundle from `main`.
- Reconcile `.workflow` state against `git log` and export timestamps after an interrupted supervisor loop; landed pack commits can outpace the stale implementer report and run log.
- Rerun aligned Stage `8` and `9` plus provenance, unscored, overview, and release exports after recovering a landed family replay so queue counts reflect the new family outcomes.

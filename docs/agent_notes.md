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
- After accepting a pack, move `.workflow` forward immediately so the next instruction does not leave the branch parked on a stale review checkpoint.
- Retire null-window supported-family revisit rows in family `final_adjudications.json`; prove coverage first with `finalize_* --dry-run` before running the real family closeout.
- Keep the aviation-space revisit slice narrow; retire plane-crash and vague satellite-network rows instead of widening the current satellite-action catalog.
- Check git writability before pack closeout; this sandbox can block `git commit` by rejecting `.git/index.lock` even when normal workspace edits succeed.
- When pack content is complete but `.git` is unwritable, mark the branch `blocked`, keep the active pack unchanged, and require coherent commits before advancing to the next pack.
- Package recovered supervisor-loop diffs with family-batch commits, aligned export closeout, and a final implementer handoff commit once git writes are restored.

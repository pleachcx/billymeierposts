# AGENTS.md

## Notes & Learning Loop
- Read `/docs/agent_notes.md` before starting each task; create the file if missing.
- Append bullet-only notes to `/docs/agent_notes.md` after failures, retries, rework, ambiguity resolution, reusable helper creation, and task completion.
- Keep notes short, imperative, and actionable; include root cause, effective fix, and preventive checks when relevant.
- Promote repeated stable notes from `/docs/agent_notes.md` into permanent repository docs when appropriate.

## Workflow
- Keep repo commited before and after you commence work on a work bundle
- Agents can push when explicitly instructed; merge only when explicitly instructed and when no higher-priority policy forbids it.
- Don't create branches with explicit approval previous branch has been merged

## DB
- DB is accessible via env vars

## Root Cause 
- Fix root causes first; do not ship bandaid or temporary-only patches as the final solution.

## Branch Workflow
- Branches should follow a straight-line workflow: create a branch from `main`, do the work on that branch, merge that branch, return to `main`, delete the merged branch, then create the next branch from `main`.
- Do not create a feature branch from another feature branch unless explicitly instructed.
- Before starting a new work bundle, make sure the prior branch is merged and `main` is current.
- You may merge a branch only when explicitly instructed and only when no higher-priority runtime or environment policy forbids it.
- Preferred branch lifecycle:
- Start from updated `main`.
- Create one feature branch for one work bundle.
- Complete work, validate, and merge that branch.
- Switch back to `main` before starting the next bundle.
- Delete merged local and remote branches before creating the next branch.

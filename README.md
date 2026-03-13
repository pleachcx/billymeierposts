# billymeierposts
This repo contains a collection of pregenerated social media posts, content ideas and a breakdown into the probability of Billy nailing the predictions that he made throughout the contact reports.

Feel free to use any of this content, anytime, without hesitation. Available to all who seek and spread the truth.

Site: [https://pleachcx.github.io/billymeierposts/](https://pleachcx.github.io/billymeierposts/)

## Prediction Audit Release Bundle

The aligned prediction-audit baseline is currently `stage2-20260313T024045Z`.

Refresh the branch-level release bundle with one command:

```bash
python3 scripts/export_prediction_audit_release_report.py --stage2-run-key stage2-20260313T024045Z
```

The script will load `DatabaseURL` from the current environment and falls back to `.env` when that file exists. By default it refreshes the aligned overview, provenance, research-queue, and unscored exports before composing the release bundle under `data/exports/release/`.

If you only want to recompute the release bundle from already-aligned exports without generating fresh branch-level export directories, use:

```bash
python3 scripts/export_prediction_audit_release_report.py --stage2-run-key stage2-20260313T024045Z --skip-refresh
```

Current checked-in release snapshot:

- Markdown summary: `data/exports/release/prediction-audit-release-20260313T041050Z/release_summary.md`
- Machine summary: `data/exports/release/prediction-audit-release-20260313T041050Z/summary.json`

The release bundle keeps three cohorts separate on purpose:

- claimed-date scored results,
- public-date-clean rows,
- currently unrescued publication conflicts plus the live research queue and unscored queue references.

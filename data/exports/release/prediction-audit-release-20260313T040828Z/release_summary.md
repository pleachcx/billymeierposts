# Prediction Audit Release Snapshot

- Generated at: `2026-03-13T04:08:28.246832+00:00`
- Stage 2 baseline: `stage2-20260313T024045Z`
- Script version: `prediction_audit_release_report_v1`

## Claimed-Date Baseline

- Candidate predictions parsed: `6797`
- Eligible predictions: `1226`
- Significant predictions: `316`
- Included scored rows: `110`
- Hits: `103` (`77` exact, `18` near, `8` similar-only)
- Misses: `7`
- Combined observed log10 probability sum: `-168.682579`

## Public-Date Separation

| Cohort | Rows | Hits | Exact | Misses | log10(sum p_obs) |
| --- | --- | --- | --- | --- | --- |
| public-date clean | 8 | 1 | 1 | 7 | -13.353612 |
| pending evidence | 19 | 19 | 13 | 0 | -24.385249 |
| publication conflicts | 83 | 83 | 63 | 0 | -130.943717 |

## Family Summary

| Family | Claimed Scored | Claimed Exact | Public Clean | Clean Exact | Pending | Conflicts |
| --- | --- | --- | --- | --- | --- | --- |
| aviation_space | 1 | 1 | 0 | 0 | 0 | 1 |
| earthquake | 52 | 27 | 5 | 0 | 10 | 37 |
| epidemic | 15 | 12 | 2 | 1 | 4 | 9 |
| politics_election | 17 | 17 | 0 | 0 | 0 | 17 |
| storm | 6 | 6 | 0 | 0 | 0 | 6 |
| volcano | 19 | 14 | 1 | 0 | 5 | 13 |

## Top Exact Hits

_Ranked by observed probability under each family's current null model. These family-specific nulls are not directly interchangeable._

| Report/Candidate | Family | Public-Date Status | p_obs | Observed Event |
| --- | --- | --- | --- | --- |
| 465/5 | earthquake | publication conflict | 0.000154971 | M 7.9 - 58 km W of Tianpeng, China |
| 136/89 | earthquake | publication conflict | 0.000258271 | M 6.7 - 5 km SW of Domvraína, Greece |
| 400/7 | earthquake | pending evidence | 0.000258271 | M 7.6 - 21 km NNE of Muzaffar?b?d, Pakistan |
| 829/17 | politics_election | publication conflict | 0.000278203 | Death of Pope Emeritus Benedict XVI |
| 459/5 | politics_election | publication conflict | 0.000314911 | Kosovo Declaration of Independence |
| 448/8 | storm | publication conflict | 0.000477373 | Greensburg tornado devastated the town |
| 442/4 | earthquake | publication conflict | 0.000516476 | M 6.0 - 181 km SW of Sagres, Portugal |
| 246/23 | earthquake | publication conflict | 0.000671366 | M 7.7 - 107 km W of Iwanai, Japan |
| 427/1 | earthquake | publication conflict | 0.000929464 | M 7.7 - 226 km SSW of Singaparna, Indonesia |
| 402/5 | earthquake | publication conflict | 0.00103268 | M 4.4 - 2 km NNW of Brugg, Switzerland |
| 459/4 | politics_election | publication conflict | 0.00106895 | Apology to Australia's Indigenous Peoples |
| 453/17 | earthquake | publication conflict | 0.00113589 | M 6.0 - 46 km S of Paracas, Peru |
| 408/5 | earthquake | pending evidence | 0.00144546 | M 6.7 - 26 km NE of Kýthira, Greece |
| 446/10 | earthquake | pending evidence | 0.00160021 | M 6.0 - 5 km SE of El Paraíso, Mexico |
| 136/90 | earthquake | publication conflict | 0.00190963 | M 6.7 - 5 km SW of Domvraína, Greece |

## Top Publication-Conflict Rows

| Rank | Report/Candidate | Family | Surprisal | Gap Bucket | Observed Event |
| --- | --- | --- | --- | --- | --- |
| 1 | 465/5 | earthquake | 3.80975 | deep_archive_gap | M 7.9 - 58 km W of Tianpeng, China |
| 2 | 136/89 | earthquake | 3.587924 | deep_archive_gap | M 6.7 - 5 km SW of Domvraína, Greece |
| 3 | 829/17 | politics_election | 3.555638 | tiny_gap | Death of Pope Emeritus Benedict XVI |
| 4 | 459/5 | politics_election | 3.501812 | deep_archive_gap | Kosovo Declaration of Independence |
| 5 | 448/8 | storm | 3.321143 | deep_archive_gap | Greensburg tornado devastated the town |
| 6 | 442/4 | earthquake | 3.28695 | large_gap | M 6.0 - 181 km SW of Sagres, Portugal |
| 7 | 246/23 | earthquake | 3.173041 | deep_archive_gap | M 7.7 - 107 km W of Iwanai, Japan |
| 8 | 427/1 | earthquake | 3.031767 | large_gap | M 7.7 - 226 km SSW of Singaparna, Indonesia |
| 9 | 402/5 | earthquake | 2.986032 | deep_archive_gap | M 4.4 - 2 km NNW of Brugg, Switzerland |
| 10 | 459/4 | politics_election | 2.971044 | deep_archive_gap | Apology to Australia's Indigenous Peoples |
| 11 | 453/17 | earthquake | 2.944662 | deep_archive_gap | M 6.0 - 46 km S of Paracas, Peru |
| 12 | 136/90 | earthquake | 2.719051 | deep_archive_gap | M 6.7 - 5 km SW of Domvraína, Greece |
| 13 | 519/1 | earthquake | 2.643879 | deep_archive_gap | M 7.1 - 29 km ESE of Ishinomaki, Japan |
| 14 | 459/7 | aviation_space | 2.63799 | deep_archive_gap | USA-193 satellite intercept |
| 15 | 150/133 | volcano | 2.5904 | deep_archive_gap | Kilauea East Rift Zone eruption began |

## Open Queues

- Research queue rows: `83`. Current aligned export: `data/exports/provenance/public-date-research-queue-20260313T035032Z`
- Unscored queue rows: `1627`. Current aligned export: `data/exports/unscored/unscored-prediction-queue-20260313T035033Z`

| Rank | Report/Candidate | Family | Stage 2 Label | Recovery Bucket |
| --- | --- | --- | --- | --- |
| 1 | 72/1 | volcano | significant_prediction | promote_via_existing_family_pipeline |
| 2 | 136/136 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 3 | 136/138 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 4 | 136/139 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 5 | 136/148 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 6 | 136/161 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 7 | 136/163 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 8 | 136/211 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 9 | 136/221 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 10 | 150/105 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 11 | 202/5 | aviation_space | significant_prediction | promote_via_existing_family_pipeline |
| 12 | 214/26 | volcano | significant_prediction | promote_via_existing_family_pipeline |
| 13 | 225/7 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 14 | 238/41 | volcano | significant_prediction | promote_via_existing_family_pipeline |
| 15 | 238/85 | politics_election | significant_prediction | promote_via_existing_family_pipeline |

## Source Exports

- `overview`: `data/exports/overview/prediction-audit-overview-20260313T035032Z`
- `publication_timing`: `data/exports/provenance/publication-timing-audit-20260313T035031Z`
- `cohort_comparison`: `data/exports/provenance/cohort-comparison-20260313T035031Z`
- `research_queue`: `data/exports/provenance/public-date-research-queue-20260313T035032Z`
- `unscored_queue`: `data/exports/unscored/unscored-prediction-queue-20260313T035033Z`
- `family/aviation_space`: `data/exports/aviation_space/stage4-aviation-space-20260313T145310Z`
- `family/earthquake`: `data/exports/earthquake/stage5-earthquake-20260313T145520Z`
- `family/epidemic`: `data/exports/epidemic/stage4-epidemic-20260313T161510Z`
- `family/politics_election`: `data/exports/politics/stage4-politics-20260313T161910Z`
- `family/storm`: `data/exports/storm/stage4-storm-20260313T031110Z`
- `family/volcano`: `data/exports/volcano/stage4-volcano-20260313T161710Z`


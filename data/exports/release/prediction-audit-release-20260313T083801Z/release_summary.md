# Prediction Audit Release Snapshot

- Generated at: `2026-03-13T08:38:01.447481+00:00`
- Stage 2 baseline: `stage2-20260313T024045Z`
- Script version: `prediction_audit_release_report_v1`

## Claimed-Date Baseline

- Candidate predictions parsed: `6797`
- Eligible predictions: `1226`
- Significant predictions: `316`
- Included scored rows: `127`
- Hits: `120` (`89` exact, `20` near, `11` similar-only)
- Misses: `7`
- Combined observed log10 probability sum: `-195.438683`

## Public-Date Separation

| Cohort | Rows | Hits | Exact | Misses | log10(sum p_obs) |
| --- | --- | --- | --- | --- | --- |
| public-date clean | 8 | 1 | 1 | 7 | -13.353612 |
| pending evidence | 22 | 22 | 16 | 0 | -28.765177 |
| publication conflicts | 97 | 97 | 72 | 0 | -153.319894 |

## Family Summary

| Family | Claimed Scored | Claimed Exact | Public Clean | Clean Exact | Pending | Conflicts |
| --- | --- | --- | --- | --- | --- | --- |
| aviation_space | 1 | 1 | 0 | 0 | 0 | 1 |
| earthquake | 61 | 34 | 5 | 0 | 11 | 45 |
| epidemic | 18 | 14 | 2 | 1 | 6 | 10 |
| politics_election | 21 | 20 | 0 | 0 | 0 | 21 |
| storm | 6 | 6 | 0 | 0 | 0 | 6 |
| volcano | 20 | 14 | 1 | 0 | 5 | 14 |

## Top Exact Hits

_Ranked by observed probability under each family's current null model. These family-specific nulls are not directly interchangeable._

| Report/Candidate | Family | Public-Date Status | p_obs | Observed Event |
| --- | --- | --- | --- | --- |
| 136/28 | earthquake | publication conflict | 5.16596e-05 | M 6.9 - 2 km N of Cairano, Italy |
| 465/5 | earthquake | publication conflict | 0.000154971 | M 7.9 - 58 km W of Tianpeng, China |
| 446/7 | earthquake | pending evidence | 0.000206622 | M 6.7 - 46 km NW of Nanao, Japan |
| 136/89 | earthquake | publication conflict | 0.000258271 | M 6.7 - 5 km SW of Domvraína, Greece |
| 400/7 | earthquake | pending evidence | 0.000258271 | M 7.6 - 21 km NNE of Muzaffar?b?d, Pakistan |
| 829/17 | politics_election | publication conflict | 0.000278203 | Death of Pope Emeritus Benedict XVI |
| 136/86 | earthquake | publication conflict | 0.000309917 | M 6.7 - 5 km SW of Domvraína, Greece |
| 459/5 | politics_election | publication conflict | 0.000314911 | Kosovo Declaration of Independence |
| 448/8 | storm | publication conflict | 0.000477373 | Greensburg tornado devastated the town |
| 442/4 | earthquake | publication conflict | 0.000516476 | M 6.0 - 181 km SW of Sagres, Portugal |
| 136/24 | earthquake | publication conflict | 0.000568108 | M 6.9 - 2 km N of Cairano, Italy |
| 136/26 | earthquake | publication conflict | 0.000568108 | M 6.9 - 2 km N of Cairano, Italy |
| 246/23 | earthquake | publication conflict | 0.000671366 | M 7.7 - 107 km W of Iwanai, Japan |
| 427/1 | earthquake | publication conflict | 0.000929464 | M 7.7 - 226 km SSW of Singaparna, Indonesia |
| 238/30 | earthquake | publication conflict | 0.00103268 | M 5.8 - The 1991 Sierra Madre, California Earthquake |

## Top Publication-Conflict Rows

| Rank | Report/Candidate | Family | Surprisal | Gap Bucket | Observed Event |
| --- | --- | --- | --- | --- | --- |
| 1 | 136/28 | earthquake | 4.286849 | deep_archive_gap | M 6.9 - 2 km N of Cairano, Italy |
| 2 | 465/5 | earthquake | 3.80975 | deep_archive_gap | M 7.9 - 58 km W of Tianpeng, China |
| 3 | 136/89 | earthquake | 3.587924 | deep_archive_gap | M 6.7 - 5 km SW of Domvraína, Greece |
| 4 | 829/17 | politics_election | 3.555638 | tiny_gap | Death of Pope Emeritus Benedict XVI |
| 5 | 136/86 | earthquake | 3.508754 | deep_archive_gap | M 6.7 - 5 km SW of Domvraína, Greece |
| 6 | 459/5 | politics_election | 3.501812 | deep_archive_gap | Kosovo Declaration of Independence |
| 7 | 448/8 | storm | 3.321143 | deep_archive_gap | Greensburg tornado devastated the town |
| 8 | 442/4 | earthquake | 3.28695 | large_gap | M 6.0 - 181 km SW of Sagres, Portugal |
| 9 | 136/24 | earthquake | 3.245569 | deep_archive_gap | M 6.9 - 2 km N of Cairano, Italy |
| 10 | 136/26 | earthquake | 3.245569 | deep_archive_gap | M 6.9 - 2 km N of Cairano, Italy |
| 11 | 246/23 | earthquake | 3.173041 | deep_archive_gap | M 7.7 - 107 km W of Iwanai, Japan |
| 12 | 427/1 | earthquake | 3.031767 | large_gap | M 7.7 - 226 km SSW of Singaparna, Indonesia |
| 13 | 238/30 | earthquake | 2.986032 | deep_archive_gap | M 5.8 - The 1991 Sierra Madre, California Earthquake |
| 14 | 402/5 | earthquake | 2.986032 | deep_archive_gap | M 4.4 - 2 km NNW of Brugg, Switzerland |
| 15 | 459/4 | politics_election | 2.971044 | deep_archive_gap | Apology to Australia's Indigenous Peoples |

## Open Queues

- Research queue rows: `97`. Current aligned export: `data/exports/provenance/public-date-research-queue-20260313T083743Z`
- Unscored queue rows: `1539`. Current aligned export: `data/exports/unscored/unscored-prediction-queue-20260313T083756Z`

| Rank | Report/Candidate | Family | Stage 2 Label | Recovery Bucket |
| --- | --- | --- | --- | --- |
| 1 | 238/85 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 2 | 238/182 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 3 | 238/186 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 4 | 238/195 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 5 | 252/2 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 6 | 383/5 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 7 | 589/4 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 8 | 687/12 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 9 | 718/25 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 10 | 786/7 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 11 | 788/8 | epidemic | significant_prediction | promote_via_existing_family_pipeline |
| 12 | 830/12 | epidemic | significant_prediction | promote_via_existing_family_pipeline |
| 13 | 833/3 | aviation_space | significant_prediction | promote_via_existing_family_pipeline |
| 14 | 136/58 | volcano | significant_prediction | promote_via_existing_family_pipeline |
| 15 | 182/24 | volcano | significant_prediction | promote_via_existing_family_pipeline |

## Source Exports

- `overview`: `data/exports/overview/prediction-audit-overview-20260313T083751Z`
- `publication_timing`: `data/exports/provenance/publication-timing-audit-20260313T083734Z`
- `cohort_comparison`: `data/exports/provenance/cohort-comparison-20260313T083739Z`
- `research_queue`: `data/exports/provenance/public-date-research-queue-20260313T083743Z`
- `unscored_queue`: `data/exports/unscored/unscored-prediction-queue-20260313T083756Z`
- `family/aviation_space`: `data/exports/aviation_space/stage4-aviation-space-20260313T145310Z`
- `family/earthquake`: `data/exports/earthquake/stage5-earthquake-20260313T083506Z`
- `family/epidemic`: `data/exports/epidemic/stage4-epidemic-20260313T073853Z`
- `family/politics_election`: `data/exports/politics/stage4-politics-20260313T074129Z`
- `family/storm`: `data/exports/storm/stage4-storm-20260313T071705Z`
- `family/volcano`: `data/exports/volcano/stage4-volcano-20260313T071705Z`


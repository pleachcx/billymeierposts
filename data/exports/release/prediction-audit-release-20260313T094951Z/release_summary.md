# Prediction Audit Release Snapshot

- Generated at: `2026-03-13T09:49:51.894514+00:00`
- Stage 2 baseline: `stage2-20260313T092015Z`
- Script version: `prediction_audit_release_report_v1`

## Claimed-Date Baseline

- Candidate predictions parsed: `6797`
- Eligible predictions: `1289`
- Significant predictions: `340`
- Included scored rows: `138`
- Hits: `127` (`98` exact, `20` near, `9` similar-only)
- Misses: `11`
- Combined observed log10 probability sum: `-213.51021`

## Public-Date Separation

| Cohort | Rows | Hits | Exact | Misses | log10(sum p_obs) |
| --- | --- | --- | --- | --- | --- |
| public-date clean | 10 | 1 | 1 | 9 | -13.39393 |
| pending evidence | 24 | 22 | 18 | 2 | -32.547468 |
| publication conflicts | 104 | 104 | 79 | 0 | -167.568811 |

## Family Summary

| Family | Claimed Scored | Claimed Exact | Public Clean | Clean Exact | Pending | Conflicts |
| --- | --- | --- | --- | --- | --- | --- |
| aviation_space | 1 | 1 | 0 | 0 | 0 | 1 |
| earthquake | 71 | 42 | 7 | 0 | 13 | 51 |
| epidemic | 18 | 14 | 2 | 1 | 6 | 10 |
| politics_election | 21 | 20 | 0 | 0 | 0 | 21 |
| storm | 7 | 7 | 0 | 0 | 0 | 7 |
| volcano | 20 | 14 | 1 | 0 | 5 | 14 |

## Top Exact Hits

_Ranked by observed probability under each family's current null model. These family-specific nulls are not directly interchangeable._

| Report/Candidate | Family | Public-Date Status | p_obs | Observed Event |
| --- | --- | --- | --- | --- |
| 136/28 | earthquake | publication conflict | 5.16596e-05 | M 6.9 - 2 km N of Cairano, Italy |
| 150/135 | earthquake | publication conflict | 5.16596e-05 | M 6.3 - 17 km N of Dham?r, Yemen |
| 416/1 | earthquake | pending evidence | 5.16596e-05 | M 7.6 - 80 km NE of Tilichiki, Russia |
| 465/5 | earthquake | publication conflict | 0.000154971 | M 7.9 - 58 km W of Tianpeng, China |
| 446/7 | earthquake | pending evidence | 0.000206622 | M 6.7 - 46 km NW of Nanao, Japan |
| 136/89 | earthquake | publication conflict | 0.000258271 | M 6.7 - 5 km SW of Domvraína, Greece |
| 400/7 | earthquake | pending evidence | 0.000258271 | M 7.6 - 21 km NNE of Muzaffar?b?d, Pakistan |
| 829/17 | politics_election | publication conflict | 0.000278203 | Death of Pope Emeritus Benedict XVI |
| 136/86 | earthquake | publication conflict | 0.000309917 | M 6.7 - 5 km SW of Domvraína, Greece |
| 459/5 | politics_election | publication conflict | 0.000314911 | Kosovo Declaration of Independence |
| 238/210 | earthquake | publication conflict | 0.000361561 | M 6.7 - 8 km W of Cimin, Turkey |
| 136/172 | earthquake | publication conflict | 0.00046484 | M 6.7 - 73 km SE of Kerman, Iran |
| 448/8 | storm | publication conflict | 0.000477373 | Greensburg tornado devastated the town |
| 442/4 | earthquake | publication conflict | 0.000516476 | M 6.0 - 181 km SW of Sagres, Portugal |
| 136/24 | earthquake | publication conflict | 0.000568108 | M 6.9 - 2 km N of Cairano, Italy |

## Top Publication-Conflict Rows

| Rank | Report/Candidate | Family | Surprisal | Gap Bucket | Observed Event |
| --- | --- | --- | --- | --- | --- |
| 1 | 136/28 | earthquake | 4.286849 | deep_archive_gap | M 6.9 - 2 km N of Cairano, Italy |
| 2 | 150/135 | earthquake | 4.286849 | deep_archive_gap | M 6.3 - 17 km N of Dham?r, Yemen |
| 3 | 465/5 | earthquake | 3.80975 | deep_archive_gap | M 7.9 - 58 km W of Tianpeng, China |
| 4 | 136/89 | earthquake | 3.587924 | deep_archive_gap | M 6.7 - 5 km SW of Domvraína, Greece |
| 5 | 829/17 | politics_election | 3.555638 | tiny_gap | Death of Pope Emeritus Benedict XVI |
| 6 | 136/86 | earthquake | 3.508754 | deep_archive_gap | M 6.7 - 5 km SW of Domvraína, Greece |
| 7 | 459/5 | politics_election | 3.501812 | deep_archive_gap | Kosovo Declaration of Independence |
| 8 | 238/210 | earthquake | 3.441819 | deep_archive_gap | M 6.7 - 8 km W of Cimin, Turkey |
| 9 | 136/172 | earthquake | 3.332696 | deep_archive_gap | M 6.7 - 73 km SE of Kerman, Iran |
| 10 | 448/8 | storm | 3.321143 | deep_archive_gap | Greensburg tornado devastated the town |
| 11 | 442/4 | earthquake | 3.28695 | large_gap | M 6.0 - 181 km SW of Sagres, Portugal |
| 12 | 136/24 | earthquake | 3.245569 | deep_archive_gap | M 6.9 - 2 km N of Cairano, Italy |
| 13 | 136/26 | earthquake | 3.245569 | deep_archive_gap | M 6.9 - 2 km N of Cairano, Italy |
| 14 | 246/23 | earthquake | 3.173041 | deep_archive_gap | M 7.7 - 107 km W of Iwanai, Japan |
| 15 | 427/1 | earthquake | 3.031767 | large_gap | M 7.7 - 226 km SSW of Singaparna, Indonesia |

## Open Queues

- Research queue rows: `104`. Current aligned export: `data/exports/provenance/public-date-research-queue-20260313T094941Z`
- Unscored queue rows: `1611`. Current aligned export: `data/exports/unscored/unscored-prediction-queue-20260313T094941Z`

| Rank | Report/Candidate | Family | Stage 2 Label | Recovery Bucket |
| --- | --- | --- | --- | --- |
| 1 | 238/20 | storm | significant_prediction | promote_via_existing_family_pipeline |
| 2 | 238/85 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 3 | 238/182 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 4 | 238/186 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 5 | 238/195 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 6 | 241/12 | epidemic | significant_prediction | promote_via_existing_family_pipeline |
| 7 | 252/2 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 8 | 376/12 | storm | significant_prediction | promote_via_existing_family_pipeline |
| 9 | 383/5 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 10 | 589/4 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 11 | 675/3 | epidemic | significant_prediction | promote_via_existing_family_pipeline |
| 12 | 688/10 | storm | significant_prediction | promote_via_existing_family_pipeline |
| 13 | 718/25 | politics_election | significant_prediction | promote_via_existing_family_pipeline |
| 14 | 731/18 | epidemic | significant_prediction | promote_via_existing_family_pipeline |
| 15 | 748/17 | epidemic | significant_prediction | promote_via_existing_family_pipeline |

## Source Exports

- `overview`: `data/exports/overview/prediction-audit-overview-20260313T094941Z`
- `publication_timing`: `data/exports/provenance/publication-timing-audit-20260313T094941Z`
- `cohort_comparison`: `data/exports/provenance/cohort-comparison-20260313T094941Z`
- `research_queue`: `data/exports/provenance/public-date-research-queue-20260313T094941Z`
- `unscored_queue`: `data/exports/unscored/unscored-prediction-queue-20260313T094941Z`
- `family/aviation_space`: `data/exports/aviation_space/stage4-aviation-space-20260313T094808Z`
- `family/earthquake`: `data/exports/earthquake/stage5-earthquake-20260313T094155Z`
- `family/epidemic`: `data/exports/epidemic/stage4-epidemic-20260313T094908Z`
- `family/politics_election`: `data/exports/politics/stage4-politics-20260313T094027Z`
- `family/storm`: `data/exports/storm/stage4-storm-20260313T092542Z`
- `family/volcano`: `data/exports/volcano/stage4-volcano-20260313T092459Z`


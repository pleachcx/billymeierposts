# Billy Meier Contact Reports Prediction Audit — Staged Protocol

## Objective
This protocol separates the work into distinct stages so the system does not jump from a text fragment to a claimed “hit” too quickly. The purpose is to create an auditable pipeline:

1. **Stage 1 — Find candidate predictions**
2. **Stage 2 — Decide whether the candidates are meaningful and measurable**
3. **Stage 3 — Build the event ledger of what actually occurred**
4. **Stage 4 — Match predictions to events and score them**
5. **Stage 5 — Estimate how unusual the results are under baseline models**

The current request focuses on Stages 1–3. Those stages should be run and frozen before final matching/scoring.

---

## Guiding principle
At no stage should the system be allowed to “save” a vague statement by looking ahead to a later event. The pipeline must move in one direction:

**text -> candidate prediction -> eligible prediction -> event search -> match decision**

not

**text -> famous event -> reinterpret text to fit the event**

---

# Global definitions

## Prediction
A statement counts as a **prediction candidate** if all of the following are true:

1. It asserts or implies a **future** event, condition, or state of affairs relative to the date of public availability.
2. It refers to something that is in principle **observable in the real world**.
3. It can be reduced to an explicit proposition of the form:
   - **something will happen**,
   - **something will not happen**,
   - **a condition will arise**, or
   - **a trend/change will occur**.

A statement is **not** a prediction candidate if it is only:

- a description of the present or past,
- a moral warning with no observable outcome,
- symbolic or mystical language with no agreed objective referent,
- purely conditional language whose triggering condition is not operationalizable,
- a prophecy that is already known publicly to have happened before publication,
- or a statement so broad that nearly any future can be made to fit it.

## Eligible prediction
A candidate becomes an **eligible prediction** only after Stage 2 if it is both:

- **meaningful**: substantive enough that success would matter,
- **measurable**: sufficiently precise that miss conditions exist.

## Similar event
A **similar event** is a real-world event that belongs to the same event family as the prediction but does not meet all conditions for an exact hit.

Similar events are divided into:

- **near hit**: close enough on the pre-registered dimensions to deserve partial credit,
- **log-only similar event**: relevant enough to record for transparency, but not close enough for scoring.

Similar events must never be re-labeled as exact hits after review.

---

# Stage 0 — Corpus freeze and provenance

Before any extraction starts, freeze the source corpus.

## Inputs
- Raw contact reports
- Metadata on report number, claimed contact date, publication date, language, edition, translator, source URL/file

## Required provenance fields
For each report store:

- `report_id`
- `report_title`
- `claimed_contact_date`
- `earliest_provable_public_date`
- `edition_or_translation`
- `source_path`
- `source_hash`
- `language`
- `translator`
- `provenance_notes`

## Rule
For validity, the operative reference date is:

**earliest provable public date**

not merely the alleged internal conversation date.

This does not forbid recording the claimed contact date; it means the public date is the default date for evaluation.

---

# Stage 1 — Find candidate predictions

## Goal
Extract every sentence or passage that could plausibly be read as a future-oriented claim, without yet deciding whether it is strong, meaningful, measurable, or true.

## Stage 1 question
> “Does this passage contain a claim about a future observable event or state of the world?”

## Stage 1 must not do
Stage 1 must **not**:

- decide whether the claim is impressive,
- decide whether it came true,
- search for confirming events,
- inflate metaphors into concrete forecasts,
- combine multiple vague fragments into one stronger statement,
- or silently discard ambiguous but plausible candidates.

## Extraction rules
A passage should be extracted as a candidate if it contains any of the following:

- explicit future tense,
- modal future language (“will,” “shall,” “is going to,” “can be expected to”),
- warnings clearly framed as future occurrence,
- trend forecasts that imply a future state,
- future negative claims (“X will not happen”),
- sequence claims (“after X, Y will occur”).

## Stage 1 output classes
Each extracted item must be assigned one of these provisional classes:

1. **Discrete event**
   - earthquake, war, assassination, crash, epidemic, eruption, invention, etc.
2. **State change**
   - collapse, unification, regulation, social deterioration, environmental decline, etc.
3. **Trend claim**
   - increase/decrease in some phenomenon over time.
4. **Conditional future claim**
   - “if A occurs, B will follow.”
5. **Ambiguous future claim**
   - extracted for review, but likely to fail Stage 2.

## Candidate extraction unit
The extraction unit is the **smallest self-contained future claim**. Compound sentences must be split into atomic claims whenever possible.

Example:
> “There will be severe earthquakes in California and later a great storm will devastate part of the coast.”

must become two candidates, not one.

## Stage 1 JSON schema
```json
{
  "report_id": "CR-0251",
  "candidate_id": "CR-0251-P001",
  "source_quote": "...",
  "source_start_offset": 1204,
  "source_end_offset": 1342,
  "future_claim_present": true,
  "candidate_class": "discrete_event",
  "claim_normalized": "A severe earthquake will occur in California.",
  "event_family_provisional": "earthquake",
  "time_text": "in the coming years",
  "location_text": "California",
  "actor_text": null,
  "magnitude_text": "severe",
  "conditionality": "none",
  "ambiguity_flags": ["vague_time"],
  "extractor_confidence": 0.78,
  "notes": "Extracted as candidate only; not yet evaluated."
}
```

## Stage 1 decision standard
Bias toward **capturing** possible future claims rather than rejecting them early.

Reason: Stage 1 is recall-oriented.

## Stage 1 human review
Human review should remove only obvious non-predictions, such as:

- pure retrospective narration,
- obvious formatting errors,
- duplicate extraction of the same sentence,
- metaphor with no plausible real-world referent.

## Stage 1 outputs
At the end of Stage 1, freeze:

- full candidate list,
- excluded-nonprediction list,
- extraction prompt version,
- corpus version.

No event matching is allowed before this freeze.

---

# Stage 2 — Are these predictions meaningful and measurable?

## Goal
Filter candidate predictions into a smaller set of **eligible predictions** that are substantive enough to matter and precise enough to test.

## Stage 2 questions
1. Is the candidate **meaningful**?
2. Is the candidate **measurable**?
3. Is the candidate **significant** enough to include in the main score?

These are separate questions.

---

## 2A. Meaningful
A candidate is **meaningful** if it predicts something more than a trivial, inevitable, or essentially universal possibility.

### Meaningful = yes if at least one is true
- The event would be socially, politically, scientifically, or physically consequential.
- The event is uncommon enough that being right would be noteworthy.
- The statement includes a distinctive mechanism, target, actor, location, or severity.
- The claim narrows the future in a nontrivial way.

### Meaningful = no if most of the following apply
- It predicts a broad human constant (“there will be conflict,” “people will lie,” “governments will deceive”).
- It describes an open-ended deterioration with no threshold.
- It is compatible with nearly all futures.
- It is too generic to distinguish success from background noise.

### Meaningfulness scoring
Use a 0–3 score:

- **0** = trivial / universal / empty
- **1** = weakly substantive
- **2** = moderately substantive
- **3** = strongly substantive / notable if true

A candidate must score **>= 2** to count as meaningful.

---

## 2B. Measurable
A candidate is **measurable** if independent reviewers could later determine whether it hit, nearly hit, or missed using explicit criteria.

### Measurable dimensions
Evaluate the presence of the following testable dimensions:

1. **Event family**
   - what kind of event/state is being predicted?
2. **Time**
   - when is it supposed to happen?
3. **Location or actor**
   - where or to whom?
4. **Magnitude / severity / mechanism / distinctive feature**
   - how large, what type, what special characteristic?

### Measurability scoring
Give 1 point for each dimension that is sufficiently specified.

- **0** = not measurable
- **1** = too vague
- **2** = weakly testable
- **3** = testable
- **4** = strongly testable

A candidate must score **>= 3** to count as measurable.

### Rule for missing time
If no time information exists at all, the claim is normally **not measurable** for the main analysis.

Possible exception:
- If the claim is otherwise exceptionally specific and rare, it may be kept in a secondary “weak timing” analysis, but not in the main score.

---

## 2C. Provenance
A candidate must also pass a provenance screen.

### Provenance scoring
- **0** = public date unknown / unreliable
- **1** = public date plausible but weakly evidenced
- **2** = public date well evidenced

Main analysis requires provenance **>= 1**.
Sensitivity analysis may restrict to provenance **= 2** only.

---

## 2D. Significant prediction
A candidate becomes a **significant prediction** if:

- meaningfulness score **>= 2**
- measurability score **>= 3**
- provenance score **>= 1**

This is the default threshold for the main scoring set.

---

## 2E. Rulebook for similar events
Stage 2 must define, in advance, what counts as exact vs near vs log-only for each event family.

This is essential because similar events must be captured without letting them blur into “hits.”

### General structure
For each event family, define:

- exact-match requirements,
- near-hit tolerances,
- log-only tolerances,
- and disqualifying mismatches.

The dimensions are usually:

- time,
- geography,
- event family,
- severity/magnitude,
- actor/target,
- mechanism.

---

## 2F. Example rulebook — Earthquakes
This is the model example requested.

### Prediction template
> “An earthquake will occur at or near location X within time window T, optionally with magnitude/severity M.”

### Definitions
- **Target point** = the named location converted to coordinates.
- **Target polygon** = if the prediction names a region/city/country boundary rather than a point.
- **Event family** = earthquake.
- **Reference catalog** = chosen seismic event database fixed before matching.

### Default minimum event threshold
If no magnitude/severity is given, count only earthquakes that are:

- **Mw >= 5.5**, or
- below 5.5 but associated with documented damage, injuries, or unusual salience.

Reason: otherwise routine small quakes will create false “hits.”

### Exact hit rule
An earthquake counts as an **exact hit** only if all apply:

1. same event family,
2. event date inside the exact time window,
3. epicenter inside the named polygon or within **25 km** of the target point,
4. if magnitude stated numerically, actual magnitude within **±0.5 Mw**,
5. if severity stated verbally (“strong,” “severe,” “devastating”), actual event matches the mapped severity band.

### Near hit rule
An earthquake counts as a **near hit** if all apply:

1. same event family,
2. event date inside exact time window **or** pre-registered near-time grace,
3. epicenter is more than 25 km and up to **50 km** from target point, or just outside the named polygon but clearly adjacent,
4. magnitude within **±1.0 Mw**, or one mapped severity band away.

### Log-only similar event rule
Record as **log-only similar event** if any of the following apply:

- distance is **> 50 km and <= 100 km**,
- correct family and rough region but outside near-hit tolerance,
- correct family and location but magnitude far off,
- correct family and severity but outside the near-time grace.

These events are transparent context only. They do not count toward the score.

### Earthquake disqualifiers
Not a hit or near hit if:

- wrong event family,
- event is below minimum threshold and not salient,
- event is too far outside the spatial band,
- time miss exceeds near-time grace,
- prediction names a distinctive mechanism or magnitude clearly contradicted by the actual event.

---

## 2G. Time normalization rules
Every natural-language time phrase must be converted to a fixed machine-readable interval.

### Default lexicon
- “soon” -> 0 to 2 years
- “in the near future” -> 0 to 5 years
- “in the coming years” -> 0 to 10 years
- “before long” -> 0 to 3 years
- “in this century” -> remainder of that century from publication date

If the text provides a more explicit date, that overrides the lexicon.

### Near-time grace
Near hits may use a limited time grace:

- day-specific claims: **±7 days**
- month-specific claims: **±1 month**
- year-specific or multi-year windows: **25% of window length on either side**, capped at **1 year**

This grace is for near hits only, never exact hits.

---

## 2H. Duplicate and restatement rule
If essentially the same prediction appears in multiple reports, count it as one **prediction family** unless later versions add materially new constraints.

### Same prediction family if
- same event family,
- same target/location or actor,
- same broad time window,
- no substantial new specificity.

### New family if
- later wording adds a new narrow time window,
- names a different actor or location,
- adds a distinctive mechanism or magnitude,
- or turns a trend warning into a discrete event claim.

Stage 2 must freeze duplicate-family assignments before Stage 3 event search is finalized.

---

## Stage 2 output labels
Each candidate must end Stage 2 as one of:

- `not_a_prediction`
- `prediction_but_not_meaningful`
- `prediction_but_not_measurable`
- `prediction_with_weak_provenance`
- `eligible_prediction`
- `significant_prediction`
- `duplicate_restating_prior_prediction`

## Stage 2 JSON schema
```json
{
  "candidate_id": "CR-0251-P001",
  "meaningfulness_score": 2,
  "measurability_score": 3,
  "provenance_score": 2,
  "event_family_final": "earthquake",
  "time_window_start": "1980-01-01",
  "time_window_end": "1990-12-31",
  "target_type": "point",
  "target_name": "San Francisco",
  "target_lat": 37.7749,
  "target_lon": -122.4194,
  "severity_band": "strong",
  "eligible": true,
  "significant": true,
  "prediction_family_id": "PF-0044",
  "similarity_rulebook": "earthquake_v1",
  "review_notes": "Measurable but timing remains broad."
}
```

## Stage 2 outputs
Freeze:

- eligible prediction set,
- significant prediction set,
- duplicate family map,
- event-family rulebooks,
- time normalization lexicon,
- scoring thresholds.

Only after this freeze can Stage 3 proceed.

---

# Stage 3 — What events occurred?

## Goal
Construct an external event ledger of real-world events that could match the frozen eligible predictions.

## Stage 3 question
> “Given the pre-registered event family, time window, location, actor, and severity rules, what events actually occurred?”

## Stage 3 must not do
Stage 3 must **not**:

- revise the prediction text,
- widen the rulebook because no exact match was found,
- search opportunistically for “kind of similar” famous events only,
- or ignore non-matching evidence.

## Stage 3 process
For each eligible prediction:

1. Read the frozen Stage 2 representation.
2. Query the reference event sources appropriate to that event family.
3. Pull every candidate event inside the exact and near-hit search bands.
4. Store them in an event ledger.
5. Do not classify exact/near/miss yet unless Stage 4 explicitly does so.

## Event ledger principles
The event ledger should be broader than the exact hit set. It should include:

- events satisfying the exact criteria,
- events satisfying near-hit criteria,
- log-only similar events,
- and explicitly empty results where nothing relevant was found.

This prevents invisible cherry-picking.

---

## 3A. Reference sources
Each event family must use fixed reference sources chosen before search begins.

Examples:
- earthquakes -> a chosen seismic catalog
- wars/conflicts -> chosen conflict dataset and reputable historical sources
- epidemics -> chosen public health datasets and official records
- aviation/space incidents -> chosen official investigative or catalog sources

The source list must be frozen and versioned.

---

## 3B. Search bands
For each eligible prediction, Stage 3 should compute three nested search bands:

1. **Exact search band**
2. **Near-hit search band**
3. **Log-only search band**

### Example for earthquake prediction
If the claim names a point location:

- exact spatial band: `0–25 km`
- near-hit band: `>25–50 km`
- log-only band: `>50–100 km`

Time bands:

- exact = frozen time window
- near = exact + near-time grace
- log-only = optionally one further context band if pre-registered

Magnitude band:

- exact = stated magnitude ±0.5 Mw, else default threshold
- near = stated magnitude ±1.0 Mw, else one severity band off

---

## 3C. Event schema
```json
{
  "event_id": "EQ-1989-001",
  "event_family": "earthquake",
  "source_system": "reference_catalog_v1",
  "event_title": "Loma Prieta earthquake",
  "date_start": "1989-10-17",
  "date_end": "1989-10-17",
  "lat": 37.036,
  "lon": -121.883,
  "location_text": "Northern California",
  "magnitude_value": 6.9,
  "severity_band": "major",
  "actors": [],
  "description": "Major earthquake in Northern California",
  "source_refs": ["..."],
  "ingest_notes": "Imported from fixed reference catalog"
}
```

---

## 3D. Prediction-event candidate link table
Stage 3 should create a link table showing every event that falls into exact, near, or log-only search space.

```json
{
  "prediction_family_id": "PF-0044",
  "candidate_id": "CR-0251-P001",
  "event_id": "EQ-1989-001",
  "band": "near_or_exact_candidate",
  "distance_km": 42.7,
  "time_offset_days": 120,
  "magnitude_delta": 0.7,
  "reason_logged": "Same family, within near spatial band, within frozen time rule"
}
```

At this stage the row is a **candidate link**, not yet a final classification.

---

## 3E. Empty-result logging
If no event is found in the log-only search band, record that explicitly.

Example:
```json
{
  "prediction_family_id": "PF-0044",
  "search_status": "no_relevant_events_found",
  "exact_band_checked": true,
  "near_band_checked": true,
  "log_band_checked": true,
  "notes": "No earthquakes above threshold in bands during window"
}
```

This is essential for transparency and later miss classification.

---

## Stage 3 outputs
Freeze:

- event ledger,
- prediction-event candidate links,
- source versions,
- empty-result logs,
- unresolved-source or ambiguity notes.

Only after this freeze should Stage 4 perform final hit / near / miss adjudication.

---

# Recommended operating sequence

## Pass 1
Run Stage 1 over all reports and freeze candidates.

## Pass 2
Run Stage 2 over all candidates and freeze eligible/significant predictions plus rulebooks.

## Pass 3
Run Stage 3 to build the event ledger for all eligible predictions and freeze it.

## Pass 4
Only then classify:
- exact hits
- near hits
- misses
- untestable / unresolved

## Pass 5
Compute probabilities and baseline comparisons.

---

# Minimal success criteria for each stage

## Stage 1 success
- High recall of future-oriented passages
- No truth assessment
- No event lookup

## Stage 2 success
- Clear yes/no rules for meaningfulness and measurability
- Pre-registered “similar event” tolerances
- Duplicate families resolved

## Stage 3 success
- Event search performed against frozen criteria
- Exact/near/log-only bands recorded
- Empty results logged as carefully as positive results

---

# Recommended folders

```text
/project
  /raw_reports
  /normalized_reports
  /stage1_candidates
  /stage2_eligibility
  /rulebooks
  /event_ledgers
  /stage3_candidate_links
  /frozen_runs
  /audit_logs
```

---

# Recommended first implementation milestone

A practical first milestone is:

1. Choose 50 reports
2. Run Stage 1 only
3. Review extraction quality and refine prompts
4. Run Stage 2 on those 50 reports
5. Lock the earthquake rulebook first
6. Run Stage 3 only for earthquake-family predictions

This will expose the real ambiguity before scaling to all 900+ reports.

---

# Key audit rule
At every stage, keep both:

- the machine-readable structured output,
- and the original quote with offsets.

No structured claim is valid unless it can be traced back to an exact passage in the source text.

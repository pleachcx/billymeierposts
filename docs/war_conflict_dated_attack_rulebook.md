# War Conflict Dated-Attack Rulebook

## Scope

This first `war_conflict` slice is limited to atomic predictions about dated attack events that can be checked against one public event record without geopolitical interpretation.

Included rows must have all of the following:

- an explicit day or a tightly bounded post-anchor window already normalized in Stage 2
- a concrete attack form such as bombing, coordinated terror attack, armed raid, or vehicle attack
- a named place, target cluster, or victim cluster that can be matched without inventing a second family
- one defensible public source entry that captures the event date and the relevant attack description

This slice does not score:

- generic war, world-war, Cold War, or ideology claims
- broad regional turmoil or open-ended terrorism trends
- retaliation, executions, policing, or follow-on political consequences unless the attack event itself is the scoped prediction
- rows that require rewriting the stored Stage 2 chronology to reach the only plausible match

## Match Dimensions

Every row is matched on the same dimensions:

- `event_type`: the narrow attack class defined in the override
- `jurisdiction`: country or named region used by the curated catalog
- `target_keywords`: place, institution, transport system, or target cluster
- `window_start` and `window_end`: the normalized Stage 2 time window, optionally narrowed in the override

The matcher must also reject any event dated before `claimed_contact_date`.

## Outcome Rules

`exact_hit`

- catalog event type matches exactly
- jurisdiction matches exactly
- target keywords match
- event date lands inside the override window

`near_hit`

- same event type, jurisdiction, and target cluster
- event date is within 7 days of the override window

`similar_only`

- same scoped attack family and target cluster
- but timing falls outside the exact or near window, or one distinctive dimension is only partial
- log for transparency only; do not upgrade later

`miss`

- no curated event in the scoped catalog meets the same attack type and target cluster well enough to count as exact, near, or similar-only

`permanently_unresolved`

- use only when the row cannot be scored objectively under the stored baseline
- current retirement triggers in this slice:
- the corpus chronology points to one exact day, but the only plausible real-world analogue requires overriding that stored day or year
- the stored claim stays at the level of world war, Cold War, regime change, retaliation trend, ceasefire, military ceremony, or other non-attack framing that cannot be reduced to one atomic dated attack without changing the claim itself
- the row was mis-grouped into `war_conflict` even though the stored event family is geologic or otherwise outside the attack catalog

## Source Catalog

The checked-in source catalog for this slice is [data/war_conflict_official_events.json](/home/pl/apps/billymeierposts/data/war_conflict_official_events.json).

Catalog policy:

- one entry per public event used by the slice
- use institutional or otherwise stable public sources where possible
- keep excerpts short and factual
- do not infer hidden actors, motives, or casualty revisions beyond what the source supports

## Initial Target Manifest

This first replay tranche is limited to:

- `136:170` IRP headquarters bombing on 1981-06-28
- `136:202` Tehran leadership bombing after the first Tehran attack
- `206:5` 2001-09-11 mass-casualty terror attack in America
- `393:8` 2005-07-07 London transport bombings
- `394:3` follow-on London attack row with a stored 2004 exact-day window
- `400:1` 2005-10-01 Bali bombings
- `400:21` 2005-10-13 Nalchik raid
- `668:2` 2016-12-19 Berlin holiday-market vehicle attack

## Row-Specific Notes

- `394:3` stays in-slice for transparency, but the final review must not "repair" the stored `2004-07-21` window to `2005-07-21`. If the similar 2005 attempted London bombings remain the only plausible analogue, retire the row from statistics.
- `668:2` scopes only the Berlin market attack clause from the broader three-crimes sentence.
- `136:202` scopes only the second Tehran attack that killed the president and a minister; the first attack belongs to `136:170`.

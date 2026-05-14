---
name: add-adr
description: Walk through creating a new Architecture Decision Record for the eventstore framework — numbering, status convention, supersedes/amends back-refs, MADR sections, index update. Use when the user wants to write a new ADR, propose a design decision, or document an architectural change.
---

# add-adr

Interactive helper for adding a well-formed ADR to `docs/adr/`. ADRs in
this repo are immutable once Accepted — if a decision changes, write a
new ADR that **supersedes** the old. This skill enforces the existing
conventions.

The authoritative reference is [docs/adr/README.md](../../../docs/adr/README.md).
Read it once; treat this skill as the interactive companion for
materializing the file.

## When this skill applies

- User wants to write a new ADR
- User has made an architectural decision and wants to document it
- User is proposing a change to a previous decision (supersedes/amends)
- User is reconsidering or deferring an existing ADR

If the user is asking *what* the ADRs say (a lookup question), point
them at [docs/adr/README.md](../../../docs/adr/README.md) instead of
writing anything new.

## What to gather first

Ask one question at a time, not a checklist:

1. **Title.** Short noun phrase naming the decision. Examples:
   "Crypto-Shredding", "Workflow-Orchestrated Command Bus",
   "state_cache Subsumes Snapshots". Will become both the heading
   and (slugified, lowercase, hyphen-separated) the filename.
2. **Status.** One of:
   - `Proposed` — under discussion; can still change
   - `Accepted` — the decision is in effect
   - `Deferred` — known relevant but not now (with a one-line "until X")
3. **Relationships.** Does this ADR:
   - **Supersede** an earlier ADR? (The old one becomes immutable
     archaeology; new code follows the new one.)
   - **Amend** an earlier ADR? (Refines without invalidating — e.g.
     ADR 0019 amends 0017 + 0018.)
   - **Pair with** another ADR? (Two ADRs that must be read together
     to make sense — e.g. ADR 0026 pairs with 0012 + 0025.)
4. **Context.** What problem is this decision responding to? What
   constraints are in play? (Get prose, not bullets — the ADR tone
   is narrative.)
5. **The decision itself.** One sentence first, then the supporting
   detail. If the user is giving you three decisions, push back: one
   decision per ADR is the convention.
6. **Alternatives considered.** What other options were on the table,
   and why they were rejected. A future maintainer must be able to
   reconstruct this without asking.
7. **Consequences.** What this commits us to: positive, negative, and
   neutral-but-load-bearing facts.

## Step 1 — Compute the next number

ADRs are zero-padded four digits. Find the next number:

```bash
ls docs/adr/ | grep -E '^[0-9]{4}-' | sort -r | head -1
```

If the highest is `0026-foo.md`, the new one is `0027`. **Never reuse
numbers**, including for superseded ADRs — they keep their slot.

## Step 2 — Build the filename

```
docs/adr/<NNNN>-<slug>.md
```

Where `<slug>` is the title:
- lowercase
- spaces → hyphens
- drop punctuation
- max ~5 words; if longer, summarize

Examples from history:
- "state_cache Subsumes Snapshots" → `0023-state-cache-supersedes-snapshots.md`
- "Workflow-Orchestrated Command Bus" → `0025-workflow-orchestrated-command-bus.md`

## Step 3 — Write the file

Use this skeleton. Keep the header block compact; metadata lines only
when they apply.

```markdown
# ADR <NNNN>: <Title>

- **Status:** <Proposed | Accepted | Deferred (until ...)>
- **Date:** <YYYY-MM-DD>
- **Supersedes:** ADR <MMMM> (<Title>)        ← only if applicable
- **Amends:** ADR <MMMM> (<Title>)            ← only if applicable
- **Pairs with:** ADR <X>, ADR <Y>            ← only if applicable

## Context

<Prose. What problem, what constraints, what's already been tried.
Multiple paragraphs are fine. No "TL;DR" — the title is the TL;DR.>

## Decision

<One-sentence statement of the decision, then the supporting detail.
Break into numbered subsections if there are sub-decisions all tied to
the same axis — see ADR 0026 for the model.>

## Alternatives considered

<Each alternative gets a paragraph: what it would have looked like,
and why it was rejected. Don't strawman — capture the real version.>

## Consequences

<Bulleted is OK here. What this commits us to, what costs we accept,
what's now harder, what's now easier.>
```

If you're stuck on tone, copy the structure of the closest existing
ADR (e.g., for a runtime decision, look at ADR 0025; for a data-model
decision, ADR 0008).

## Step 4 — Wire the back-refs

If this ADR supersedes ADR `<MMMM>`, **also update that ADR's header**:

```markdown
- **Status:** Superseded by ADR <NNNN> (<short reason>)
```

The reason matters — a future reader looking at the old ADR should
immediately understand why it's archived. Example from ADR 0011:

```markdown
- **Status:** Superseded by ADR 0023 (state_cache absorbs the snapshot role)
```

If amending, no back-ref is required — but a one-line "See also: ADR
<NNNN>" at the bottom of the amended ADR is courteous.

## Step 5 — Update the README index

Edit [docs/adr/README.md](../../../docs/adr/README.md). The table is
sequential by number; insert the new row. Match the existing column
widths for diff cleanliness.

```markdown
| <NNNN> | [<Title>](./<NNNN>-<slug>.md)                                | <Status>   |
```

If the new ADR supersedes an existing one, **also update the old row**
in the index so its status reads `Superseded by ADR <NNNN>`.

## Style notes

- **Prose, not bullets.** Context and Decision sections read as
  narrative. Bullets in Consequences only.
- **One decision per ADR.** If you find yourself writing "Decision 1,
  Decision 2, Decision 3" about unrelated axes, split into multiple
  ADRs. Sub-decisions on the *same* axis (e.g., the four sub-decisions
  in ADR 0026 all about workflow adapters) belong together.
- **Capture the why.** A future maintainer should be able to challenge
  the decision and find that the obvious counter-argument is already
  addressed in Alternatives.
- **Cite ADRs by number.** "Per ADR 0007" not "per the multi-tenancy
  ADR" — numbers are stable, titles drift.
- **No emojis.** Match the existing house style.

## After writing

- Run `task lint:proto` if the ADR touches proto schema.
- If the ADR is `Accepted` and implies code changes, surface them as
  follow-up tasks for the user to schedule — the ADR itself stays as
  the design record; implementation is tracked separately.

---
name: add-recipe
description: Walk through creating a new cookbook recipe for the eventstore framework — problem statement, primitives used, primitives deliberately NOT used, failure modes, index update. Use when the user wants to write a new cookbook recipe, document an application pattern, or add a how-to guide.
---

# add-recipe

Interactive helper for adding a well-formed recipe to `docs/cookbook/`.
The cookbook is the framework's answer to "where do I learn how to
*apply* the primitives" — sagas, process managers, deployment shapes,
HTTP edges, schema migrations. **Nothing in the cookbook is baked into
the framework**; every recipe uses only public API.

The authoritative reference is [docs/cookbook/README.md](../../../docs/cookbook/README.md).
Read it once; treat this skill as the interactive companion.

## When this skill applies

- User has a working pattern they want to publish
- User wants to document a coordination shape (saga, process manager, retry policy)
- User wants to document a deployment or operational topology
- User is responding to a recurring question from contributors with "this should be a recipe"

If the user is asking *what* a recipe says (lookup), point them at
[docs/cookbook/README.md](../../../docs/cookbook/README.md). If the
pattern is so general it deserves to live in the framework instead of
the cookbook, push back — most "the framework should provide X"
turns out to be a five-line subscriber.

## What to gather first

Ask one question at a time:

1. **The problem.** One sentence. "When event X happens, dispatch
   command Y" / "Cancel order after 24h with no payment" /
   "Expose the bus over HTTP". If the user can't compress to one
   sentence, the recipe is probably two recipes.
2. **What primitives the pattern uses.** Subscribers? `cmdworkflow.Workflow`?
   `state_cache`? Outbox? Linked projections? Crypto-shredding? The
   "primitives used" framing is structural — readers should be able
   to scan it and know whether the recipe is relevant.
3. **What primitives the pattern deliberately does NOT use.** This is
   the most valuable section in any cookbook recipe and the one most
   often skipped. Example: "deliberately not using framework snapshots
   — use `state_cache` (ADR 0023) instead." It signals to the reader
   "I considered X and chose not to" rather than "I didn't know about X."
4. **The smallest working example.** Code that compiles. Ideally
   reusing an existing aggregate from `examples/` rather than
   inventing a new domain — the reader shouldn't have to learn
   `MagicalWidget` to follow the pattern.
5. **Failure modes.** What happens on retry, on partial failure, on
   bus delivery duplication, on schema mismatch, on tenant context
   missing. Spell out the *expected* behavior under each.
6. **Relevant ADRs.** Which architectural decisions the recipe lands
   on top of (e.g., a saga recipe lands on ADR 0015; a state-cache
   recipe lands on ADR 0023). Cite by number.

## Step 1 — Compute the next number

Recipes are sequential, two-digit, zero-padded:

```bash
ls docs/cookbook/ | grep -E '^[0-9]{2}-' | sort -r | head -1
```

If the highest is `15-http-edge-with-connect.md`, the new one is `16`.
Never reuse numbers, including for retired recipes (mark them
deprecated in the README index instead of recycling the slot).

## Step 2 — Build the filename

```
docs/cookbook/<NN>-<slug>.md
```

`<slug>` rules:
- lowercase
- spaces → hyphens
- drop articles ("the", "a")
- max ~5 words; if longer, summarize

Examples from history:
- "Stateful saga" → `02-stateful-saga.md`
- "Time-based triggers" → `04-time-based-triggers.md`
- "HTTP edge with Connect-go" → `15-http-edge-with-connect.md`

## Step 3 — Write the file

Use this skeleton:

```markdown
# <NN>: <Title>

<One- or two-paragraph statement of the problem and what the recipe
solves. The reader should know within 30 seconds whether this is
relevant to them.>

## What it shows

- The primitives the recipe demonstrates
- The shape of the pattern (subscriber? saga? handler chain?)
- Where the code lives (which `examples/` directory)

## What it deliberately does NOT show

- Framework primitive X — why this recipe doesn't use it
- Common-but-wrong approach Y — why it's wrong here
- (Skipping this section is the #1 way recipes age badly. Be explicit.)

## The pattern

<Code. Minimal, working, compiling. If the snippet is more than ~40
lines, link to the file in `examples/` instead of inlining it.>

<Prose explaining the load-bearing decisions: why this subscriber's
Mode is Sync vs Async, why this OnExhausted is DLQ vs Compensate,
why the idempotency key is derived this way.>

## Failure modes

| Failure                    | What happens / what to do                   |
| -------------------------- | ------------------------------------------- |
| Subscriber Handle panics   | ...                                         |
| Retry exhaustion           | ...                                         |
| Bus duplicate delivery     | ...                                         |
| Tenant missing from ctx    | ErrTenantMissing — caller must set it       |
| Schema-version mismatch    | Upcaster chain (ADR 0013) handles it / ...  |

## When NOT to use this recipe

<Brief. The other side of "what it deliberately does NOT show."
Example: "If your edge is a message queue trigger, you call
bus.HandleCmd directly — this recipe adds nothing."

This section saves readers from cargo-culting.>

## Reference

- ADR <NNNN> — <why this design exists>
- Related recipes: cookbook <NN>, <NN>
- Worked example: [examples/<dir>/](../../../examples/<dir>/)
```

## Step 4 — Ship a worked example (recommended)

The strongest recipes pair with a runnable directory under `examples/`:

- Has its own `go.mod` (so reader can `cd && go test ./...` without
  spelunking the monorepo)
- Imports the framework via local `replace` per the existing
  examples pattern
- Tests cover the happy path + at least one failure mode the recipe
  documents
- README that explains what it shows and what it deliberately doesn't
  (same framing as the recipe)

If shipping an example, add it to `go.work` and to the cookbook recipe
as a "Worked example" link. Examples are not published modules — they
go in `go.work` but not in `scripts/release.sh`'s MODULES list.

## Step 5 — Update the README index

Edit [docs/cookbook/README.md](../../../docs/cookbook/README.md). The
table is sequential by number; append the new row:

```markdown
| <NN> | [<Title>](./<NN>-<slug>.md) | <One-sentence problem + key primitives>. |
```

Keep the "What it solves" column under ~25 words. Readers scan this
column to find recipes; verbose entries make the index useless.

## Style notes

- **Problem first.** Don't open with "in this recipe we will explore."
  Open with the problem the reader has.
- **One pattern per recipe.** If you're tempted to add "and also you
  can do Y," split. Cross-link recipes; don't merge them.
- **Be explicit about what's NOT in framework.** The cookbook exists
  because the framework deliberately stays small. Recipes are the
  contract: "we chose not to bake X in; here's how to do X yourself."
- **Failure modes are not optional.** A recipe without a Failure Modes
  section is a tutorial, not a recipe.
- **Cite ADRs by number.** "Per ADR 0025" not "per the workflow ADR."
- **No emojis.** Match house style.

## After writing

- Run `task test` if the recipe ships a worked example — make sure
  the example tests pass before linking from the recipe.
- If the recipe motivates a future ADR (you've discovered a new
  primitive worth extracting), surface that to the user — but write
  the recipe first; the ADR follows real usage.

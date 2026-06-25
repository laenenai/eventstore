# Spikes

Time-boxed performance, capacity, and feasibility investigations
that exercise the framework against a hypothesis with measurable
SLOs. Different from ADRs — an ADR captures a decision; a spike
generates the evidence a decision needs.

Each spike is a single Markdown document that starts as a **plan**
(goal, hypothesis, scope, scenarios, effort estimate) and grows
into a **report** as findings land (audit results, smoke output,
per-scenario measurements, recommendation). The plan and report
live in the same file with explicit `Status:` tracking. Same
document, different states.

## Index

| #    | Title                                                                                      | Status       |
| ---- | ------------------------------------------------------------------------------------------ | ------------ |
| 0001 | [Eventstore tenancy at `idi_*` scale](./0001-laenen-tenancy.md)                             | Audit complete; Smoke pending |

## Conventions

- **Numbering:** sequential, zero-padded to four digits, matching
  the ADR convention.
- **Status:** one of `Planning`, `Audit`, `Smoke`, `Phase 1`,
  `Phase 2`, `Reporting`, `Concluded`. The status field tells a
  reader at a glance which sections are forward-looking vs.
  evidence-backed.
- **Cross-references:** if a spike was requested by an upstream
  document (e.g., an ADR in another repo gating on the spike's
  outcome), cite that document in the header so the dependency is
  legible.
- **Reproducibility:** every spike must produce a harness committed
  to the framework repo — not throwaway scripts. The harness lives
  under `estest/bench/` or a similar shared location and is
  maintained as part of the framework's test surface even after the
  spike concludes.
- **Concluded spikes stay.** A spike that has shipped its
  recommendation is not deleted; it's a load-bearing reference for
  any future capacity or schema change that revisits the same
  hypothesis.

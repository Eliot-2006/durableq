# Product

<!-- impeccable:product-schema 1 -->

> Inferred, not interviewed: written from the W2 task brief and the repository docs because no one was available to answer init questions in this worker session. Every fact below traces to `README.md`, `docs/*.md` or the brief.

## Platform

web

## Stack

Astro (static output) + TypeScript (strict) + Apache ECharts + hand-written SVG; npm; GitHub Pages at `https://eliot-2006.github.io/durableq/` (base path `/durableq/`). No backend, no analytics, no third-party CDN fonts.

## Users

Recruiters and hiring managers skimming for evidence of engineering judgment, and backend / infrastructure / distributed-systems engineers who will read the claims critically and check them against the repository.

## Product Purpose

DurableQ is a durable job queue in Go with PostgreSQL as its only coordinator, also runnable on Kubernetes (Helm, KEDA, Prometheus, Grafana). The site documents it: what it guarantees, how it works, what was measured and in which environment, and what it deliberately does not claim. Success is a reader finishing with an accurate model of the guarantees and the numbers, and trusting the project because its limits are stated plainly.

## Positioning

Correctness is demonstrated, not asserted: chaos tests kill real processes and seven invariants are checked in SQL. A neighbouring project could not truthfully copy the measured results, the fencing/lease semantics, or the stated non-guarantees.

## Operating Context

Readers arrive from a README or CV link, often on a phone, usually for a few minutes. Evidence lives in `docs/` and `bench/` on GitHub; the site links to it rather than copying it.

## Capabilities and Constraints

- Guarantees (exact): at-least-once execution; at most one terminal transition per job; fenced transitions; exactly-once *transactional* effects (`CommitFunc` writes commit iff that attempt's completion commits); bounded attempts.
- Not guaranteed: exactly-once external side effects (handlers must be idempotent); ordering.
- Never claimed: exactly-once execution, zero duplicate executions, unlimited scalability, production readiness, high availability.
- Fencing rejects a stale database transition and its transactional effects; it cannot undo an external side effect a stale handler already performed.
- Bare-process benchmark (about 10.9k jobs/s, 8x50 slots, host Postgres) and kind numbers (at most 2k jobs/s, 6 connections per pod, Postgres inside a kind node) are different setups and are never compared as one.
- Every measured number comes from `src/data/results.json`; unmeasured cells stay absent.

## Brand Commitments

Name: DurableQ. Repo: `github.com/Eliot-2006/durableq`. MIT licence. No logo exists. Voice: plain, precise, first-person-plural-free technical prose; limits stated before strengths.

## Evidence on Hand

Raw benchmark and chaos files under `bench/`, a real Grafana screenshot to be supplied at `site/public/media/grafana-keda.png`. No testimonials, customers or production deployments exist and none may be implied.

## Product Principles

- State the limit next to the claim.
- Name the environment next to every number.
- Link to the evidence instead of restating it.
- A simulation is labelled a simulation.
- Show mechanisms, not adjectives.

## Accessibility & Inclusion

WCAG AA contrast, visible focus, full keyboard operation, light and dark themes, reduced-motion respected, usable at 360px without horizontal scroll.

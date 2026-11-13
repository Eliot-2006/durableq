# DurableQ site: design system

Recorded from the shipped CSS (`src/styles/global.css`). Product truth is in `PRODUCT.md`.

## World

A measurement report, not a product site. Read mode (documentation), with one Operate surface (the reliability simulator). Every number carries its environment and a link to its raw source; claims carry their limits. No cards-as-structure, no gradients, no shadows beyond focus, no hero metrics.

## Colour

Each token is declared once with `light-dark()`; the theme toggle only flips `color-scheme` on `<html>` (`data-theme`), and the system preference applies until the user chooses. Tokens are hex so ECharts can parse them after the browser resolves `light-dark()`.

| Token | Light | Dark | Role |
|---|---|---|---|
| `--bg` / `--bg-sunk` | `#fafbfc` / `#f0f2f6` | `#0f141b` / `#161c25` | page / code, table heads, callouts |
| `--ink` / `--ink-2` / `--ink-3` | `#1b2330` / `#4a5568` / `#596478` | `#e6eaf0` / `#a9b3c2` / `#8f9bad` | text, secondary, tertiary (all ≥ 4.5:1 on `--bg`) |
| `--rule` / `--rule-strong` | `#dde1e8` / `#bcc3d0` | `#263040` / `#3a4760` | hairlines (decorative only) |
| `--control` | `#7a8499` | `#66758f` | outlines of buttons and segmented controls (≥ 3:1) |
| `--accent` / `--accent-bg` | `#4540c0` / `#eceafb` | `#a5a8ff` / `#232652` | links, selection, current state, primary action only |
| `--ok` / `--bad` / `--warn` | `#17703f` / `#b42318` / `#8a5a00` | `#4cc38a` / `#ff8a80` / `#e0b050` | invariant and log state |
| `--c1` `--c2` `--c3` | indigo, vermilion, teal | lighter equivalents | chart series |

**Signature: environment tags.** `process` (slate, square marker) and `kind` (amber, diamond marker) never share a colour or a shape, so a bare-process figure cannot be read as a kind figure. `sim` (neutral, round marker) labels the simulator. Each tag also carries a text label.

## Type

System stacks only: `ui-sans-serif, system-ui, …` for reading and UI, `ui-monospace, 'SF Mono', …` for code, SQL, units and tags. Body 17px / 1.65, measure 44rem. Headings 650 weight, tracking −0.015em to −0.025em, `text-wrap: balance`. Scale: h1 2rem (2.5rem from 40rem), h2 1.5rem, h3 1.1875rem, UI 0.9375rem, notes 0.875rem. Tabular numerals in tables.

## Layout

`--page` 64rem container, `--measure` 44rem prose. `.row` pairs a prose column with a 15rem evidence margin (`.note`) from 70rem, and collapses the note beneath the prose below that. Every grid declares `minmax(0, 1fr)` first so tables and `pre` never force horizontal page scroll. Wide figures and tables use the full container.

## Components

- **Masthead:** brand, six nav links (`aria-current="page"` underline), GitHub, theme toggle. Nav wraps under the brand below 50rem.
- **Ledger:** measured highlights as rows (claim, detail, environment tag, sources), never a number-over-label template.
- **Evidence note:** margin note with title, optional environment tag, source links.
- **Architecture explorer:** portrait SVG (viewBox 420 × 668) of eight `role="button"` nodes beside a responsibility panel; all panels render without JS.
- **Charts:** ECharts (canvas) re-themed on `themechange`; each has a data table in a `<details>`.
- **Simulator:** controls, caption (`aria-live`), job, workers, attempts, effects, log with expandable SQL, invariants (pending until the run completes).
- Buttons 40px minimum height; focus ring 2px `--accent`, offset 2px.

## Motion

Colour transitions of 150 ms, one 400 ms lease-bar width ease, and the animated terminal GIF. All disabled under `prefers-reduced-motion: reduce`; the GIF swaps to its final frame through `<picture>`; ECharts animation is off.

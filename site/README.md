# DurableQ site

Static documentation site for DurableQ, built with Astro and published to GitHub Pages at
<https://eliot-2006.github.io/durableq/>. There is no backend, no analytics and no third-party font or script
host: every page is prerendered HTML, a little client-side TypeScript, and a system font stack.

## Run it locally

Requires Node 22.23.4 or newer (CI uses Node 24).

```sh
cd site
npm ci
npm run dev        # http://localhost:4321/durableq/  (note the base path)
```

Checks, in the order CI runs them:

```sh
npx astro check                                  # TypeScript and Astro diagnostics
npm run build                                    # static output in dist/
node scripts/check-links.mjs dist /durableq/     # every internal link, asset and #fragment in dist/
node --test src/sim/engine.test.ts               # the simulation engine's tests
npm run preview                                  # serve dist/ locally under the base path
```

## How data flows

```
bench/**, docs/*.md ──► go run ./tools/resultsgen ──► src/data/results.json ──► src/lib/data.ts ──► pages, charts
```

- `src/data/results.json` holds every measured number, with `sources` pointing at the raw files in `bench/` and
  `docs/`. **Do not edit it by hand.** Regenerate it with `go run ./tools/resultsgen` from the repository root.
- `src/lib/data.ts` is the only place pages and charts read it. Speed-ups, CPU in cores and the like are computed
  there or in the page from the raw values, never typed in, so a regenerated file updates every figure at once.
- Prose that quotes a number from `docs/` rather than from `results.json` (for example the 41,131 abandoned
  attempts) is the exception. Check those when the docs change.
- Unmeasured cells stay absent: charts plot only rows that exist in the data.
- `src/sim/engine.ts` is the deterministic reliability simulation, with its own tests. The Reliability page uses only
  its exports (`initialState`, `step`, `injectFailure`, `canInjectFailure`, `defaultConfig`).
- `public/media/` holds the real Grafana screenshots and terminal recording. `docs/demo/README.md` in the repository
  records how each was produced.

## Layout

```
astro.config.mjs        site, base ('/durableq/'), trailingSlash
src/layouts/Base.astro  masthead, theme toggle, footer
src/components/         ArchDiagram (interactive SVG), Env tag, evidence Note, Sources
src/pages/              one .astro file per route
src/scripts/charts.ts   ECharts setup for the Performance page (echarts/core, only the charts used)
src/styles/global.css   all CSS: tokens (light-dark()), document type, components
src/lib/url.ts          href() and blob(): the only way to build links
scripts/check-links.mjs dependency-free link and asset checker
```

## Add a page

1. Create `src/pages/<name>.astro` wrapping its content in `<Base title="…" description="…">`.
2. Build every internal link with `href('<name>/')` and every GitHub link with `blob('<repo path>')` from
   `src/lib/url.ts`. A hard-coded `/foo` would escape the `/durableq/` base path; the link checker fails the build if
   one slips through.
3. Add the page to the `nav` array in `src/layouts/Base.astro` if it belongs in the masthead.
4. Name the environment next to any number with `<Env env="process" | "kind" | "sim" />`, and link its raw source with
   `<Sources paths={[…]} />`. Bare-process and kind figures are never compared with each other.

## Deployment

`.github/workflows/pages.yml` builds on pull requests that touch `site/**` or the workflow, and on pushes to `main`.
It runs `npm ci`, `astro check`, `astro build`, the link checker and the engine tests. Only pushes to `main` and
manual runs (`workflow_dispatch`) go on to deploy, through `actions/configure-pages`, `upload-pages-artifact` and
`deploy-pages`. Every action is pinned by commit SHA.

**One-time repository setting:** Settings → Pages → Source: **GitHub Actions**. Without it the deploy job fails.

If the repository is renamed or moved, update `site` and `base` in `astro.config.mjs`, the default base path in
`scripts/check-links.mjs` and the workflow, and `REPO` in `src/lib/url.ts`.

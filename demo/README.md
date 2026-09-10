# ED-Alpha Static Demo

This directory contains the static ED-Alpha demo site used for GitHub Pages. It is separate from the live Docker app under `app/`: this demo reads from `data/demo-data.json` and does not require the FastAPI backend or Postgres.

## Routes

- `/` shows the ED-Alpha landing page with the embedded MP4 walkthrough.
- `/tutorial/` shows the interactive dashboard tutorial.
- `/walkthrough/` shows the timed HTML walkthrough with playback controls, subtitles, highlights, and narration.
- `/demo-video/` keeps the same walkthrough available for older links.

## Paper results

The interactive dashboard uses Experiment 20 and the paper-matching runs:
Sonnet 4.5 (50), GPT-5 (52), and Gemini Pro 2.5 (54). Company scores are the
arithmetic mean of their article scores, with unrounded scores determining rank
and CIK ascending breaking ties. All 100 labelled companies are retained;
companies without scored articles receive zero. The displayed K values are 5, 25,
and 50. The source is the submitted paper at commit
`434e376c6a6b52f36683d70ab6b75c305cf37795`.

`data/demo-data.json` contains all 100 companies per run, their labels, average
scores, score sums/counts, up to three article evidence records, and the tracked
filings supporting positive labels. Exports identify the model, Run and mean
aggregation method. Hit/Miss uses the experiment label, and filing links come
from `filing_experiment_label_evidence` within September 1–5, not any nearby 8-K.

The old stored DB totals and the live backend under `app/` still use sum scoring.
This change does not migrate or overwrite them. The static demo is self-contained
and needs no database credentials when built or served.

To regenerate from the original article scores using a read-only connection:

```bash
python -m pip install -r scripts/requirements-export.txt
python scripts/export_paper_snapshot.py --env-file /absolute/path/to/private.env
pnpm test
pnpm typecheck
```

The exporter uses a repeatable-read, read-only transaction with timeouts and
validates the paper's true-positive counts before replacing the local JSON. It
never calls scoring APIs or writes SQL. Keep env files outside the repository.
Run 50 has 4,290 scored article-company pairs; Runs 52 and 54 have 4,291 each.

The landing page's prerecorded MP4 retains its earlier sum-scoring capture and
is labelled accordingly, with a link to the updated interactive results. The
HTML walkthrough shares the updated dashboard; its existing narration assets
have not been regenerated (including the pre-existing August 31 date statement).

## Local Development

```bash
corepack enable
pnpm install
pnpm dev
```

Open http://localhost:3000.

## Static Build

```bash
pnpm build
```

The build writes static files to `out/`. The app is configured with `output: "export"` and includes `.nojekyll` so it can be hosted on GitHub Pages.

For project Pages, build with the repository base path:

```bash
NEXT_PUBLIC_BASE_PATH=/ED-ALPHA pnpm build
```

For a custom domain at the site root, leave `NEXT_PUBLIC_BASE_PATH` unset.

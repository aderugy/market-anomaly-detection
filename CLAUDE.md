# Market Anomaly Detection

Python research repo: anomaly detection on market data (odds, prices, volumes).

## How to work with me

- Do only what I ask in the current message. Don't anticipate next steps, add features,
  or create files I didn't request. If you see something worth doing, mention it in one line and stop.
- If a request is ambiguous, ask one short question before writing code.
- For anything touching more than one file, propose a short plan first and wait for my OK.
- Show evidence, not claims: row counts, the printed metric, or the path of a saved figure.
- Keep changes small and reviewable. Don't refactor code unrelated to the request.

## Commands

- Install: `uv sync`
- Tests: `uv run pytest -q`
- Lint and format: `uv run ruff check --fix . && uv run ruff format .`
- Run a module: `uv run python -m market_anomaly_detection.<module>`

## Layout

- `data/raw`: downloaded files, never modified (gitignored)
- `data/clean`: derived tables, always rebuildable by a script in `src` (gitignored)
- `src/market_anomaly_detection`: reusable code; `notebooks`: exploration only; `tests`: pytest
- `reports/figures`: saved figures

## Research rules

- No look-ahead: anything that detects or predicts uses only data available at time t.
  Rolling baselines exclude the current row. Outcomes and future prices are for evaluation only.
- Tune on earlier periods, evaluate on later ones. Never re-tune after seeing test results.
- Report every metric next to a naive baseline (random at the same rate, fixed threshold).
- Work on a small sample first, then the full data.
- Log row counts before and after every filter or join.
- A result that looks too good is a leak until proven otherwise.

## Conventions

- Timestamps in UTC, columns suffixed `_utc`. Odds are decimal; probabilities in [0, 1].
- Explicit column names (`p_consensus`, `gap_pp`). Pure functions in `src`; I/O in scripts.
- Write a test for each non-trivial function you add to `src`.

# market-anomaly-detection

Real-time detection of price inconsistencies and abnormal volumes on market data.

## Introduction

[Beat the bookie paper](https://arxiv.org/pdf/1710.02824)
- The market consensus odds is a very good predictor of the match issue
- There is a risk for a bookmaker if he underestimates the probability of an event (thinks its less likely than it really is)

## Definitions

### Market Price
Median devigged probabilities of all lines of a market across all but the selected bookmakers
at time t, with at least n bookmakers.

### Alert
Notification sent by the model when he detects that a bookmaker's position differs from
the market price at time t.

We need to distinguish the following movements :
- Promotions from bookmakers: the distance to the market price is stable
- The book is ahead: book moves, market doesn't
- **The book is late**: the market moved, and the book didn't

## Formalisation

### Definitions
- $t \in \{ 0, ..., 71 \}$
- $p_{b,i}(t)$ : devigged probability provided by book b for outcome i at snapshot t
- $B_{-b,i}(t)$ if $|B_{-b,i}(t)| \ge n_{min}$ : devigged probabilities of books other than b for outcome i at snapshot t.
- $m_{-b,i}(t) = med(\{ p_{c,i}(t) | c \in B_{-b}(t)\})$ : market price of book b on outcome i at time t
- $iqr_{-b,i}(t) = Q3 - Q1$ : Interquartile range
- $d_{b,i}(t) = p_{b,i}(t) - m_{-b,i}(t)$ : market price deviation
- $z_{b,i}(t) = \frac{d_{b,i}(t)}{iqr_{-b,i}(t)}$ : standardized market price deviation
- $\Delta m = m_{-b,i}(t) - m_{-b,i}(t - k)$
- $\Delta p = p_{b,i}(t) - p_{b,i}(t - k)$

### Hypotheses

1. The closing market price (all books, $t = 71$) is a good estimator of the probability of an outcome
2. A book $b$ is declared late if the following conditions are met. That's the baseline
   3. $|\Delta m| \gt \delta$: The market has moved
   4. $|z_{b, i}(t)| \gt z^{\star}$: The deviation from the market priced is significant
   5. $\frac{\Delta p}{\Delta m} \lt \rho$: The book didn't follow the market

## Run

```bash
uv sync
uv run pytest -q
```

## Project 1: Detect lagging bookmakers, validated on historical data

Dataset: [Dataset](https://www.kaggle.com/datasets/austro/beat-the-bookie-worldwide-football-dataset)

### Step 1: Data exploration

The data contains five CSV tables :
- closing_odds
- odds_series
- odds_series_matches
- odds_series_b
- odds_series_b_matches

### Step 2: Data Cleaning (ELT Pipeline)

We load the raw CSVs into a DuckDB.

Odds series are separated in two tables and have 72x3x32 columns for the time series (one row per match).
Using unpivot and regexes: we split the very long rows in multiple rows such that there is one row per (match_id, odd, book, outcome, step).
We replace NaN with NULL to avoid surprises when aggregating.

For all tables: we apply the classic transformations
- Trimming text columns

### Step 3: Data validation

Before making any further analysis, let's try to verify that the data can be exploited for the current use case.

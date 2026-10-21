"""Read the Beat the Bookie odds series as (n, 3, 32, 72) cubes: outcome x book x step."""

import re
from collections.abc import Iterator
from pathlib import Path

import numpy as np
import pandas as pd

OUTCOMES = ("home", "draw", "away")
N_BOOKS = 32
N_STEPS = 72
ODDS_COLUMN = re.compile(r"^(home|draw|away)_b(\d+)_(\d+)$")


def odds_column_index(columns: list[str]) -> tuple[list[str], np.ndarray, np.ndarray, np.ndarray]:
    """Odds column names and their (outcome, book, step) positions in the cube.

    Books are 1-based in the names (b1..b32) and 0-based in the cube.
    """
    names, o_idx, b_idx, s_idx = [], [], [], []
    for col in columns:
        m = ODDS_COLUMN.match(col.strip())
        if m is None:
            continue
        names.append(col)
        o_idx.append(OUTCOMES.index(m.group(1)))
        b_idx.append(int(m.group(2)) - 1)
        s_idx.append(int(m.group(3)))
    return names, np.array(o_idx), np.array(b_idx), np.array(s_idx)


def to_cube(values: np.ndarray, o_idx, b_idx, s_idx) -> np.ndarray:
    """Place a (n, n_odds_columns) block into a NaN-filled (n, 3, 32, 72) float32 cube."""
    cube = np.full((values.shape[0], len(OUTCOMES), N_BOOKS, N_STEPS), np.nan, dtype=np.float32)
    cube[:, o_idx, b_idx, s_idx] = values
    return cube


def read_header(path: Path) -> list[str]:
    return list(pd.read_csv(path, nrows=0).columns)


def iter_odds_chunks(
    path: Path, chunksize: int = 2000, nrows: int | None = None
) -> Iterator[tuple[pd.DataFrame, np.ndarray]]:
    """Yield (meta, cube) per chunk; meta holds the non-odds columns (match_id, date, time...)."""
    columns = read_header(path)
    names, o_idx, b_idx, s_idx = odds_column_index(columns)
    meta_cols = [c for c in columns if c not in set(names)]
    dtype = {c: np.float32 for c in names} | {c: str for c in meta_cols}
    reader = pd.read_csv(path, dtype=dtype, chunksize=chunksize, nrows=nrows)
    for chunk in reader:
        meta = chunk[meta_cols].reset_index(drop=True)
        meta["match_id"] = meta["match_id"].astype(np.int64)
        yield meta, to_cube(chunk[names].to_numpy(dtype=np.float32), o_idx, b_idx, s_idx)


def read_matches(path: Path) -> pd.DataFrame:
    """Match table (latin-1, leading spaces in names and some values), whitespace stripped."""
    df = pd.read_csv(path, encoding="latin-1", dtype=str, skipinitialspace=True)
    df.columns = [c.strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].str.strip()
    df["match_id"] = df["match_id"].astype(np.int64)
    return df

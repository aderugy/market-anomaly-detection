import argparse
from pathlib import Path

import duckdb

from market_anomaly_detection.pipeline import Filter, Fix, Load, Transform, prune, run_steps

ROOT = Path(__file__).resolve().parent.parent
DATASET_DIR = ROOT / "data" / "raw" / "beat_the_bookie"
CLEAN_DIR = ROOT / "data" / "clean"
TMP_DIR = ROOT / "data" / "tmp"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)

db = duckdb.connect(str(CLEAN_DIR / "clean.duckdb"))
db.execute(f"SET temp_directory = '{TMP_DIR}'")
db.execute("SET preserve_insertion_order = false")
db.execute("SET threads = 4")
db.execute("SET memory_limit = '14GB'")

ID_COLS = "match_id, match_date, match_time, score_home, score_away"


def load_csv(table: str, file: str, encoding: str) -> Load:
    """Faithful read of a narrow CSV; unparseable rows are kept in <table>__rejects."""
    return Load(
        "load",
        f"""
    FROM read_csv('{DATASET_DIR / file}',
                  header = true, filename = false, sample_size = -1,
                  encoding = '{encoding}',
                  store_rejects = true,
                  rejects_table = 'rej_{table}',
                  rejects_scan = 'rej_scan_{table}')""",
        rejects=f"rej_{table}",
    )


def file_list(files: dict[str, str]) -> str:
    return "[" + ", ".join(f"'{DATASET_DIR / f}'" for f in files.values()) + "]"


def source_col(files: dict[str, str]) -> str:
    """`source` ('a', 'b', ...) from the `filename` column of a multi-file read_csv."""
    whens = " ".join(f"WHEN '{DATASET_DIR / f}' THEN '{s}'" for s, f in files.items())
    return f"CASE filename {whens} END AS source"


def load_sources(table: str, files: dict[str, str], encoding: str) -> Load:
    """Faithful read of narrow CSVs with the same header, tagged with their `source`;
    unparseable rows are kept in <table>__rejects."""
    return Load(
        "load",
        f"""
    SELECT {source_col(files)}, * EXCLUDE (filename)
    FROM read_csv({file_list(files)},
                  header = true, filename = true, sample_size = -1,
                  encoding = '{encoding}',
                  store_rejects = true,
                  rejects_table = 'rej_{table}',
                  rejects_scan = 'rej_scan_{table}')""",
        rejects=f"rej_{table}",
    )


def load_unpivot(files: dict[str, str], encoding: str) -> Load:
    """Read wide CSVs with the same header (one column per series) as long rows
    (source, col, odds)."""
    return Load(
        "load_unpivot",
        f"""
    SELECT {source_col(files)}, * EXCLUDE (filename)
    FROM (UNPIVOT (FROM read_csv({file_list(files)},
                                 header = true, filename = true,
                                 encoding = '{encoding}'))
          ON COLUMNS(* EXCLUDE ({ID_COLS}, filename))
          INTO NAME col VALUE odds)""",
    )


# Steps are views, so this costs no storage, but each extra step rescans the gzipped CSV
# to count rows (214M rows in A, 425M in B): keep the split and the casts in one step, or
# set materialize=True on it before appending more steps.
SPLIT_ODDS_COL = [
    Transform(
        "split_col",
        r"""
        SELECT *                              EXCLUDE (col, parts), parts.outcome,
               CAST(parts.book AS INTEGER) AS book,
               CAST(parts.step AS INTEGER) AS step
        FROM (SELECT *,
                     regexp_extract(col, '^(home|draw|away)_b(\d+)_(\d+)$',
                         ['outcome', 'book', 'step']) AS parts
              FROM {prev})""",
        materialize=True
    ),
]

DROP_MATCH_COLUMNS_ODDS_SERIES = [
    Transform(
        "drop_match_columns",
        r"""
        SELECT * EXCLUDE (match_date, match_time, score_home, score_away)
        FROM {prev}
        """,
    )
]

MATCHES_TEXT_COLS = ["league", "home_team", "away_team", "score", "detailed_score"]
MATCHES_STEPS = [Fix(f"trim_{c}", c, f"trim({c})") for c in MATCHES_TEXT_COLS]

CLOSING_ODDS_STEPS = [
    *[Fix(f"trim_{c}", c, f"trim({c})") for c in ["league", "home_team", "away_team"]],
    # (date, home, away) is the join key to the odds series: drop every row of a repeated key
    Filter(
        "unique_date_home_away",
        "count(*) OVER (PARTITION BY match_date, home_team, away_team) = 1",
    ),
]

# Bookie name -> 0-based row of the dataset's odds matrix (from the dataset's PHP scraper).
# Column b<N> of the odds series is row N - 1, so `book` = index + 1.
BOOKIE_INDEX = {
    "Interwetten": 0, "bwin": 1, "bet-at-home": 2, "Unibet": 3, "Stan James": 4,
    "Expekt": 5, "10Bet": 6, "William Hill": 7, "bet365": 8, "Pinnacle Sports": 9,
    "Pinnacle": 9, "DOXXbet": 10, "Betsafe": 11, "Betway": 12, "888sport": 13,
    "Ladbrokes": 14, "Betclic": 15, "Sportingbet": 16, "myBet": 17, "Betsson": 18,
    "188BET": 19, "Jetbull": 20, "Paddy Power": 21, "Tipico": 22, "Coral": 23,
    "SBOBET": 24, "BetVictor": 25, "12BET": 26, "Titanbet": 27, "youwin": 28,
    "ComeOn": 29, "Betadonis": 30, "Betfair Sports": 31, "Betfair": 31, "mybet": 17,
}  # fmt: skip
# Names missing from the scraper's list of all bookies: alternative spellings of a book
BOOKIE_ALIASES = {"Pinnacle", "Betfair", "mybet"}

SERIES_FILES = {"a": "odds_series.csv.gz", "b": "odds_series_b.csv.gz"}
SERIES_MATCHES_FILES = {"a": "odds_series_matches.csv.gz", "b": "odds_series_b_matches.csv.gz"}

BOOKIES_VALUES = ", ".join(
    f"('{name}', {i}, {name in BOOKIE_ALIASES})" for name, i in BOOKIE_INDEX.items()
)

PIPELINES = {
    # one row per name; join on book with `NOT alias` to get one name per book
    "bookies": [
        Load(
            "load",
            f"""
    SELECT bookie, book_index, book_index + 1 AS book, alias
    FROM (VALUES {BOOKIES_VALUES}) AS t(bookie, book_index, alias)""",
        )
    ],
    # unlike the *_matches files, UTF-8
    "closing_odds": [
        load_csv("closing_odds", "closing_odds.csv.gz", "utf-8"),
        *CLOSING_ODDS_STEPS,
    ],
    # A (2015-09 to 2016-02) and B (2016-03 to 2016-11) share no match, but one match_id
    # (1866143) names a different game in each: the key is (source, match_id)
    "odds_series_matches": [
        load_sources("odds_series_matches", SERIES_MATCHES_FILES, "latin-1"),
        *MATCHES_STEPS,
    ],
    "odds_series": [
        load_unpivot(SERIES_FILES, "latin-1"),
        *DROP_MATCH_COLUMNS_ODDS_SERIES,
        *SPLIT_ODDS_COL,
        Fix("replace_nan_by_null", "odds", "CASE WHEN isnan(odds) THEN NULL ELSE odds END"),
    ],
}


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build data/clean/clean.duckdb. Unchanged steps are reused from the cache."
    )
    ap.add_argument("--only", nargs="+", choices=list(PIPELINES), help="tables to build")
    ap.add_argument("--force", action="store_true", help="ignore the cache, rebuild every step")
    ap.add_argument(
        "--prune", action="store_true", help="turn layers kept from old finals into views"
    )
    args = ap.parse_args()
    for final in args.only or PIPELINES:
        run_steps(db, final, PIPELINES[final], force=args.force)
    if args.prune:
        for name in prune(db):
            print(f"pruned {name} (now a view)")


if __name__ == "__main__":
    main()

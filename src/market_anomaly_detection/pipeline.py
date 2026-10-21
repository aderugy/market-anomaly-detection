"""Sequential, named cleaning steps on DuckDB, from ingestion to one final table, cached like
Docker layers and logged.

The first step is a `Load` that reads the source. Every step before the last is a view
`<final>__<NN>_<name>`, so it costs no storage and can still be queried after a run; pass
`materialize=True` to make one a table (a checkpoint). The last step is written directly as the
table `<final>`, so the data is stored once. Filters also expose the rows they drop as the view
`<final>__<NN>_<name>__dropped`.

Caching: each step has a key, the hash of the previous key and of the step's definition (and,
for a Load, of the content of the files its SQL reads). `pipeline_cache` keeps, per step, the
key, the object holding the result and its counts. A run reuses the longest prefix of steps
whose keys match and whose objects exist, and rebuilds from the first changed step. When steps
are appended after the previous final, that table is renamed into a layer rather than
recomputed; `prune` turns such layers back into views to free disk.

Not tracked: tables other than the previous step that a step's SQL reads (e.g. a join on
another pipeline's table), and DuckDB itself. Use `force=True` when those change.

Views are lazy: counting a step re-runs the chain from the last table or the source file.
"""

import dataclasses
import glob
import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb

# Bump when the SQL that run_steps generates for a step kind changes: it invalidates all caches.
PIPELINE_VERSION = 1


@dataclass(frozen=True)
class Load:
    """First step: a query reading the source (read_csv, read_parquet, an existing table...).

    Files named in quoted strings in `sql` (globs included) are hashed into the cache key, so
    editing a raw file invalidates the pipeline. Tables read by `sql` are not tracked.

    `rejects` names the `rejects_table` used in `sql`, if any. DuckDB fills reject tables only
    while the read runs and appends on every rescan, so a Load with `rejects` is always a table.
    The rejects are kept as `<final>__rejects` and counted in the log.
    """

    name: str
    sql: str
    rejects: str | None = None
    materialize: bool = False


@dataclass(frozen=True)
class Filter:
    """Keep rows where `keep` is true; rows where it is false or NULL go to <step>__dropped.

    `keep` may use window functions, e.g. `count(*) OVER (PARTITION BY match_id) = 1`.
    """

    name: str
    keep: str
    materialize: bool = False


@dataclass(frozen=True)
class Fix:
    """Rewrite one column in place; logs how many rows change."""

    name: str
    column: str
    expr: str
    materialize: bool = False


@dataclass(frozen=True)
class Transform:
    """Any query over {prev}: dedup, join, derived columns. Logs rows in and out only."""

    name: str
    sql: str
    materialize: bool = False


Step = Load | Filter | Fix | Transform

LOG_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_log (
    run_at_utc TIMESTAMP, tbl VARCHAR, step_no INTEGER, step VARCHAR, kind VARCHAR,
    stored_as VARCHAR, rows_in BIGINT, rows_out BIGINT, rows_dropped BIGINT,
    rows_changed BIGINT, rows_rejected BIGINT, seconds DOUBLE PRECISION
)
"""

CACHE_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_cache (
    tbl VARCHAR, step_no INTEGER, step VARCHAR, kind VARCHAR, key VARCHAR, object VARCHAR,
    stored_as VARCHAR, kept_final BOOLEAN, query VARCHAR, rows_in BIGINT, rows_out BIGINT,
    rows_dropped BIGINT, rows_changed BIGINT, rows_rejected BIGINT, built_at_utc TIMESTAMP
)
"""

COUNT_COLS = ["rows_in", "rows_out", "rows_dropped", "rows_changed", "rows_rejected"]

_digests: dict[tuple[str, int, int], str] = {}


def count(con: duckdb.DuckDBPyConnection, relation: str) -> int:
    return con.sql(f"SELECT count(*) FROM {relation}").fetchone()[0]


def object_type(con: duckdb.DuckDBPyConnection, name: str) -> str | None:
    """'table', 'view', or None if `name` does not exist in the main schema."""
    found = con.execute(
        "SELECT table_type FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name = ?",
        [name],
    ).fetchone()
    return None if found is None else ("view" if found[0] == "VIEW" else "table")


def drop(con: duckdb.DuckDBPyConnection, name: str) -> None:
    kind = object_type(con, name)
    if kind is not None:
        con.execute(f"DROP {kind.upper()} {name}")


def create(con: duckdb.DuckDBPyConnection, name: str, query: str, as_table: bool) -> None:
    """CREATE OR REPLACE `name` as a table or a view, dropping an object of the other kind."""
    if object_type(con, name) == ("view" if as_table else "table"):
        drop(con, name)
    con.execute(f"CREATE OR REPLACE {'TABLE' if as_table else 'VIEW'} {name} AS {query}")


def file_digest(path: Path) -> str:
    """sha256 of a file's content, memoised on (path, size, mtime) within the process."""
    st = path.stat()
    memo = (str(path.resolve()), st.st_size, st.st_mtime_ns)
    if memo not in _digests:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for block in iter(lambda: f.read(1 << 20), b""):
                h.update(block)
        _digests[memo] = h.hexdigest()
    return _digests[memo]


def load_inputs(sql: str) -> list[Path]:
    """Existing files named in single-quoted strings of `sql`; glob patterns are expanded."""
    paths = set()
    for s in re.findall(r"'([^']+)'", sql):
        if any(c in s for c in "*?["):
            paths.update(Path(p) for p in glob.glob(s) if Path(p).is_file())
        elif Path(s).is_file():
            paths.add(Path(s))
    return sorted(paths)


def step_key(prev_key: str, step: Step) -> str:
    """Hash of the previous key, the step's kind and fields, and a Load's input files."""
    payload = {
        "version": PIPELINE_VERSION,
        "prev": prev_key,
        "kind": type(step).__name__,
        "fields": dataclasses.asdict(step),
    }
    if isinstance(step, Load):
        payload["inputs"] = [[str(p), file_digest(p)] for p in load_inputs(step.sql)]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def step_keys(steps: list[Step]) -> list[str]:
    keys, prev = [], ""
    for step in steps:
        prev = step_key(prev, step)
        keys.append(prev)
    return keys


def layer_name(final: str, step_no: int, step: str) -> str:
    return f"{final}__{step_no:02d}_{step}"


def cached_rows(con: duckdb.DuckDBPyConnection, final: str) -> dict[int, dict]:
    cur = con.execute("FROM pipeline_cache WHERE tbl = ? ORDER BY step_no", [final])
    cols = [d[0] for d in cur.description]
    return {r[1]: dict(zip(cols, r, strict=True)) for r in cur.fetchall()}


def reusable_prefix(con: duckdb.DuckDBPyConnection, keys: list[str], cache: dict[int, dict]) -> int:
    """Number of leading steps whose cached key matches and whose object still exists."""
    p = 0
    for i, key in enumerate(keys, 1):
        row = cache.get(i)
        if row is None or row["key"] != key or object_type(con, row["object"]) != row["stored_as"]:
            break
        p = i
    return p


def save_cache_row(con: duckdb.DuckDBPyConnection, row: dict) -> None:
    con.execute(CACHE_DDL)
    con.execute(
        "DELETE FROM pipeline_cache WHERE tbl = ? AND step_no = ?", [row["tbl"], row["step_no"]]
    )
    cols = ", ".join(row)
    con.execute(
        f"INSERT INTO pipeline_cache ({cols}) VALUES ({', '.join('?' * len(row))})",
        list(row.values()),
    )


def build_query(con: duckdb.DuckDBPyConnection, final: str, i: int, step: Step, prev):
    """SQL for step `i` over `prev`, plus the counts known before building it."""
    changed = None
    match step:
        case Load(sql=sql):
            query = sql
        case Filter(keep=keep):
            flagged = f"(SELECT *, ({keep}) AS _keep FROM {prev})"
            query = f"SELECT * EXCLUDE (_keep) FROM {flagged} WHERE _keep IS TRUE"
            create(
                con,
                f"{layer_name(final, i, step.name)}__dropped",
                f"SELECT * EXCLUDE (_keep) FROM {flagged} WHERE _keep IS NOT TRUE",
                as_table=False,
            )
        case Fix(column=col, expr=expr):
            changed = count(con, f"{prev} WHERE ({expr}) IS DISTINCT FROM {col}")
            query = f"SELECT * REPLACE (({expr}) AS {col}) FROM {prev}"
        case Transform(sql=sql):
            query = sql.format(prev=prev)
    return query, changed


def prepare(con, final: str, steps: list[Step], cache: dict[int, dict], p: int) -> None:
    """Drop stale objects past the reused prefix; move the old final out of the way or in."""
    for i, row in cache.items():
        if i > p:
            drop(con, row["object"])
            drop(con, f"{layer_name(final, i, row['step'])}__dropped")
    con.execute("DELETE FROM pipeline_cache WHERE tbl = ? AND step_no > ?", [final, p])
    if p == 0:
        return
    row = cache[p]
    if p < len(steps) and row["object"] == final:
        # Steps were appended after the previous final: keep it as a layer, do not recompute.
        layer = layer_name(final, p, row["step"])
        drop(con, layer)
        con.execute(f"ALTER TABLE {final} RENAME TO {layer}")
        save_cache_row(con, row | {"object": layer, "kept_final": True})
    elif p == len(steps) and row["object"] != final:
        # Steps were removed: the last reused step becomes the final table.
        drop(con, final)
        if row["stored_as"] == "table":
            con.execute(f"ALTER TABLE {row['object']} RENAME TO {final}")
        else:
            con.execute(f"CREATE TABLE {final} AS FROM {row['object']}")
            drop(con, row["object"])
        save_cache_row(con, row | {"object": final, "stored_as": "table", "kept_final": False})


def run_steps(
    con: duckdb.DuckDBPyConnection, final: str, steps: list[Step], force: bool = False
) -> list[dict]:
    """Run `steps` (a Load first) into the table `final`, reusing cached steps; return the log.

    `force=True` ignores the cache and rebuilds every step.
    """
    if not steps or not isinstance(steps[0], Load):
        raise ValueError("the first step must be a Load")
    if any(isinstance(s, Load) for s in steps[1:]):
        raise ValueError("only the first step can be a Load")
    names = [s.name for s in steps]
    if len(set(names)) != len(names):
        raise ValueError(f"step names must be unique: {names}")
    con.execute(LOG_DDL)
    con.execute("ALTER TABLE pipeline_log ADD COLUMN IF NOT EXISTS cached BOOLEAN")
    con.execute(CACHE_DDL)
    run_at = datetime.now(UTC).replace(tzinfo=None)

    keys = step_keys(steps)
    cache = cached_rows(con, final)
    p = 0 if force else reusable_prefix(con, keys, cache)
    prepare(con, final, steps, cache, p)
    cache = cached_rows(con, final)

    prev = cache[p]["object"] if p else None
    rows_in = cache[p]["rows_out"] if p else None
    log = []
    for i, step in enumerate(steps, 1):
        t0 = time.perf_counter()
        if i <= p:
            row = cache[i]
            counts = {c: row[c] for c in COUNT_COLS}
            stored_as, cached = row["stored_as"], True
        else:
            last = i == len(steps)
            out = final if last else layer_name(final, i, step.name)
            as_table = (
                last or step.materialize or (isinstance(step, Load) and step.rejects is not None)
            )
            query, changed = build_query(con, final, i, step, prev)
            create(con, out, query, as_table)
            rows_out = count(con, out)
            dropped = rows_in - rows_out if isinstance(step, Filter) else None
            rejected = None
            if isinstance(step, Load) and step.rejects is not None:
                create(con, f"{final}__rejects", f"FROM temp.main.{step.rejects}", as_table=True)
                rejected = count(con, f"{final}__rejects")
            counts = dict(
                zip(COUNT_COLS, [rows_in, rows_out, dropped, changed, rejected], strict=True)
            )
            stored_as, cached = ("table" if as_table else "view"), False
            save_cache_row(
                con,
                {
                    "tbl": final,
                    "step_no": i,
                    "step": step.name,
                    "kind": type(step).__name__.lower(),
                    "key": keys[i - 1],
                    "object": out,
                    "stored_as": stored_as,
                    "kept_final": False,
                    "query": query,
                    **counts,
                    "built_at_utc": run_at,
                },
            )
            prev, rows_in = out, rows_out
        row = {
            "run_at_utc": run_at,
            "tbl": final,
            "step_no": i,
            "step": step.name,
            "kind": type(step).__name__.lower(),
            "stored_as": stored_as,
            **counts,
            "seconds": round(time.perf_counter() - t0, 2),
            "cached": cached,
        }
        con.execute(
            f"INSERT INTO pipeline_log ({', '.join(row)}) VALUES ({', '.join('?' * len(row))})",
            list(row.values()),
        )
        log.append(row)
        print_step(row)
    return log


def print_step(row: dict) -> None:
    extras = [
        f"{k}={row[f'rows_{k}']:,}"
        for k in ["dropped", "changed", "rejected"]
        if row[f"rows_{k}"] is not None
    ]
    rows_in = "-" if row["rows_in"] is None else f"{row['rows_in']:,}"
    status = "cached" if row["cached"] else "built"
    print(
        f"{row['tbl']:<22} {row['step_no']:02d} {row['step']:<24} {row['stored_as']:<5} "
        f"{status:<6} {rows_in:>11} -> {row['rows_out']:>11,}  " + "  ".join(extras)
    )


def prune(con: duckdb.DuckDBPyConnection, final: str | None = None) -> list[str]:
    """Turn layers kept from a previous final back into views, freeing their storage.

    The rebuilt view runs the step's stored query, so later steps still work; the next run
    that resumes past it recomputes from the nearest table. Returns the layers pruned.
    """
    con.execute(CACHE_DDL)
    # A Load with rejects stays a table: as a view its reject table would be unreliable.
    where, params = "kept_final AND stored_as = 'table' AND rows_rejected IS NULL", []
    if final is not None:
        where, params = where + " AND tbl = ?", [final]
    rows = con.execute(
        f"SELECT tbl, step_no, object, query FROM pipeline_cache WHERE {where}", params
    )
    pruned = []
    for tbl, step_no, obj, query in rows.fetchall():
        drop(con, obj)
        con.execute(f"CREATE VIEW {obj} AS {query}")
        con.execute(
            "UPDATE pipeline_cache SET stored_as = 'view', kept_final = false "
            "WHERE tbl = ? AND step_no = ?",
            [tbl, step_no],
        )
        pruned.append(obj)
    return pruned

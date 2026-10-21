import duckdb
import pytest

from market_anomaly_detection.pipeline import (
    Filter,
    Fix,
    Load,
    Transform,
    count,
    load_inputs,
    prune,
    run_steps,
)

SRC = Load("load", "FROM src")
TRIM = Fix("trim_team", "team", "trim(team)")
ODDS = Filter("odds_above_1", "odds > 1")
ONE_PER_TEAM = Transform(
    "one_per_team",
    "FROM {prev} QUALIFY row_number() OVER (PARTITION BY team ORDER BY match_id) = 1",
)


@pytest.fixture
def con():
    con = duckdb.connect()
    con.execute("""CREATE TABLE src AS FROM (VALUES
        (1, ' Arsenal', 2.0), (2, 'Chelsea', NULL), (3, 'Arsenal ', 0.9), (3, 'Fulham', 1.5)
    ) v(match_id, team, odds)""")
    return con


def kinds(con) -> dict[str, str]:
    rows = con.sql("SELECT table_name, table_type FROM information_schema.tables").fetchall()
    return dict(rows)


def layers(con, final="out") -> dict[str, str]:
    return {n: t for n, t in kinds(con).items() if n.startswith(f"{final}__")}


def built(log) -> list[str]:
    return [r["step"] for r in log if not r["cached"]]


def mutate_src(con):
    """Change the source table. A Load reading a table is not tracked, so a cached Load keeps
    the old data: this makes "was it replayed?" visible in the output."""
    con.execute("INSERT INTO src VALUES (9, 'Zeta', 3.0)")


# ---------------------------------------------------------------- steps


def test_first_step_must_be_load(con):
    with pytest.raises(ValueError):
        run_steps(con, "out", [TRIM])


def test_step_names_must_be_unique(con):
    with pytest.raises(ValueError):
        run_steps(con, "out", [SRC, TRIM, TRIM])


def test_filter_null_predicate_goes_to_dropped(con):
    log = run_steps(con, "out", [SRC, ODDS])
    assert count(con, "out") == 2
    assert count(con, "out__02_odds_above_1__dropped") == 2  # odds 0.9 and odds NULL
    assert log[1]["rows_in"] == 4 and log[1]["rows_out"] == 2 and log[1]["rows_dropped"] == 2


def test_filter_accepts_window_predicate(con):
    run_steps(con, "out", [SRC, Filter("unique_id", "count(*) OVER (PARTITION BY match_id) = 1")])
    assert sorted(r[0] for r in con.sql("SELECT match_id FROM out").fetchall()) == [1, 2]
    assert "_keep" not in con.sql("FROM out").columns


def test_fix_counts_only_changed_rows(con):
    log = run_steps(con, "out", [SRC, TRIM])
    assert log[1]["rows_changed"] == 2
    assert log[1]["rows_in"] == log[1]["rows_out"] == 4
    assert con.sql("SELECT count(DISTINCT team) FROM out").fetchone()[0] == 3


def test_only_the_last_step_is_stored(con):
    log = run_steps(con, "out", [SRC, TRIM, ODDS, ONE_PER_TEAM])
    assert [r["rows_out"] for r in log] == [4, 4, 2, 2]
    assert [r["rows_in"] for r in log[1:]] == [r["rows_out"] for r in log[:-1]]
    assert kinds(con)["out"] == "BASE TABLE"
    assert layers(con) == {
        "out__01_load": "VIEW",
        "out__02_trim_team": "VIEW",
        "out__03_odds_above_1": "VIEW",
        "out__03_odds_above_1__dropped": "VIEW",
    }
    assert con.sql(
        "SELECT step_no, kind, stored_as FROM pipeline_log ORDER BY step_no"
    ).fetchall() == [(1, "load", "view"), (2, "fix", "view"), (3, "filter", "view"),
                     (4, "transform", "table")]  # fmt: skip


def test_materialize_makes_a_table_and_rerun_switches_kind(con):
    run_steps(con, "out", [SRC, Fix("trim_team", "team", "trim(team)", materialize=True), ODDS])
    assert kinds(con)["out__02_trim_team"] == "BASE TABLE"
    run_steps(con, "out", [SRC, TRIM, ODDS])
    assert kinds(con)["out__02_trim_team"] == "VIEW"
    assert count(con, "out") == 2


def test_load_with_rejects_keeps_them(con, tmp_path):
    csv = tmp_path / "t.csv"
    csv.write_text("a,b\n1,2\n3,x\n5,6\n")
    log = run_steps(con, "out", [csv_load(csv), Fix("a_plus_1", "a", "a + 1")])
    assert log[0]["stored_as"] == "table" and log[0]["rows_out"] == 2
    assert log[0]["rows_rejected"] == 1 and count(con, "out__rejects") == 1


def csv_load(csv) -> Load:
    return Load(
        "load",
        f"""FROM read_csv('{csv}', columns = {{'a': 'INTEGER', 'b': 'INTEGER'}},
                          store_rejects = true,
                          rejects_table = 'rej_t', rejects_scan = 'rej_scan_t')""",
        rejects="rej_t",
    )


# ---------------------------------------------------------------- cache


def test_second_run_is_fully_cached(con):
    run_steps(con, "out", [SRC, TRIM, ODDS])
    mutate_src(con)
    log = run_steps(con, "out", [SRC, TRIM, ODDS])
    assert built(log) == []
    assert [r["rows_out"] for r in log] == [4, 4, 2]  # counts come from the cache
    assert count(con, "out") == 2  # not replayed: the new source row is not there
    assert con.sql("SELECT count(*) FROM pipeline_log WHERE cached").fetchone()[0] == 3


def test_appended_step_resumes_from_previous_final(con):
    run_steps(con, "out", [SRC, TRIM, ODDS])
    mutate_src(con)
    log = run_steps(con, "out", [SRC, TRIM, ODDS, ONE_PER_TEAM])
    assert built(log) == ["one_per_team"]
    assert kinds(con)["out__03_odds_above_1"] == "BASE TABLE"  # old final kept as a layer
    assert count(con, "out") == 2 and log[-1]["rows_in"] == 2


def test_changed_step_rebuilds_it_and_everything_after(con):
    run_steps(con, "out", [SRC, TRIM, ODDS, ONE_PER_TEAM])
    log = run_steps(con, "out", [SRC, TRIM, Filter("odds_above_1", "odds > 1.6"), ONE_PER_TEAM])
    assert built(log) == ["odds_above_1", "one_per_team"]
    assert count(con, "out") == 1


def test_removed_step_makes_the_previous_layer_final_without_recompute(con):
    run_steps(con, "out", [SRC, TRIM, ODDS])
    run_steps(con, "out", [SRC, TRIM, ODDS, ONE_PER_TEAM])  # out__03 is now a table layer
    mutate_src(con)
    log = run_steps(con, "out", [SRC, TRIM, ODDS])
    assert built(log) == []
    assert count(con, "out") == 2
    assert "out__03_odds_above_1" not in kinds(con)  # renamed into the final, not copied


def test_removed_steps_drop_stale_objects(con):
    run_steps(con, "out", [SRC, TRIM, ODDS, ONE_PER_TEAM])
    run_steps(con, "out", [SRC, TRIM])
    assert layers(con) == {"out__01_load": "VIEW"}
    steps = con.sql("SELECT step FROM pipeline_cache ORDER BY step_no").fetchall()
    assert steps == [("load",), ("trim_team",)]


def test_force_rebuilds_everything(con):
    run_steps(con, "out", [SRC, TRIM, ODDS])
    mutate_src(con)
    log = run_steps(con, "out", [SRC, TRIM, ODDS], force=True)
    assert built(log) == ["load", "trim_team", "odds_above_1"]
    assert count(con, "out") == 3


def test_changed_source_file_invalidates_the_load(con, tmp_path):
    csv = tmp_path / "t.csv"
    csv.write_text("a,b\n1,2\n3,x\n5,6\n")
    run_steps(con, "out", [csv_load(csv), Fix("a_plus_1", "a", "a + 1")])
    assert built(run_steps(con, "out", [csv_load(csv), Fix("a_plus_1", "a", "a + 1")])) == []
    csv.write_text("a,b\n1,2\n3,4\n5,6\n")
    con.execute("DROP TABLE temp.main.rej_t")  # a fresh process would not have it
    log = run_steps(con, "out", [csv_load(csv), Fix("a_plus_1", "a", "a + 1")])
    assert built(log) == ["load", "a_plus_1"]
    assert count(con, "out") == 3 and log[0]["rows_rejected"] == 0


def test_dropped_layer_object_is_rebuilt(con):
    steps = [SRC, Fix("trim_team", "team", "trim(team)", materialize=True), ODDS]
    run_steps(con, "out", steps)
    con.execute("DROP TABLE out__02_trim_team")
    assert built(run_steps(con, "out", steps)) == ["trim_team", "odds_above_1"]


def test_prune_turns_kept_final_into_a_view_and_keeps_the_chain_working(con):
    run_steps(con, "out", [SRC, TRIM, ODDS])
    run_steps(con, "out", [SRC, TRIM, ODDS, Fix("upper_team", "team", "upper(team)")])
    assert kinds(con)["out__03_odds_above_1"] == "BASE TABLE"
    assert prune(con) == ["out__03_odds_above_1"]
    assert kinds(con)["out__03_odds_above_1"] == "VIEW"
    assert count(con, "out__03_odds_above_1") == 2
    steps = [SRC, TRIM, ODDS, Fix("upper_team", "team", "upper(team)"), ONE_PER_TEAM]
    log = run_steps(con, "out", steps)
    assert built(log) == ["one_per_team"]
    assert count(con, "out") == 2


def test_load_inputs_finds_quoted_existing_files_and_globs(tmp_path):
    (tmp_path / "a.csv").write_text("x\n")
    (tmp_path / "b.csv").write_text("x\n")
    sql = f"FROM read_csv('{tmp_path}/a.csv', sep = ',') UNION ALL FROM '{tmp_path}/*.csv'"
    assert load_inputs(sql) == [tmp_path / "a.csv", tmp_path / "b.csv"]

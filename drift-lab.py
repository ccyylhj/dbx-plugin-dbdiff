"""The write lab: everything the acceptance test cannot do.

`acceptance-test.py` is read-only by contract -- it never executes a statement
against a database, and that is worth keeping. But three things can only be
checked by writing:

1. A **real** column drift. The acceptance test simulates one by editing the
   baseline's record, which cannot reproduce "the old columns hash the same and
   the new column has a value" -- the case the drift rule is actually about.
2. Whether the generated SQL **runs**. Nothing else in this project executes it.
3. Whether `ALTER TABLE ... ADD COLUMN ... NOT NULL` behaves as assumed. It is
   the one statement where the two families are documented to differ.

So this is a separate script, and it is **destructive on purpose**: it creates a
column, fills it, drops it, and applies the generated statements. Point it only
at throwaway databases.

Usage:
    python drift-lab.py [connection] [database] [--keep]

Defaults to `127.0.0.1` / `test_a`. The database must contain the six system
tables; it is left exactly as it was found (the probe column is dropped, and
every statement this script runs is reversible).
"""

import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import testenv  # noqa: E402

EXE = testenv.sidecar_path()
CLI = testenv.cli_path()

# The table has 85 rows on the lab database, which keeps the experiment quick and
# the generated SQL small enough to read.
TABLE = "dsfa_route_version"
IDENTITY = "dsfa_route_version_id"
PROBE = "dbx_drift_probe"
PROBE_ROWS = 5

FAILURES = 0


def check(name, ok, detail=""):
    global FAILURES
    if not ok:
        FAILURES += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{'  ' + detail if detail else ''}")


def sql(connection, statement, write=False):
    """One statement, straight through the CLI. Raises on a server error.

    `write` has to unlock both gates: the CLI blocks writes behind
    `--allow-writes`, and DDL behind `--allow-dangerous-sql` on top of that. That
    second flag is the whole reason this script is separate from the acceptance
    test, and the reason it refuses to point at a database whose name does not
    mark it as disposable.
    """
    args = [CLI, "query", connection, statement, "--json", "--timeout", "120s"]
    if write:
        args += ["--allow-writes", "--allow-dangerous-sql"]
    done = subprocess.run(args, capture_output=True, text=True, encoding="utf-8")
    text = (done.stdout or "") + (done.stderr or "")
    start = text.find("{")
    if start < 0:
        raise RuntimeError(f"{statement[:80]}...\n{text[:400]}")
    payload = json.loads(text[start:])
    if "error" in payload:
        raise RuntimeError(f"{statement[:80]}...\n{payload['error'].get('message')}")
    return payload


# The generated statements name tables without a database, deliberately: the
# target environment's database is usually named something else, so baking the
# source's name in would be wrong. An operator selects it first -- but this CLI
# cannot. `--file` does not chain statements (a two-statement file comes back
# with an empty result) and every `dbx query` is a fresh connection, so a `USE`
# neither runs with the rest nor persists. Qualifying the names here is what a
# `USE` amounts to.
#
# Only in a table position. `dsfa_route_version_id` is a *column* and starts with
# the same prefix, so a blanket rewrite would qualify it too and produce nonsense.
TABLE_REFERENCE = re.compile(
    r"\b(FROM|INTO|TABLE(?:\s+IF\s+NOT\s+EXISTS)?|LIKE|UPDATE|REFERENCES)\s+`([^`]+)`",
    re.IGNORECASE,
)


def qualify(text, database):
    return TABLE_REFERENCE.sub(lambda m: f"{m.group(1)} `{database}`.`{m.group(2)}`", text)


def sql_file(connection, path, database, write=False):
    """A whole .sql file, run against `database`.

    Passing the *contents* as the argument does not work either -- the CLI reads
    a leading `--` as an option, and every one of these files opens with a
    comment banner -- so it goes through `--file`.
    """
    raw = io.open(path, encoding="utf-8").read()
    staged = path + ".staged.sql"
    io.open(staged, "w", encoding="utf-8", newline="\n").write(qualify(raw, database))
    path = staged
    args = [CLI, "query", connection, "--file", path, "--json", "--timeout", "120s"]
    if write:
        args += ["--allow-writes", "--allow-dangerous-sql"]
    done = subprocess.run(args, capture_output=True, text=True, encoding="utf-8")
    text = (done.stdout or "") + (done.stderr or "")
    start = text.find("{")
    if start < 0:
        raise RuntimeError(f"{path}\n{text[:400]}")
    payload = json.loads(text[start:])
    if "error" in payload:
        raise RuntimeError(f"{path}\n{payload['error'].get('message')}")
    return payload


def try_sql(connection, statement):
    """Best effort. Cleanup only, where "it was not there" is success.

    MySQL has no `DROP COLUMN IF EXISTS` -- that is MariaDB -- so the only way to
    make this idempotent is to ignore the error.
    """
    try:
        sql(connection, statement, write=True)
    except RuntimeError:
        pass


class Sidecar:
    def __init__(self, data_dir):
        env = dict(os.environ)
        env["DBX_DBDIFF_DATA_DIR"] = data_dir
        self.proc = subprocess.Popen(
            [EXE], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", env=env,
        )
        self.sequence = 0

    def call(self, method, params=None, timeout=600):
        self.sequence += 1
        request_id = self.sequence
        self.proc.stdin.write(
            json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}) + "\n"
        )
        self.proc.stdin.flush()
        deadline = time.time() + timeout
        while time.time() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError(f"{method}: sidecar closed stdout")
            message = json.loads(line)
            if message.get("id") != request_id:
                continue
            if "error" in message:
                raise RuntimeError(f"{method}: [{message['error'].get('code')}] {message['error'].get('message')}")
            return message["result"]
        raise TimeoutError(f"{method}: no response in {timeout}s")

    def wait(self, timeout=1800):
        deadline = time.time() + timeout
        while time.time() < deadline:
            job = self.call("snapshot/status")
            if job["phase"] != "running":
                if job["phase"] != "done":
                    raise RuntimeError(f"job {job['kind']} ended as {job['phase']}: {job.get('error')}")
                return job
            time.sleep(job.get("pollHintMs", 500) / 1000)
        raise TimeoutError("job did not finish")

    def close(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()


def read(path):
    return open(path, encoding="utf-8").read()


def main():
    argv = [a for a in sys.argv[1:] if not a.startswith("--")]
    keep = "--keep" in sys.argv
    connection = argv[0] if argv else "127.0.0.1"
    database = argv[1] if len(argv) > 1 else "test_a"
    data_dir = os.environ.get("DBX_DBDIFF_LAB_DIR") or tempfile.mkdtemp(prefix="dbx-dbdiff-lab-")
    table = f"`{database}`.`{TABLE}`"
    if not database.startswith("test_") and "--force" not in sys.argv:
        sys.exit(
            f"refusing to run against {database!r}: this script adds and drops columns and "
            f"applies generated DELETE/INSERT statements. Name a throwaway database (the "
            f"convention here is test_*), or pass --force if you are certain."
        )
    print(f"connection = {connection} · database = {database}\ndata dir   = {data_dir}\n")
    print("NOTE: this script writes. Point it only at a throwaway database.\n")

    sidecar = Sidecar(data_dir)
    try:
        # A previous run that died before its own cleanup would otherwise poison
        # this one: `ADD COLUMN` fails outright on a name that is already there.
        for leftover in (PROBE, "dbx_nn_probe"):
            try_sql(connection, f"ALTER TABLE {table} DROP COLUMN `{leftover}`")

        rows = int(sql(connection, f"SELECT COUNT(*) AS n FROM {table}")["rows"][0]["n"])
        print(f"{TABLE} has {rows} rows; the probe column will be {PROBE}\n")

        # ---------------------------------------------------------- baseline
        print("1. snapshot before the column exists")
        sidecar.call("snapshot/create",
                     {"connection": connection, "database": database, "filters": {}, "schemaFilter": ""})
        baseline = sidecar.wait()["result"]["snapshotId"]
        print(f"   snapshot {baseline}")

        # ---------------------------------------------------------- the drift
        print(f"\n2. add {PROBE}, fill it on {PROBE_ROWS} rows, leave the rest NULL")
        sql(connection, f"ALTER TABLE {table} ADD COLUMN `{PROBE}` varchar(32) NULL", write=True)
        sql(
            connection,
            f"UPDATE {table} SET `{PROBE}` = 'probe' WHERE `{IDENTITY}` IN ("
            f"  SELECT id FROM (SELECT `{IDENTITY}` AS id FROM {table}"
            f"   ORDER BY `{IDENTITY}` LIMIT {PROBE_ROWS}) picked)",
            write=True,
        )
        filled = int(sql(connection, f"SELECT COUNT(*) AS n FROM {table} WHERE `{PROBE}` IS NOT NULL")["rows"][0]["n"])
        check("the probe column has exactly the rows we filled", filled == PROBE_ROWS, f"got {filled}")

        # ---------------------------------------------------------- the diff
        print("\n3. compare against the snapshot taken before the column existed")
        sidecar.call("diff/run", {"connection": connection, "database": database, "snapshotId": baseline})
        result = sidecar.wait()["result"]
        delta = next(t for t in result["tables"] if t["table"] == TABLE)
        print(f"   {json.dumps({k: delta[k] for k in ('inserts', 'updates', 'deletes', 'unchanged')}, ensure_ascii=False)}")

        check("the column set is reported as drifted",
              any(d["table"] == TABLE and PROBE in d["added"] for d in result.get("columnDrift") or []),
              str(result.get("columnDrift")))
        check("rows with a value in the new column become updates",
              delta["updates"] == PROBE_ROWS, f"got {delta['updates']}, wanted {PROBE_ROWS}")
        check("rows with the new column empty are left alone",
              delta["unchanged"] == rows - PROBE_ROWS, f"got {delta['unchanged']}")
        check("no inserts and no deletes", delta["inserts"] == 0 and delta["deletes"] == 0,
              f"ins={delta['inserts']} del={delta['deletes']}")

        out_dir = result["outputDir"]
        data_sql = read(os.path.join(out_dir, "03-data.sql"))
        # 03 legitimately holds a DELETE: an update is backup + delete + insert,
        # and that whole sequence is 03's job. What matters is that the rows being
        # written are there and not in 04.
        delete_sql = read(os.path.join(out_dir, "04-delete.sql")) if os.path.exists(
            os.path.join(out_dir, "04-delete.sql")) else ""
        check("the writes are in 03-data.sql",
              data_sql.count(f"INSERT INTO `{TABLE}`") == PROBE_ROWS,
              str(data_sql.count(f"INSERT INTO `{TABLE}`")))
        check("and 04 does not write the table back",
              f"INSERT INTO `{TABLE}`" not in delete_sql)
        first_insert = next(
            (line for line in data_sql.splitlines() if line.startswith(f"INSERT INTO `{TABLE}` (")), "")
        check("each write carries the new column", PROBE in first_insert, first_insert[:200])

        # ------------------------------------------------- generated SQL runs
        print("\n4. execute the generated statements (this is what nothing else checks)")
        payload = sql_file(connection, os.path.join(out_dir, "00-precheck.sql"), database)
        verdicts = [row["verdict"] for row in payload["rows"]]
        check("00-precheck.sql runs and reports only ok", set(verdicts) == {"ok"}, str(verdicts))
        for name in ("03-data.sql", "04-delete.sql"):
            path = os.path.join(out_dir, name)
            if not os.path.exists(path):
                # A file with no statements in it is not generated. This run
                # deletes nothing, so 04 is legitimately absent.
                check(f"{name} is absent because it would be empty",
                      name == "04-delete.sql" and delta["deletes"] == 0,
                      f"{name} missing and delta={delta['deletes']} deletions")
                continue
            try:
                sql_file(connection, path, database, write=True)
                check(f"{name} executes", True)
            except RuntimeError as error:
                check(f"{name} executes", False, str(error)[:300])

        # The rows were deleted and re-inserted with the same values, so the
        # table must be exactly as it was.
        after = int(sql(connection, f"SELECT COUNT(*) AS n FROM {table} WHERE `{PROBE}` IS NOT NULL")["rows"][0]["n"])
        check("applying the generated SQL left the table as it was", after == PROBE_ROWS, f"got {after}")

        # ---------------------------------------------------------- empty case
        print("\n5. the same drift, with the new column empty everywhere")
        sql(connection, f"UPDATE {table} SET `{PROBE}` = NULL", write=True)
        sidecar.call("diff/run", {"connection": connection, "database": database, "snapshotId": baseline})
        empty_result = sidecar.wait()["result"]
        empty_delta = next(t for t in empty_result["tables"] if t["table"] == TABLE)
        check("an empty new column writes nothing",
              empty_delta["updates"] == 0 and empty_delta["inserts"] == 0 and empty_delta["deletes"] == 0,
              json.dumps({k: empty_delta[k] for k in ("inserts", "updates", "deletes")}))
        check("and the drift is still reported",
              any(d["table"] == TABLE for d in empty_result.get("columnDrift") or []))

        # ---------------------------------------------------------- self-consistency
        print("\n6. the snapshot written during the drift must compare clean against itself")
        sidecar.call("diff/run", {"connection": connection, "database": database, "snapshotId": empty_result["snapshotId"]})
        settled = sidecar.wait()["result"]
        totals = {k: settled[k] for k in ("totalInserts", "totalUpdates", "totalDeletes")}
        check("zero differences", all(v == 0 for v in totals.values()), json.dumps(totals))

        # ---------------------------------------------------------- add column DDL
        print("\n7. what the structure channel emits for a NOT NULL column")
        sql(connection, f"ALTER TABLE {table} ADD COLUMN `dbx_nn_probe` varchar(8) NOT NULL", write=True)
        sidecar.call("diff/run", {"connection": connection, "database": database, "snapshotId": baseline})
        nn = sidecar.wait()["result"]
        nn_dir = nn["outputDir"]
        auto = read(os.path.join(nn_dir, "01-schema-auto.sql")) if os.path.exists(os.path.join(nn_dir, "01-schema-auto.sql")) else ""
        review = read(os.path.join(nn_dir, "02-schema-review.sql")) if os.path.exists(os.path.join(nn_dir, "02-schema-review.sql")) else ""
        where = "01" if "dbx_nn_probe" in auto else ("02" if "dbx_nn_probe" in review else "nowhere")
        print(f"   ADD COLUMN ... NOT NULL landed in: {where}")
        check("a NOT NULL add is generated somewhere", where != "nowhere")
        sql(connection, f"ALTER TABLE {table} DROP COLUMN `dbx_nn_probe`", write=True)

        # --------------------------------------------------------------- cleanup
        print("\n8. put the table back")
        sql(connection, f"ALTER TABLE {table} DROP COLUMN `{PROBE}`", write=True)
        # The generated 03 and 04 create backup tables. Those are exactly what the
        # plugin tells an operator not to drop -- but this is a lab, and leaving
        # them would make the next run's schema diff see phantom tables.
        #
        # By the exact names this run generated, never by a LIKE pattern: `_` is a
        # single-character wildcard in SQL, so `'%_bak_%'` also matches tables this
        # run had nothing to do with.
        dropped = []
        for suffix in (result.get("backupTableSuffix"), result.get("deleteBackupTableSuffix")):
            if not suffix:
                continue
            name = f"{TABLE}{suffix}"
            sql(connection, f"DROP TABLE IF EXISTS `{database}`.`{name}`", write=True)
            dropped.append(name)
        print(f"   dropped this run's backup tables: {dropped}")
        sidecar.call("diff/run", {"connection": connection, "database": database, "snapshotId": baseline})
        restored = sidecar.wait()["result"]
        totals = {k: restored[k] for k in ("totalInserts", "totalUpdates", "totalDeletes")}
        check("the table is back to the baseline", all(v == 0 for v in totals.values()), json.dumps(totals))

        print("\n" + ("ALL CHECKS PASSED" if FAILURES == 0 else f"{FAILURES} CHECK(S) FAILED"))
        return 1 if FAILURES else 0
    finally:
        sidecar.close()
        if not keep:
            shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())

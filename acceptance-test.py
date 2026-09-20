"""Acceptance test for the DB Diff sidecar.

Runs the whole pipeline the way the plugin UI does, then checks both channels:

1. A database compared against a snapshot of itself must produce exactly zero
   differences -- on data and on schema.
2. Every difference class is detected in the right direction. Differences are
   injected by *editing the baseline snapshot files*, which is the same shape a
   real change takes: something missing from the baseline is new here and gets a
   statement; something only in the baseline is gone here and gets reported.
3. The generated SQL has the required shape: backup before delete, delete before
   insert, no DROP of any kind, and a guard above every schema statement.

Nothing is executed against any database -- the plugin is read-only and so is
this test.

Usage:
    python acceptance-test.py [connection] [--keep]
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import testenv  # noqa: E402

EXE = testenv.sidecar_path()
FAILURES = 0


def check(name, ok, detail=""):
    global FAILURES
    if not ok:
        FAILURES += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{'  ' + detail if detail else ''}")


class Sidecar:
    def __init__(self, data_dir):
        env = dict(os.environ)
        env["DBX_DBDIFF_DATA_DIR"] = data_dir
        self.data_dir = data_dir
        self.proc = subprocess.Popen(
            [EXE], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", env=env,
        )
        self.sequence = 0

    def call(self, method, params=None, timeout=600):
        self.sequence += 1
        request_id = self.sequence
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}) + "\n")
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
        last = None
        while time.time() < deadline:
            job = self.call("snapshot/status")
            if job["phase"] != "running":
                if job["phase"] != "done":
                    raise RuntimeError(f"job {job['kind']} ended as {job['phase']}: {job.get('error')}")
                return job
            if job.get("detail") != last:
                last = job.get("detail")
                print(f"      ... {last}")
            time.sleep(job.get("pollHintMs", 500) / 1000)
        raise TimeoutError("job did not finish")

    def close(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()


def read_jsonl(path):
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def read(path):
    return open(path, encoding="utf-8").read()


# ------------------------------------------------------------------ dialect
# The plugin takes the same inputs on either family and the files differ only in
# syntax, so the script has to know which syntax to expect. Set from the
# connection's own `type` in main().
DIALECT = "mysql"


def qi(name):
    """An identifier, quoted the way this family quotes."""
    return "`%s`" % name if DIALECT == "mysql" else '"%s"' % name


def like_statement(new, existing):
    """An empty copy of a table. PostgreSQL needs INCLUDING ALL to take the
    indexes with it, and cannot spell it as MySQL does."""
    if DIALECT == "mysql":
        return "CREATE TABLE IF NOT EXISTS %s LIKE %s" % (qi(new), qi(existing))
    return "CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING ALL)" % (qi(new), qi(existing))


def add_column_statement(table, column):
    return "ALTER TABLE %s ADD COLUMN %s" % (qi(table), qi(column))


def retype_statement(table, column):
    """MySQL modifies a column in place; PostgreSQL alters its type."""
    if DIALECT == "mysql":
        return "ALTER TABLE %s MODIFY COLUMN %s" % (qi(table), qi(column))
    return "ALTER TABLE %s ALTER COLUMN %s TYPE" % (qi(table), qi(column))


def index_created(table, index, sql):
    """Whether this file creates the index. A unique index is created with
    UNIQUE in the statement, so the plain form is not a substring of it."""
    if DIALECT == "mysql":
        return "KEY %s" % qi(index) in sql
    return ("CREATE INDEX %s ON %s" % (qi(index), qi(table)) in sql
            or "CREATE UNIQUE INDEX %s ON %s" % (qi(index), qi(table)) in sql)


def main():
    argv = [a for a in sys.argv[1:] if not a.startswith("--")]
    keep = "--keep" in sys.argv
    connection = argv[0] if argv else "timeuse"
    data_dir = os.environ.get("DBX_DBDIFF_TEST_DIR") or tempfile.mkdtemp(prefix="dbx-dbdiff-")
    print(f"connection = {connection}\ndata dir   = {data_dir}\n")

    sidecar = Sidecar(data_dir)
    try:
        info = sidecar.call("host/info")
        print(f"plugin {info['pluginId']} v{info['pluginVersion']} · hash v{info['hashAlgoVersion']} · page {info['pageSize']}")
        print(f"fixed tables: {', '.join(t['table'] for t in info['tables'])}\n")

        # -------------------------------------------------------------- scope
        # A snapshot's identity is (connection, database), so the test has to
        # resolve the same pair the UI does before it can address one.
        print("0a. resolve the connection + database scope")
        listed = sidecar.call("connections/list")["connections"]
        match = next((c for c in listed if c.get("name") == connection), None)
        if match is None:
            raise RuntimeError(f"connection {connection!r} is not in `dbx connections list`")
        global DIALECT
        kind = (match.get("type") or "").lower()
        DIALECT = "mysql" if kind in ("mysql", "mariadb", "doris", "starrocks") else "pg"
        print(f"   dialect = {DIALECT} (connection type {kind!r})")
        database = match.get("database")
        check("the connection reports a database", bool(database), repr(database))
        if not database:
            raise RuntimeError(f"connection {connection!r} has no configured database")
        print(f"   connection = {connection} · database = {database}")

        names = sidecar.call("databases/list", {"connection": connection})["databases"]
        check("that database is in databases/list", database in names, f"got {names[:8]}")
        if DIALECT == "mysql":
            check("MySQL's own schemas are left out of the picker",
                  not {"information_schema", "performance_schema", "mysql", "sys"} & set(names))
        else:
            # A PostgreSQL connection cannot reach another database, so the only
            # honest answer is the one it is connected to.
            check("the pg picker offers the connection's own database", names == [database], str(names))

        # The database is half the identity, so it is required rather than
        # inferred -- there is no "the connection's database" to fall back on.
        try:
            sidecar.call("snapshot/list", {"connection": connection})
            check("snapshot/list without a database is refused", False, "it was accepted")
        except RuntimeError as error:
            check("snapshot/list without a database is refused", "缺少参数 database" in str(error), str(error))

        # ------------------------------------------------------------ snapshot
        print("0b. take a snapshot")
        sidecar.call("snapshot/create",
                     {"connection": connection, "database": database, "filters": {}, "schemaFilter": ""})
        job = sidecar.wait()
        snapshot_id = job["result"]["snapshotId"]
        connections_dir = os.path.join(data_dir, "snapshots")
        scope_dir = os.path.join(connections_dir, os.listdir(connections_dir)[0])
        # One level deeper than before: the database is a path component, not a
        # field filtered on afterwards, so two databases cannot share a list.
        snapshot_dir = os.path.join(scope_dir, os.listdir(scope_dir)[0], snapshot_id)
        meta = json.load(open(os.path.join(snapshot_dir, "meta.json"), encoding="utf-8"))
        check("the snapshot records its connection and database",
              meta["connection"] == connection and meta["database"] == database,
              f"{meta['connection']} / {meta['database']}")
        total_rows = sum(t["rowCount"] for t in meta["dataTables"])
        print(f"   snapshot {snapshot_id}: {total_rows} rows across {len(meta['dataTables'])} tables, "
              f"{meta['schema']['tableCount']} tables of schema\n")
        check("every data table recorded its columns and primary key",
              all(t["columns"] and t["primaryKey"] for t in meta["dataTables"]))
        check("identity is a single column on every table",
              all(len(t["primaryKey"]) == 1 for t in meta["dataTables"]),
              str([t["primaryKey"] for t in meta["dataTables"]]))
        check("ds_active = '1' is baked into five of six filters",
              sum(1 for t in meta["dataTables"] if "ds_active = '1'" in t["filter"]) == 5,
              str([t["filter"] for t in meta["dataTables"]]))
        collapsed = [(t["table"], t["collapsed"]) for t in meta["dataTables"] if t["collapsed"]]
        print(f"   collapsed identities: {collapsed or 'none'}")
        missing_counts = [t["table"] for t in meta["dataTables"] if t["countStar"] is None]
        if missing_counts:
            print(f"   COUNT(*) unavailable for: {missing_counts} (transient query failure -- reported, not fatal)")
        # countStar is optional: a failed COUNT(*) leaves it null rather than
        # aborting a snapshot that is otherwise complete.
        check("collapsed rows are counted, not hidden",
              all(t["countStar"] is None or t["rowCount"] + t["collapsed"] == t["countStar"] for t in meta["dataTables"]),
              "; ".join(f"{t['table']}: {t['rowCount']}+{t['collapsed']}!={t['countStar']}"
                        for t in meta["dataTables"]
                        if t["countStar"] is not None and t["rowCount"] + t["collapsed"] != t["countStar"]))

        # ---------------------------------------------------------- identity
        print("\n1. the same database against its own snapshot -- expect exactly zero differences")
        sidecar.call("diff/run", {"connection": connection, "database": database, "snapshotId": snapshot_id})
        result = sidecar.wait()["result"]
        out_dir = result["outputDir"]
        check("no data inserts", result["totalInserts"] == 0, f"got {result['totalInserts']}")
        check("no data updates", result["totalUpdates"] == 0, f"got {result['totalUpdates']}")
        check("no data deletes", result["totalDeletes"] == 0, f"got {result['totalDeletes']}")
        check("no schema statements", result["autoStatements"] == 0 and result["reviewStatements"] == 0,
              f"auto={result['autoStatements']} review={result['reviewStatements']}")
        # A category with no statements is not written at all, so a clean run
        # produces the precheck and the report and nothing else.
        present = sorted(os.listdir(out_dir))
        for absent in ["01-schema-auto.sql", "02-schema-review.sql", "03-data.sql", "04-delete.sql"]:
            check(f"{absent} is not generated when it would be empty", absent not in present, str(present))
        check("00-precheck.sql is still written", "00-precheck.sql" in present, str(present))
        check("report says which files were skipped", "本次没有生成" in read(os.path.join(out_dir, "report.md")))
        check("the result lists the files it wrote", result.get("files") == ["00-precheck.sql"], str(result.get("files")))
        check("the compare also saved a snapshot", bool(result.get("snapshotId")), str(result.get("snapshotId")))
        listing = sidecar.call("snapshot/list", {"connection": connection, "database": database})
        check("that snapshot is in the list", len(listing["snapshots"]) == 2,
              f"got {[s['snapshotId'] for s in listing['snapshots']]}")

        # ------------------------------------------------------- inject data
        print("\n2. injected data differences on the baseline")
        target = max(meta["dataTables"], key=lambda t: t["rowCount"])
        path = os.path.join(snapshot_dir, target["file"])
        rows = read_jsonl(path)
        if len(rows) < 3:
            print(f"   {target['table']} has only {len(rows)} rows -- skipping data injection")
        else:
            # drop one -> present live, absent baseline -> INSERT
            dropped = rows.pop(0)
            # add a phantom -> present baseline, absent live -> backup + DELETE
            phantom = {"p": ["dbx-phantom-id"], "h": "0" * 64}
            rows.append(phantom)
            # change one hash -> present both, different -> backup + DELETE + INSERT
            changed = rows[0]
            changed["h"] = ("f" * 64) if changed["h"] != "f" * 64 else ("e" * 64)
            write_jsonl(path, rows)
            print(f"   on {target['table']}: dropped 1, added 1 phantom, changed 1 hash")

            sidecar.call("diff/run", {"connection": connection, "database": database, "snapshotId": snapshot_id})
            result = sidecar.wait()["result"]
            out_dir = result["outputDir"]
            # Two files now: 03 writes rows, 04 removes (and backs up) them.
            data_sql = read(os.path.join(out_dir, "03-data.sql"))
            delete_sql = read(os.path.join(out_dir, "04-delete.sql"))
            table_delta = next(t for t in result["tables"] if t["table"] == target["table"])
            print(f"   {json.dumps(table_delta, ensure_ascii=False)}")
            check("one insert detected", table_delta["inserts"] == 1, f"got {table_delta['inserts']}")
            check("one update detected", table_delta["updates"] == 1, f"got {table_delta['updates']}")
            check("one delete detected", table_delta["deletes"] == 1, f"got {table_delta['deletes']}")

            # Two files with two different backup tables: 03 handles what is new
            # or modified, 04 handles what was deleted. Each is runnable alone.
            insert_backup = f"{target['table']}{result['backupTableSuffix']}"
            delete_backup = f"{target['table']}{result['deleteBackupTableSuffix']}"
            check("the two files use different backup tables",
                  result["backupTableSuffix"].startswith("_bak_")
                  and result["deleteBackupTableSuffix"].startswith("_delbak_")
                  and insert_backup != delete_backup,
                  f"{insert_backup} / {delete_backup}")
            check("no _dbx_bak_ prefix left over",
                  "_dbx_bak_" not in data_sql and "_dbx_bak_" not in delete_sql)
            check("no DROP in either data file", "DROP " not in data_sql + delete_sql)

            # 03: backup -> delete -> insert, for the new and modified rows.
            insert_create = like_statement(insert_backup, target["table"])
            insert_select = data_sql.find("INSERT INTO %s SELECT * FROM" % qi(insert_backup))
            insert_delete = data_sql.find("DELETE FROM %s" % qi(target["table"]))
            insert_write = data_sql.find("INSERT INTO %s (" % qi(target["table"]))
            check("03 creates its own backup table", insert_create in data_sql)
            check("03 backs up before deleting", 0 < data_sql.find(insert_create) < insert_select)
            check("03 deletes before inserting", 0 < insert_select < insert_delete < insert_write,
                  f"backup={insert_select} delete={insert_delete} insert={insert_write}")
            check("03 backs up only the ids it deletes",
                  data_sql.count("SELECT * FROM") == 1, str(data_sql.count("SELECT * FROM")))
            check("03 says the whole file can be re-run", "整个重跑" in data_sql)

            # 04: its own backup table, and nothing written back.
            delete_create = like_statement(delete_backup, target["table"])
            delete_select = delete_sql.find("INSERT INTO %s SELECT * FROM" % qi(delete_backup))
            delete_stmt = delete_sql.find("DELETE FROM %s" % qi(target["table"]))
            check("04 creates a separate backup table", delete_create in delete_sql)
            check("04 backs up before deleting", 0 < delete_sql.find(delete_create) < delete_select < delete_stmt,
                  f"backup={delete_select} delete={delete_stmt}")
            check("04 backs up only the ids it deletes",
                  delete_sql.count("SELECT * FROM") == 1, str(delete_sql.count("SELECT * FROM")))
            check("04 does not reuse the backup table 03 made", qi(insert_backup) not in delete_sql)
            check("04 never writes a row back",
                  "INSERT INTO %s" % qi(target["table"]) not in delete_sql)
            check("04 says it is removals only", "只删不写" in delete_sql)

            # The id sets are disjoint, which is what makes the order between the
            # two files free -- and what makes each safe to run alone.
            inserted_key = str(dropped["p"][0])
            modified_key = str(changed["p"][0])
            check("the deleted id is only in 04",
                  "dbx-phantom-id" in delete_sql and "dbx-phantom-id" not in data_sql)
            check("an arriving id is only in 03",
                  inserted_key in data_sql and inserted_key not in delete_sql)
            check("a modified id is only in 03",
                  modified_key in data_sql and modified_key not in delete_sql)
            check("03's delete list covers the modified ids too",
                  modified_key in data_sql[:insert_write], "no insert statement found" if insert_write < 0 else "")

            # One INSERT per row, which is the point of 03's shape.
            table_inserts = data_sql.count("INSERT INTO %s (" % qi(target["table"]))
            check("one INSERT statement per arriving row",
                  table_inserts == table_delta["inserts"] + table_delta["updates"],
                  f"{table_inserts} statements for {table_delta['inserts']}+{table_delta['updates']} rows")
            insert_lines = [line for line in data_sql.splitlines()
                            if line.startswith("INSERT INTO %s (" % qi(target["table"]))]
            check("every INSERT is a single-row, single-line statement",
                  all(line.count("VALUES (") == 1 and line.endswith(");") for line in insert_lines),
                  str(insert_lines[:1]))

            check("the report says the two id sets do not overlap",
                  "id 集合不相交" in result["report"])

        # ------------------------------------------- column drift since the baseline
        print("\n2b. a table that gained a column since the baseline")
        # Simulated by taking a column *off* the baseline's record, which is the
        # same situation seen from the other side: the baseline covers fewer
        # columns than the table has. The artificial part is that its stored
        # hashes still cover the full set, so every row in that one table reads
        # as modified -- which is why the smallest table is the one to use.
        meta_path = os.path.join(snapshot_dir, "meta.json")
        original_meta = read(meta_path)
        baseline_meta = json.loads(original_meta)
        victim = min(baseline_meta["dataTables"], key=lambda t: t["rowCount"])
        removed = victim["columns"][-1]
        victim["columns"] = victim["columns"][:-1]
        with open(meta_path, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(baseline_meta, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        print(f"   baseline {victim['table']} trimmed to {len(victim['columns'])} columns"
              f" (dropped {removed!r} from the record)")

        try:
            sidecar.call("diff/run",
                         {"connection": connection, "database": database, "snapshotId": snapshot_id})
            drift_result = sidecar.wait()["result"]
            drift = drift_result.get("columnDrift") or []
            check("a changed column set is reconciled instead of refused",
                  any(d["table"] == victim["table"] and removed in d["added"] for d in drift),
                  str(drift))
            check("the report says the column set drifted", "列明细漂移" in drift_result["report"])

            drifted_id = drift_result["snapshotId"]
            drifted_dir = os.path.join(os.path.dirname(snapshot_dir), drifted_id)
            drifted_meta = json.load(open(os.path.join(drifted_dir, "meta.json"), encoding="utf-8"))
            recorded = next(t for t in drifted_meta["dataTables"] if t["table"] == victim["table"])
            check("the new snapshot records the table's current columns",
                  len(recorded["columns"]) == len(victim["columns"]) + 1,
                  f"{len(recorded['columns'])} recorded vs {len(victim['columns']) + 1} live")

            # The point of hashing twice. The file just written holds hashes over
            # the *live* columns and a `columns` list saying so; if those two
            # disagreed, comparing against this snapshot would report every row
            # as modified. Nothing else tests that they agree.
            sidecar.call("diff/run",
                         {"connection": connection, "database": database, "snapshotId": drifted_id})
            settled = sidecar.wait()["result"]
            totals = {k: settled[k] for k in ("totalInserts", "totalUpdates", "totalDeletes")}
            check("the snapshot written during a drift compares clean against itself",
                  all(value == 0 for value in totals.values()), str(totals))
        finally:
            # Put the baseline back so the later steps see what they expect.
            with open(meta_path, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(original_meta)

        # ----------------------------------------------------- inject schema
        print("\n3. injected schema differences on the baseline")
        schema_path = os.path.join(snapshot_dir, "schema.jsonl")
        objects = read_jsonl(schema_path)
        column = next(o for o in objects if o["o"] == "column" and o["d"].get("nullable") and o["n"] != "id")
        # Two columns retyped in opposite directions. It is the *baseline* that
        # gets edited, so a baseline narrower than live is a widening (safe, and
        # it belongs in 01) and a baseline wider than live is a narrowing.
        def varchar_width(obj):
            text = obj["d"].get("columnType", "")
            if not text.startswith("varchar("):
                return None
            return int(text[len("varchar("):-1])

        varchars = [o for o in objects if o["o"] == "column" and varchar_width(o) is not None
                    and (o["t"], o["n"]) != (column["t"], column["n"])]
        wider = next(o for o in varchars if varchar_width(o) >= 10)
        narrower = next(o for o in varchars if o is not wider and varchar_width(o) <= 255)
        wider_live = wider["d"]["columnType"]
        # PostgreSQL's primary key is an index with a generated name, not one
        # called PRIMARY, and there is no engine to change.
        index = next(o for o in objects
                     if o["o"] == "index" and not o["d"].get("primary") and o["n"] != "PRIMARY")
        table = next(o for o in objects
                     if o["o"] == "table" and o["t"] not in (column["t"], wider["t"], narrower["t"]))
        engine_table = next((o for o in objects
                             if o["o"] == "table" and o["d"].get("engine") == "InnoDB"), None)

        objects = [o for o in objects if (o["t"], o["o"], o["n"]) != (column["t"], "column", column["n"])]
        objects = [o for o in objects if (o["t"], o["o"], o["n"]) != (index["t"], "index", index["n"])]
        objects = [o for o in objects if o["t"] != table["t"]]
        wider["d"] = {**wider["d"], "columnType": "varchar(3)"}
        narrower["d"] = {**narrower["d"], "columnType": "varchar(1000)"}
        if engine_table is not None:
            # Only MySQL has a table engine to change.
            engine_table["d"] = {**engine_table["d"], "engine": "MyISAM"}
        objects.append({"t": column["t"], "o": "column", "n": "dbx_phantom_column",
                        "d": {"columnType": "int", "nullable": True, "default": None, "extra": "",
                              "charset": None, "collation": None, "ordinal": 999}})
        objects.append({"t": "dbx_phantom_table", "o": "table", "n": "",
                        "d": {"engine": "InnoDB", "collation": "utf8mb4_general_ci"}})
        write_jsonl(schema_path, objects)
        print(f"   dropped column {column['n']}, dropped index {index['n']}, dropped table {table['t']};"
              f" widened {wider['n']} (varchar(3) -> {wider_live}), narrowed {narrower['n']} (varchar(1000));"
              + (f" engine {engine_table['t']};" if engine_table is not None else "")
              + " added 2 phantoms")

        sidecar.call("diff/run", {"connection": connection, "database": database, "snapshotId": snapshot_id})
        result = sidecar.wait()["result"]
        out_dir = result["outputDir"]
        # Every category has statements here, so every file must exist.
        for required in ["01-schema-auto.sql", "02-schema-review.sql", "03-data.sql", "04-delete.sql"]:
            check(f"{required} is written when it has statements",
                  os.path.exists(os.path.join(out_dir, required)), str(sorted(os.listdir(out_dir))))
        auto_sql = read(os.path.join(out_dir, "01-schema-auto.sql"))
        review_sql = read(os.path.join(out_dir, "02-schema-review.sql"))
        report = result["report"]

        check("ADD COLUMN in auto file", add_column_statement(column["t"], column["n"]) in auto_sql)
        check("index created in auto file", index_created(index["t"], index["n"], auto_sql),
              str([line for line in auto_sql.splitlines() if "INDEX" in line][:2]))
        check("CREATE TABLE in auto file", "CREATE TABLE %s " % qi(table["t"]) in auto_sql)
        if engine_table is not None:
            check("engine change in auto file",
                  f"ALTER TABLE `{engine_table['t']}` ENGINE=InnoDB" in auto_sql)
        else:
            check("no engine statement where the family has no engine", "ENGINE=" not in auto_sql)
        check("widening type change goes to auto",
              retype_statement(wider["t"], wider["n"]) in auto_sql, f"varchar(3) -> {wider_live}")
        check("narrowing type change goes to review",
              retype_statement(narrower["t"], narrower["n"]) in review_sql)
        check("the widening is not also in review", qi(wider["n"]) not in review_sql)
        # The detection rows were the only thing in these files that read
        # information_schema, so their absence is the check. Matching on the word
        # "检测" alone was not: a comment elsewhere in the file can contain it.
        check("no detection statements in the auto file", "information_schema" not in auto_sql)
        check("no detection statements in the review file", "information_schema" not in review_sql)
        check("the backup table is described as an empty template",
              "空表" in read(os.path.join(out_dir, "04-delete.sql")))
        combined = auto_sql + review_sql
        for keyword in ["DROP TABLE", "DROP COLUMN", "DROP INDEX", "DROP FOREIGN KEY"]:
            check(f"no {keyword}", keyword not in combined)
        check("phantom column only reported", "dbx_phantom_column" in report)
        check("phantom table only reported", "dbx_phantom_table" in report)

        # -------------------------------------------------------- precheck
        print("\n4. precheck file")
        precheck = read(os.path.join(out_dir, "00-precheck.sql"))
        for t in meta["dataTables"]:
            if "FROM %s" % qi(t["table"]) not in precheck:
                check(f"precheck covers {t['table']}", False)
                break
        else:
            check("precheck covers all six tables", True)
        check("precheck is read-only", "SELECT" in precheck and "DELETE" not in precheck and "UPDATE" not in precheck)
        # One statement, not six: the whole precheck is one thing to run.
        check("precheck is a single statement", precheck.count(";") == 1, str(precheck.count(";")))
        check("precheck joins the tables with UNION ALL",
              precheck.count("UNION ALL") == 5, str(precheck.count("UNION ALL")))
        # UNION and not UNION ALL would merge two empty tables' rows into one.
        check("precheck uses UNION ALL, not UNION",
              precheck.count("UNION") == precheck.count("UNION ALL"))
        check("mismatch verdict reads MISMATCH -- 数量不一致",
              "'MISMATCH -- 数量不一致'" in precheck)
        check("every table carries its own filter into the precheck",
              "WHERE ds_active = '1'" in precheck)

        # ------------------------------------------------- confirm flow
        print("\n5. changed filter asks for confirmation instead of running")
        # Leading "and" on purpose: the UI says not to write it, but typing it
        # must normalize, not produce "... AND and ...".
        changed = sidecar.call("diff/run", {
            "connection": connection,
            "database": database,
            "snapshotId": snapshot_id,
            "filters": {"dsfa_rm": "and ds_version = 'project'", target["table"]: "1 = 1"},
            "schemaFilter": "",
        })
        check("returns needsConfirm instead of starting", changed.get("needsConfirm") is True,
              str({k: v for k, v in changed.items() if k != "filterDiffs"}))
        diffs = {d["table"]: d for d in changed.get("filterDiffs", [])}
        check("names the changed table", "dsfa_rm" in diffs, str(sorted(diffs)))
        check("shows baseline vs current",
              "ds_active = '1'" in diffs.get("dsfa_rm", {}).get("baseline", "")
              and "project" in diffs.get("dsfa_rm", {}).get("current", ""),
              str(diffs.get("dsfa_rm")))
        check("no job was started", sidecar.call("snapshot/status")["phase"] != "running")
        forced = sidecar.call("diff/run", {
            "connection": connection,
            "database": database,
            "snapshotId": snapshot_id,
            "filters": {"dsfa_rm": "and ds_version = 'project'", target["table"]: "1 = 1"},
            "schemaFilter": "",
            "force": True,
        })
        check("force starts the job", forced.get("started") is True)
        forced_result = sidecar.wait()["result"]
        check("forced run records the used filter",
              any(t["table"] == "dsfa_rm" and "project" in t["filter"] for t in forced_result["tables"]),
              str([(t["table"], t["filter"]) for t in forced_result["tables"] if t["table"] == "dsfa_rm"]))
        # The report's filter list has to show what this run used, not what the
        # baseline recorded -- showing the baseline's was the bug.
        section = forced_result["report"].split("## 每张表的过滤条件")[1].split("##")[0]
        check("the report's filter list shows what this run used",
              "project" in section, repr(section.strip()[:200]))

        # And so does every file that runs SQL on the target. The precheck is the
        # one that matters: its WHERE is a statement the operator executes, so
        # the old filter meant validating the wrong rows.
        forced_dir = forced_result["outputDir"]
        forced_precheck = read(os.path.join(forced_dir, "00-precheck.sql"))
        check("the precheck validates against the filter this run used",
              "WHERE ds_active = '1' AND (ds_version = 'project')" in forced_precheck,
              repr(forced_precheck[forced_precheck.find("FROM `dsfa_rm`"):][:120]))
        for name in ("03-data.sql", "04-delete.sql"):
            path = os.path.join(forced_dir, name)
            if not os.path.exists(path) or target["table"] not in read(path):
                check(f"{name} covers {target['table']}", False, "not in the file")
                continue
            body = read(path)
            # The filter comment sits inside the table block, right under its
            # "===== table：..." banner.
            block = body.split(f"-- ===== {target['table']}：", 1)[1].split("-- ===== ")[0]
            check(f"{name} carries the filter this run used",
                  "-- 过滤: ds_active = '1' AND (1 = 1)" in block,
                  repr([line for line in block.splitlines() if "过滤" in line]))
        check("the report no longer carries the DDL footer",
              "隐式提交" not in forced_result["report"])

        print("\n6. report")
        check("report has the data table", "## 数据差异" in report)
        check("report has the schema table", "## 结构差异" in report)
        check("report lists the generated files", "## 生成的文件" in report)

        print(f"\noutput: {out_dir}")
        print("ALL CHECKS PASSED" if FAILURES == 0 else f"{FAILURES} CHECK(S) FAILED")
        return 1 if FAILURES else 0
    finally:
        sidecar.close()
        if not keep:
            shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())

"""
SqlRunner — discovers and executes versioned .sql files.

Contract for SQL files
----------------------
Each .sql file must:
  - Use psycopg2 named-parameter syntax: %(param_name)s
  - Consist of a single statement (typically a data-modifying CTE)
  - Return exactly one row whose columns are all BIGINT counters suitable
    for structured logging (e.g. rejects_written, staging_written)

Files are discovered from a directory, sorted lexicographically.  Numeric
prefixes (001_, 002_, ...) control execution order.

Versioning semantics
--------------------
These files are NOT Alembic-style one-shot migrations.  They are idempotent
transforms run on every pipeline invocation.  Adding a new file (003_...) or
editing an existing one takes effect on the next pipeline run with no
migration bookkeeping required.  All writes in the SQL files use
ON CONFLICT clauses to make repeated execution safe.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import psycopg2.extras
import structlog

log = structlog.get_logger()


class SqlRunner:
    """
    Execute every ``*.sql`` file in *sql_dir* in lexicographic order.

    Parameters
    ----------
    sql_dir:
        Directory containing the .sql files to run.  Sub-directories are
        not descended into; only files matching ``*.sql`` at the top level
        of *sql_dir* are executed.
    """

    def __init__(self, sql_dir: Path) -> None:
        self.sql_dir = sql_dir

    def run(
        self,
        conn: Any,           # psycopg2.extensions.connection
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """
        Execute all .sql files and return a list of result dicts.

        Each result dict has the form::

            {"file": "001_demand.sql", "rejects_written": 3, "staging_written": 720}

        The *conn* must be an open, uncommitted psycopg2 connection.  The
        caller is responsible for ``commit()`` / ``rollback()``.

        Raises
        ------
        FileNotFoundError
            If *sql_dir* does not exist.
        psycopg2.Error
            On any database error; the connection is left in a failed
            transaction state — the caller must rollback.
        """
        if not self.sql_dir.is_dir():
            raise FileNotFoundError(
                f"SQL directory not found: {self.sql_dir}"
            )

        paths = sorted(self.sql_dir.glob("*.sql"))
        if not paths:
            log.warning("sql_runner_no_files_found", dir=str(self.sql_dir))
            return []

        results: list[dict[str, Any]] = []
        for path in paths:
            row = self._execute_file(conn, path, params)
            results.append({"file": path.name, **row})
        return results

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _execute_file(
        self,
        conn: Any,
        path: Path,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        sql = path.read_text(encoding="utf-8")
        log.info("sql_runner_executing", file=path.name)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        result = dict(row) if row else {}
        log.info("sql_runner_done", file=path.name, **result)
        return result

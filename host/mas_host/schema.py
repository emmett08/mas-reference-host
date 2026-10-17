"""Versioned SQLite repositories shared by host-side persistence adapters.

Opening a known old format is an explicit transactional migration. Read-only
inspection recognises that format without migrating it. Unknown identities,
versions or layouts are rejected before schema or application writes. Writable
opens permit SQLite's own crash rollback before inspecting the recovered state.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
import re
import sqlite3
from typing import Callable


class SchemaError(RuntimeError):
    pass


@dataclass(frozen=True)
class Schema:
    name: str
    application_id: int
    version: int
    legacy: tuple[tuple[int, int], ...]
    statements: tuple[str, ...]
    migrations: tuple[tuple[Schema, tuple[str, ...]], ...] = ()

    @cached_property
    def layout(self):
        with closing(sqlite3.connect(":memory:")) as db:
            for statement in self.statements:
                db.execute(statement)
            return _layout(db)


_SQL_TOKENS = re.compile(
    r"' (?: [^'] | '' )* ' | \" (?: [^\"] | \"\" )* \" | ` (?: [^`] | `` )* `"
    r" | \[ [^\]]* \] | --[^\n]* | /\* .*? \*/"
    r" | [A-Za-z_][A-Za-z_0-9$]* | [0-9]+(?:\.[0-9]*)?(?:[eE][+-]?[0-9]+)?"
    r" | ->> | -> | \|\| | << | >> | <= | >= | <> | != | == | \S",
    re.VERBOSE | re.DOTALL,
)


def _sql_tokens(source: str):
    # Keep token boundaries and quoted bytes. Removing every space or folding
    # quoted text would equate different CHECKs/defaults (for example 'A B'
    # and 'ab'). ASCII SQL keywords and unquoted names are case-insensitive.
    tokens = []
    for match in _SQL_TOKENS.finditer(source):
        token = match.group()
        if token.startswith(('--', '/*')):
            continue
        tokens.append(token.lower() if token[0].isascii() and (token[0].isalpha() or token[0] == '_') else token)
    return tuple(token for index, token in enumerate(tokens)
                 if not any(tokens[start:start + 3] == ['if', 'not', 'exists']
                            for start in range(max(0, index - 2), index + 1)))


def _layout(db: sqlite3.Connection):
    # The exact declared layout includes constraints, indices, views and triggers.
    return tuple((kind, name, table, _sql_tokens(sql or ""))
        for kind, name, table, sql in db.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"))


LEDGER_V2 = Schema("deployment ledger", 0x4D415344, 2, ((0, 1),), (
    "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
    "CREATE TABLE runs(run_id TEXT PRIMARY KEY,directory TEXT UNIQUE NOT NULL,admitted_at INTEGER NOT NULL,purged_at INTEGER)",
    "CREATE TABLE calls(run_id TEXT NOT NULL,call_id TEXT NOT NULL,reserved_microusd INTEGER NOT NULL,admitted_at INTEGER NOT NULL,PRIMARY KEY(run_id,call_id),FOREIGN KEY(run_id) REFERENCES runs(run_id))",
))
_BUDGET_AUDIT = "CREATE TABLE budget_amendments(revision INTEGER PRIMARY KEY,request_key TEXT UNIQUE NOT NULL,digest TEXT NOT NULL,principal TEXT NOT NULL,reason TEXT NOT NULL,policy TEXT NOT NULL,created INTEGER NOT NULL)"
LEDGER_V3 = Schema("deployment ledger", 0x4D415344, 3, (),
                (*LEDGER_V2.statements, _BUDGET_AUDIT),
                ((LEDGER_V2, (_BUDGET_AUDIT,)),))
_PRINCIPAL_QUOTAS = (
    "CREATE TABLE principal_quotas(principal TEXT PRIMARY KEY,policy TEXT NOT NULL)",
    "CREATE TABLE run_principals(run_id TEXT PRIMARY KEY,principal TEXT NOT NULL,FOREIGN KEY(run_id) REFERENCES runs(run_id))",
    "CREATE INDEX run_principal_owner ON run_principals(principal,run_id)",
)
LEDGER_V4 = Schema("deployment ledger", 0x4D415344, 4, (),
                (*LEDGER_V3.statements, *_PRINCIPAL_QUOTAS),
                ((LEDGER_V3, _PRINCIPAL_QUOTAS),
                 (LEDGER_V2, (_BUDGET_AUDIT, *_PRINCIPAL_QUOTAS))))
from .accounting_schema import ACCOUNTING_TABLES
LEDGER = Schema('deployment ledger', 0x4D415344, 5, (),
                (*LEDGER_V4.statements, *ACCOUNTING_TABLES),
                ((LEDGER_V4, ACCOUNTING_TABLES),
                 (LEDGER_V3, (*_PRINCIPAL_QUOTAS, *ACCOUNTING_TABLES)),
                 (LEDGER_V2, (_BUDGET_AUDIT, *_PRINCIPAL_QUOTAS, *ACCOUNTING_TABLES))))
JOURNAL = Schema("host journal", 0x4D41534A, 2, ((0, 0), (0x4D41534A, 1)), (
    "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
    "CREATE TABLE steps(key TEXT PRIMARY KEY,kind TEXT NOT NULL,input TEXT NOT NULL,data TEXT NOT NULL)",
    "CREATE TABLE invocations(call_id TEXT PRIMARY KEY,step_key TEXT NOT NULL,actor TEXT NOT NULL,state TEXT NOT NULL,output TEXT,evidence TEXT,reservation_microusd INTEGER)",
    "CREATE TABLE resolutions(id INTEGER PRIMARY KEY,call_id TEXT NOT NULL,decision TEXT NOT NULL,evidence TEXT NOT NULL,created INTEGER NOT NULL)",
))
JOBS = Schema("Cursor jobs", 0x4D415343, 1, (), (
    "CREATE TABLE jobs(job_id TEXT PRIMARY KEY,spec TEXT NOT NULL,binding TEXT NOT NULL,agent_id TEXT UNIQUE NOT NULL,run_id TEXT UNIQUE,state TEXT NOT NULL,snapshot TEXT,host_run_dir TEXT,call_id TEXT,cancel_requested INTEGER NOT NULL DEFAULT 0,created INTEGER NOT NULL,updated INTEGER NOT NULL)",
    "CREATE TABLE audit(id INTEGER PRIMARY KEY,job_id TEXT NOT NULL,event TEXT NOT NULL,evidence TEXT NOT NULL,created INTEGER NOT NULL)",
))


def validate(db: sqlite3.Connection, schema: Schema) -> bool:
    """Return whether migration is needed; never change the database."""
    identity = (db.execute("PRAGMA application_id").fetchone()[0],
                db.execute("PRAGMA user_version").fetchone()[0])
    for prior, _ in schema.migrations:
        if identity in ((prior.application_id, prior.version), *prior.legacy):
            if _layout(db) != prior.layout:
                raise SchemaError(f"unsupported {schema.name} prior layout")
            return True
    if identity != (schema.application_id, schema.version) and identity not in schema.legacy:
        raise SchemaError(f"unsupported {schema.name} identity or version")
    if _layout(db) != schema.layout:
        raise SchemaError(f"unsupported {schema.name} layout")
    return identity != (schema.application_id, schema.version)


def connect(path: Path, schema: Schema, *, readonly: bool = False,
            initialise: Callable[[sqlite3.Connection], None] | None = None,
            validate_content: Callable[[sqlite3.Connection], None] | None = None) -> sqlite3.Connection:
    """Open an identified repository; only a new file can be initialised.

    The caller serialises initial creation. On an existing file, both preflight
    inspection and transaction-local validation precede schema/application
    mutations. SQLite crash recovery is allowed only for writable openings.
    """
    path = Path(path).absolute()
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise SchemaError("database must be a regular file")
    exists = path.exists()
    if not exists and (readonly or initialise is None):
        raise SchemaError(f"missing {schema.name}")
    fresh = not exists
    if exists:
        # A real machine reset can leave a hot rollback journal. Even SELECT
        # needs SQLite to restore that transaction before reading its schema;
        # mode=ro fails with SQLITE_READONLY_ROLLBACK. Permit native recovery for
        # a writable owner, while query_only still forbids SQL data/DDL writes
        # during preflight. Diagnostic readonly opens never perform recovery.
        mode = 'ro' if readonly else 'rw'
        with closing(sqlite3.connect(path.as_uri() + '?mode=' + mode, uri=True, timeout=10)) as check:
            check.execute('PRAGMA query_only=ON')
            fresh = (initialise is not None and not _layout(check)
                     and check.execute("PRAGMA application_id").fetchone()[0] == 0
                     and check.execute("PRAGMA user_version").fetchone()[0] == 0)
            if not fresh:
                validate(check, schema)
                if validate_content:
                    validate_content(check)
    if readonly:
        db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=10)
        try:
            db.execute("PRAGMA foreign_keys=ON")
            validate(db, schema)
            if validate_content:
                validate_content(db)
        except Exception:
            db.close()
            raise
    else:
        db = sqlite3.connect(path, timeout=10)
        try:
            db.execute("PRAGMA foreign_keys=ON")
            # DELETE-journal commits also need the journal directory synced:
            # FULL can lose the last committed transaction after an OS reset.
            db.execute("PRAGMA synchronous=EXTRA")
            db.execute("BEGIN IMMEDIATE")
            if not fresh:
                migrate = validate(db, schema)
                if validate_content:
                    validate_content(db)
            else:
                # Do not initialise over a file populated after preflight.
                if _layout(db) or any(db.execute(f"PRAGMA {key}").fetchone()[0]
                                      for key in ("application_id", "user_version")):
                    raise SchemaError(f"{schema.name} appeared during initialisation")
                for statement in schema.statements:
                    db.execute(statement)
                initialise(db)
                migrate = True
            if migrate:
                identity = (db.execute("PRAGMA application_id").fetchone()[0],
                            db.execute("PRAGMA user_version").fetchone()[0])
                for prior, statements in schema.migrations:
                    if identity in ((prior.application_id, prior.version), *prior.legacy):
                        for statement in statements:
                            db.execute(statement)
                        break
                if _layout(db) != schema.layout:
                    raise SchemaError(f"invalid {schema.name} migrated layout")
                db.execute(f"PRAGMA application_id={schema.application_id}")
                db.execute(f"PRAGMA user_version={schema.version}")
            db.commit()
            path.chmod(0o600)
        except Exception:
            db.rollback()
            db.close()
            raise
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=10000")
    return db

#!/usr/bin/env python3
"""
Migrate project data from MySQL to SQLite.
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

try:
    import pymysql
except ModuleNotFoundError:
    print("Missing dependency: pymysql", file=sys.stderr)
    print("Install with: pip install pymysql", file=sys.stderr)
    sys.exit(1)


TABLE_COLUMNS: Dict[str, Sequence[str]] = {
    "gallery": (
        "gid",
        "token",
        "archiver_key",
        "title",
        "title_jpn",
        "category",
        "thumb",
        "uploader",
        "posted",
        "filecount",
        "filesize",
        "expunged",
        "removed",
        "replaced",
        "rating",
        "torrentcount",
        "root_gid",
        "bytorrent",
    ),
    "tag": ("id", "name"),
    "gid_tid": ("gid", "tid"),
    "torrent": ("id", "gid", "name", "hash", "addedstr", "fsizestr", "uploader", "expunged"),
}


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def extract_js_default(text: str, key: str, default: str) -> str:
    # key: process.env.KEY || 'value'
    pattern_str = rf"{re.escape(key)}\s*:\s*[^,\n]*\|\|\s*'([^']*)'"
    m = re.search(pattern_str, text)
    if m:
        return m.group(1)
    pattern_num = rf"{re.escape(key)}\s*:\s*[^,\n]*\|\|\s*(\d+)"
    m = re.search(pattern_num, text)
    if m:
        return m.group(1)
    return default


def load_config_defaults(config_path: Path) -> Dict[str, str]:
    text = read_text(config_path)
    return {
        "dbHost": extract_js_default(text, "dbHost", "localhost"),
        "dbPort": extract_js_default(text, "dbPort", "3306"),
        "dbName": extract_js_default(text, "dbName", "e-hentai-db"),
        "dbUser": extract_js_default(text, "dbUser", "root"),
        "dbPass": extract_js_default(text, "dbPass", ""),
        "sqlitePath": extract_js_default(text, "sqlitePath", "./e-hentai-db.sqlite3"),
    }


def env_or_default(env_key: str, default: str) -> str:
    value = os.getenv(env_key)
    if value is None or value == "":
        return default
    return value


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    default_config = repo_root / "config.js"
    defaults = load_config_defaults(default_config)

    parser = argparse.ArgumentParser(description="Migrate data from MySQL to SQLite for e-hentai-db")
    parser.add_argument("--mysql-host", default=env_or_default("DB_HOST", defaults["dbHost"]))
    parser.add_argument("--mysql-port", type=int, default=int(env_or_default("DB_PORT", defaults["dbPort"])))
    parser.add_argument("--mysql-db", default=env_or_default("DB_NAME", defaults["dbName"]))
    parser.add_argument("--mysql-user", default=env_or_default("DB_USER", defaults["dbUser"]))
    parser.add_argument("--mysql-pass", default=env_or_default("DB_PASS", defaults["dbPass"]))
    parser.add_argument("--sqlite-path", default=env_or_default("SQLITE_PATH", defaults["sqlitePath"]))
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--drop-existing", action="store_true", help="Drop destination sqlite db file before migrating")
    return parser.parse_args()


def normalize_cell(value):
    if isinstance(value, bool):
        return 1 if value else 0
    return value


def normalize_rows(rows: Iterable[Sequence[object]]) -> List[Tuple[object, ...]]:
    return [tuple(normalize_cell(c) for c in row) for row in rows]


def ensure_schema(sqlite_conn: sqlite3.Connection, schema_path: Path) -> None:
    sqlite_conn.executescript(read_text(schema_path))
    sqlite_conn.commit()


def copy_table(
    mysql_conn,
    sqlite_conn: sqlite3.Connection,
    table: str,
    columns: Sequence[str],
    batch_size: int,
) -> int:
    mysql_col_sql = ", ".join(f"`{c}`" for c in columns)
    sqlite_col_sql = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    insert_mode = "IGNORE" if table == "gid_tid" else "REPLACE"
    insert_sql = f"INSERT OR {insert_mode} INTO {table} ({sqlite_col_sql}) VALUES ({placeholders})"

    with mysql_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM `{table}`")
        total = int(cur.fetchone()[0])

    copied = 0
    with mysql_conn.cursor(pymysql.cursors.SSCursor) as cur:
        cur.execute(f"SELECT {mysql_col_sql} FROM `{table}`")
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            sqlite_conn.executemany(insert_sql, normalize_rows(rows))
            copied += len(rows)
            if copied % (batch_size * 5) == 0 or copied == total:
                print(f"[{table}] copied {copied}/{total}")
    sqlite_conn.commit()
    return copied


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    sqlite_path = Path(args.sqlite_path)
    if not sqlite_path.is_absolute():
        sqlite_path = (repo_root / sqlite_path).resolve()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    if args.drop_existing and sqlite_path.exists():
        sqlite_path.unlink()

    print(
        "Connecting MySQL:",
        f"{args.mysql_user}@{args.mysql_host}:{args.mysql_port}/{args.mysql_db}",
    )
    mysql_conn = pymysql.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_pass,
        database=args.mysql_db,
        charset="utf8mb4",
        autocommit=True,
    )

    print(f"Opening SQLite: {sqlite_path}")
    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.execute("PRAGMA synchronous = NORMAL")

    ensure_schema(sqlite_conn, repo_root / "struct.sql")

    migrated = {}
    try:
        for table, columns in TABLE_COLUMNS.items():
            migrated[table] = copy_table(mysql_conn, sqlite_conn, table, columns, args.batch_size)
    finally:
        sqlite_conn.close()
        mysql_conn.close()

    print("Migration completed.")
    for table in ("gallery", "tag", "gid_tid", "torrent"):
        print(f"  {table}: {migrated.get(table, 0)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

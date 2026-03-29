#!/usr/bin/env python3
"""
Import records missing in MySQL `gallery` (by gid) from PostgreSQL.

This script syncs:
1) gallery rows
2) tags (PG gallery.tags jsonb -> MySQL tag + gid_tid)
3) torrent rows (for the missing gids only)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterator, List, Optional, Sequence, Tuple


def env_first(keys: Sequence[str], default: str) -> str:
    for key in keys:
        value = os.getenv(key)
        if value is not None and value != "":
            return value
    return default


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Import missing gids from PostgreSQL into MySQL "
            "(gallery + tags/gid_tid + torrent)."
        )
    )

    parser.add_argument("--pg-host", default=env_first(["PGHOST"], "127.0.0.1"))
    parser.add_argument("--pg-port", type=int, default=int(env_first(["PGPORT"], "5432")))
    parser.add_argument("--pg-db", default=env_first(["PGDATABASE"], "ehentai_db"))
    parser.add_argument("--pg-user", default=env_first(["PGUSER"], "postgres"))
    parser.add_argument("--pg-pass", default=env_first(["PGPASSWORD"], ""))
    parser.add_argument("--pg-schema", default="public")

    parser.add_argument(
        "--mysql-host",
        default=env_first(["MYSQL_HOST", "DB_HOST"], "127.0.0.1"),
    )
    parser.add_argument(
        "--mysql-port",
        type=int,
        default=int(env_first(["MYSQL_PORT", "DB_PORT"], "3306")),
    )
    parser.add_argument(
        "--mysql-db",
        default=env_first(["MYSQL_DB", "DB_NAME"], "e-hentai-db"),
    )
    parser.add_argument(
        "--mysql-user",
        default=env_first(["MYSQL_USER", "DB_USER"], "root"),
    )
    parser.add_argument(
        "--mysql-pass",
        default=env_first(["MYSQL_PASSWORD", "DB_PASS"], ""),
    )

    parser.add_argument(
        "--scan-batch-size",
        type=int,
        default=5000,
        help="How many PG gids to compare against MySQL in one round.",
    )
    parser.add_argument(
        "--write-batch-size",
        type=int,
        default=1000,
        help="How many rows to insert per executemany batch.",
    )
    parser.add_argument(
        "--mysql-in-batch-size",
        type=int,
        default=2000,
        help="Max gids per MySQL IN (...) query.",
    )
    parser.add_argument(
        "--start-gid",
        type=int,
        default=None,
        help="Optional lower bound gid (inclusive).",
    )
    parser.add_argument(
        "--end-gid",
        type=int,
        default=None,
        help="Optional upper bound gid (inclusive).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report missing gids count; do not write to MySQL.",
    )

    return parser.parse_args()


def chunked(items: Sequence[int] | Sequence[Tuple] | Sequence[str], size: int) -> Iterator[Sequence]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def to_unix_seconds(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    else:
        raise ValueError(f"Unsupported posted type: {type(value)!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def normalize_bool(value: object) -> int:
    return 1 if bool(value) else 0


def normalize_rating(value: object, limit: Optional[int]) -> str:
    if value is None:
        result = "0"
    elif isinstance(value, Decimal):
        result = format(value.normalize(), "f")
    else:
        try:
            result = format(Decimal(str(value)).normalize(), "f")
        except (InvalidOperation, ValueError):
            result = str(value)

    if "." in result:
        result = result.rstrip("0").rstrip(".")
    if result == "":
        result = "0"
    if limit is not None and len(result) > limit:
        return result[:limit]
    return result


def truncate(value: Optional[str], limit: Optional[int]) -> Optional[str]:
    if value is None or limit is None:
        return value
    if len(value) <= limit:
        return value
    return value[:limit]


def normalize_tags(raw_tags: object, tag_len_limit: Optional[int]) -> List[str]:
    if raw_tags is None:
        return []

    tags = raw_tags
    if isinstance(tags, str):
        text = tags.strip()
        if text == "":
            return []
        try:
            tags = json.loads(text)
        except json.JSONDecodeError:
            # Fallback for unexpected format.
            tags = [text]

    if not isinstance(tags, list):
        return []

    result: List[str] = []
    seen = set()
    for item in tags:
        tag = str(item).strip()
        if not tag:
            continue
        tag = truncate(tag, tag_len_limit) or ""
        if not tag or tag in seen:
            continue
        seen.add(tag)
        result.append(tag)
    return result


def load_mysql_column_limits(mysql_conn, mysql_db: str) -> Dict[Tuple[str, str], Optional[int]]:
    targets = [
        ("gallery", "token"),
        ("gallery", "archiver_key"),
        ("gallery", "title"),
        ("gallery", "title_jpn"),
        ("gallery", "category"),
        ("gallery", "thumb"),
        ("gallery", "uploader"),
        ("gallery", "rating"),
        ("tag", "name"),
        ("torrent", "name"),
        ("torrent", "hash"),
        ("torrent", "addedstr"),
        ("torrent", "fsizestr"),
        ("torrent", "uploader"),
    ]

    with mysql_conn.cursor() as cur:
        cur.execute(
            """
            SELECT TABLE_NAME, COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND (
                  (TABLE_NAME = 'gallery' AND COLUMN_NAME IN ('token','archiver_key','title','title_jpn','category','thumb','uploader','rating'))
               OR (TABLE_NAME = 'tag' AND COLUMN_NAME = 'name')
               OR (TABLE_NAME = 'torrent' AND COLUMN_NAME IN ('name','hash','addedstr','fsizestr','uploader'))
              )
            """,
            (mysql_db,),
        )
        rows = cur.fetchall()

    limit_map: Dict[Tuple[str, str], Optional[int]] = {(t, c): None for t, c in targets}
    for table_name, col_name, char_len in rows:
        limit_map[(table_name, col_name)] = int(char_len) if char_len is not None else None
    return limit_map


def build_in_sql(base_sql: str, count: int) -> str:
    placeholders = ",".join(["%s"] * count)
    return f"{base_sql} ({placeholders})"


def fetch_mysql_existing_gids(mysql_conn, gids: Sequence[int], in_batch_size: int) -> set[int]:
    existing: set[int] = set()
    if not gids:
        return existing

    with mysql_conn.cursor() as cur:
        for part in chunked(gids, in_batch_size):
            sql = build_in_sql("SELECT gid FROM gallery WHERE gid IN", len(part))
            cur.execute(sql, tuple(part))
            for (gid,) in cur.fetchall():
                existing.add(int(gid))
    return existing


def fetch_pg_gallery_rows(pg_conn, schema: str, gids: Sequence[int]) -> List[dict]:
    if not gids:
        return []

    with pg_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                gid, token, archiver_key, title, title_jpn, category, thumb, uploader,
                posted, filecount, filesize, expunged, removed, replaced,
                rating, torrentcount, root_gid, bytorrent, tags
            FROM {schema}.gallery
            WHERE gid = ANY(%s)
            ORDER BY gid ASC
            """,
            (list(gids),),
        )
        return cur.fetchall()


def fetch_pg_torrents(pg_conn, schema: str, gids: Sequence[int]) -> List[dict]:
    if not gids:
        return []
    with pg_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, gid, name, hash, addedstr, fsizestr, uploader, expunged
            FROM {schema}.torrent
            WHERE gid = ANY(%s)
            ORDER BY gid ASC, id ASC
            """,
            (list(gids),),
        )
        return cur.fetchall()


def insert_gallery_rows(mysql_conn, rows: Sequence[dict], limits: Dict[Tuple[str, str], Optional[int]], write_batch_size: int) -> int:
    if not rows:
        return 0

    values: List[Tuple] = []
    for row in rows:
        values.append(
            (
                int(row["gid"]),
                truncate(row["token"], limits[("gallery", "token")]),
                truncate(row["archiver_key"] or "", limits[("gallery", "archiver_key")]),
                truncate(row["title"] or "", limits[("gallery", "title")]),
                truncate(row["title_jpn"] or "", limits[("gallery", "title_jpn")]),
                truncate(row["category"] or "", limits[("gallery", "category")]),
                truncate(row["thumb"] or "", limits[("gallery", "thumb")]),
                truncate(row["uploader"], limits[("gallery", "uploader")]),
                to_unix_seconds(row["posted"]),
                int(row["filecount"]),
                int(row["filesize"]),
                normalize_bool(row["expunged"]),
                normalize_bool(row["removed"]),
                normalize_bool(row["replaced"]),
                normalize_rating(row["rating"], limits[("gallery", "rating")]),
                int(row["torrentcount"]),
                int(row["root_gid"]) if row["root_gid"] is not None else None,
                normalize_bool(row["bytorrent"]),
            )
        )

    sql = """
        INSERT IGNORE INTO gallery (
            gid, token, archiver_key, title, title_jpn, category, thumb, uploader,
            posted, filecount, filesize, expunged, removed, replaced,
            rating, torrentcount, root_gid, bytorrent
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    inserted = 0
    with mysql_conn.cursor() as cur:
        for part in chunked(values, write_batch_size):
            cur.executemany(sql, part)
            inserted += cur.rowcount
    return inserted


def sync_tags(mysql_conn, rows: Sequence[dict], limits: Dict[Tuple[str, str], Optional[int]], write_batch_size: int, in_batch_size: int) -> Tuple[int, int]:
    if not rows:
        return 0, 0

    tag_len_limit = limits[("tag", "name")]
    gid_to_tags: Dict[int, List[str]] = {}
    all_tags: List[str] = []
    all_seen = set()
    gids: List[int] = []

    for row in rows:
        gid = int(row["gid"])
        gids.append(gid)
        tags = normalize_tags(row.get("tags"), tag_len_limit)
        gid_to_tags[gid] = tags
        for tag in tags:
            if tag in all_seen:
                continue
            all_seen.add(tag)
            all_tags.append(tag)

    with mysql_conn.cursor() as cur:
        for part in chunked(gids, in_batch_size):
            sql = build_in_sql("DELETE FROM gid_tid WHERE gid IN", len(part))
            cur.execute(sql, tuple(part))

        tag_map: Dict[str, int] = {}
        for part in chunked(all_tags, in_batch_size):
            sql = build_in_sql("SELECT id, name FROM tag WHERE name IN", len(part))
            cur.execute(sql, tuple(part))
            for tid, name in cur.fetchall():
                if name not in tag_map:
                    tag_map[name] = int(tid)

        missing_tags = [tag for tag in all_tags if tag not in tag_map]
        new_tags_count = 0
        if missing_tags:
            cur.executemany("INSERT INTO tag (name) VALUES (%s)", [(tag,) for tag in missing_tags])
            new_tags_count = cur.rowcount
            for part in chunked(missing_tags, in_batch_size):
                sql = build_in_sql("SELECT id, name FROM tag WHERE name IN", len(part))
                cur.execute(sql, tuple(part))
                for tid, name in cur.fetchall():
                    if name not in tag_map:
                        tag_map[name] = int(tid)

        gid_tid_pairs: List[Tuple[int, int]] = []
        pair_seen = set()
        for gid, tags in gid_to_tags.items():
            for tag in tags:
                tid = tag_map.get(tag)
                if tid is None:
                    continue
                pair = (gid, tid)
                if pair in pair_seen:
                    continue
                pair_seen.add(pair)
                gid_tid_pairs.append(pair)

        inserted_gid_tid = 0
        if gid_tid_pairs:
            for part in chunked(gid_tid_pairs, write_batch_size):
                cur.executemany("INSERT INTO gid_tid (gid, tid) VALUES (%s, %s)", part)
                inserted_gid_tid += cur.rowcount

    return new_tags_count, inserted_gid_tid


def sync_torrents(
    mysql_conn,
    pg_torrents: Sequence[dict],
    missing_gids: Sequence[int],
    limits: Dict[Tuple[str, str], Optional[int]],
    write_batch_size: int,
    in_batch_size: int,
) -> int:
    if not missing_gids:
        return 0

    values: List[Tuple] = []
    for row in pg_torrents:
        values.append(
            (
                int(row["id"]),
                int(row["gid"]),
                truncate(row["name"] or "", limits[("torrent", "name")]),
                truncate(row["hash"], limits[("torrent", "hash")]),
                truncate(row["addedstr"], limits[("torrent", "addedstr")]),
                truncate(row["fsizestr"], limits[("torrent", "fsizestr")]),
                truncate(row["uploader"] or "", limits[("torrent", "uploader")]) or "",
                normalize_bool(row["expunged"]),
            )
        )

    sql = """
        INSERT INTO torrent (
            id, gid, name, hash, addedstr, fsizestr, uploader, expunged
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    inserted = 0
    with mysql_conn.cursor() as cur:
        for part in chunked(missing_gids, in_batch_size):
            del_sql = build_in_sql("DELETE FROM torrent WHERE gid IN", len(part))
            cur.execute(del_sql, tuple(part))

        if values:
            for part in chunked(values, write_batch_size):
                cur.executemany(sql, part)
                inserted += cur.rowcount

    return inserted


def get_pg_gid_count(pg_conn, schema: str, start_gid: Optional[int], end_gid: Optional[int]) -> int:
    where, params = build_gid_range_where(start_gid, end_gid)
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS total FROM {schema}.gallery {where}", params)
        row = cur.fetchone()
    return int(row["total"])


def build_gid_range_where(start_gid: Optional[int], end_gid: Optional[int]) -> Tuple[str, Tuple]:
    clauses: List[str] = []
    params: List[int] = []
    if start_gid is not None:
        clauses.append("gid >= %s")
        params.append(start_gid)
    if end_gid is not None:
        clauses.append("gid <= %s")
        params.append(end_gid)
    if not clauses:
        return "", tuple()
    return "WHERE " + " AND ".join(clauses), tuple(params)


def run() -> int:
    args = parse_args()

    if args.scan_batch_size <= 0 or args.write_batch_size <= 0 or args.mysql_in_batch_size <= 0:
        print("Batch sizes must be positive integers.", file=sys.stderr)
        return 1
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", args.pg_schema):
        print(f"Invalid pg schema name: {args.pg_schema!r}", file=sys.stderr)
        return 1

    try:
        import pymysql
        import psycopg
        from psycopg.rows import dict_row
    except ImportError:
        print(
            "Missing dependencies. In conda base, install:\n"
            "  conda run -n base pip install 'psycopg[binary]' PyMySQL",
            file=sys.stderr,
        )
        return 1

    pg_where, pg_params = build_gid_range_where(args.start_gid, args.end_gid)

    pg_conn = None
    mysql_conn = None
    try:
        pg_conn = psycopg.connect(
            host=args.pg_host,
            port=args.pg_port,
            dbname=args.pg_db,
            user=args.pg_user,
            password=args.pg_pass,
            row_factory=dict_row,
        )
        mysql_conn = pymysql.connect(
            host=args.mysql_host,
            port=args.mysql_port,
            user=args.mysql_user,
            password=args.mysql_pass,
            database=args.mysql_db,
            charset="utf8mb4",
            autocommit=False,
        )

        limits = load_mysql_column_limits(mysql_conn, args.mysql_db)
        total_pg_rows = get_pg_gid_count(pg_conn, args.pg_schema, args.start_gid, args.end_gid)
        print(f"[info] pg gallery rows in scope: {total_pg_rows}")
        print("[info] scanning pg gids and comparing with mysql ...")

        scanned = 0
        missing_total = 0
        inserted_gallery_total = 0
        inserted_gid_tid_total = 0
        inserted_torrent_total = 0
        inserted_tag_total = 0
        batch_no = 0

        with pg_conn.cursor(name="scan_missing_gids") as scan_cur:
            scan_cur.itersize = args.scan_batch_size
            scan_cur.execute(
                f"SELECT gid FROM {args.pg_schema}.gallery {pg_where} ORDER BY gid ASC",
                pg_params,
            )

            while True:
                rows = scan_cur.fetchmany(args.scan_batch_size)
                if not rows:
                    break
                batch_no += 1
                gid_batch = [int(row["gid"]) for row in rows]
                scanned += len(gid_batch)

                existing = fetch_mysql_existing_gids(mysql_conn, gid_batch, args.mysql_in_batch_size)
                missing = [gid for gid in gid_batch if gid not in existing]
                if not missing:
                    print(
                        f"[batch {batch_no}] scanned={scanned}/{total_pg_rows}, "
                        f"missing_in_batch=0"
                    )
                    continue

                missing_total += len(missing)
                print(
                    f"[batch {batch_no}] scanned={scanned}/{total_pg_rows}, "
                    f"missing_in_batch={len(missing)}, missing_total={missing_total}"
                )

                if args.dry_run:
                    continue

                try:
                    pg_rows = fetch_pg_gallery_rows(pg_conn, args.pg_schema, missing)
                    inserted_gallery = insert_gallery_rows(
                        mysql_conn,
                        pg_rows,
                        limits,
                        args.write_batch_size,
                    )
                    new_tags, inserted_gid_tid = sync_tags(
                        mysql_conn,
                        pg_rows,
                        limits,
                        args.write_batch_size,
                        args.mysql_in_batch_size,
                    )
                    pg_torrents = fetch_pg_torrents(pg_conn, args.pg_schema, missing)
                    inserted_torrent = sync_torrents(
                        mysql_conn,
                        pg_torrents,
                        missing,
                        limits,
                        args.write_batch_size,
                        args.mysql_in_batch_size,
                    )
                    mysql_conn.commit()
                except Exception:
                    mysql_conn.rollback()
                    raise

                inserted_gallery_total += inserted_gallery
                inserted_gid_tid_total += inserted_gid_tid
                inserted_torrent_total += inserted_torrent
                inserted_tag_total += new_tags

                print(
                    f"[batch {batch_no}] inserted gallery={inserted_gallery}, "
                    f"gid_tid={inserted_gid_tid}, torrent={inserted_torrent}, "
                    f"new_tag={new_tags}"
                )

        print("")
        print("[done] sync complete")
        print(f"  scanned_pg_gids: {scanned}")
        print(f"  missing_gids: {missing_total}")
        if args.dry_run:
            print("  dry_run: true (no data written)")
        else:
            print(f"  inserted_gallery: {inserted_gallery_total}")
            print(f"  inserted_gid_tid: {inserted_gid_tid_total}")
            print(f"  inserted_torrent: {inserted_torrent_total}")
            print(f"  inserted_tag: {inserted_tag_total}")
        return 0
    finally:
        if pg_conn is not None:
            pg_conn.close()
        if mysql_conn is not None:
            mysql_conn.close()


if __name__ == "__main__":
    sys.exit(run())

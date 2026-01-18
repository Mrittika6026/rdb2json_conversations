#!/usr/bin/env python3
"""
Export a Redis RDB file to JSON by temporarily starting a local redis-server
pointed at the RDB, then reading keys via the Redis protocol.

Why this approach:
- Redis RDB format versions change over time; parsing RDB directly requires a
  parser that supports your dump's format version. Loading via redis-server is
  the most compatible option when you have a sufficiently new Redis.

Usage:
  python3 rdb_to_json.py --rdb ./dump.rdb --out ./dump.json

Requirements:
  pip install redis

Notes:
- If your local redis-server is too old to load the RDB (e.g. "Can't handle RDB
  format version 12"), install a newer Redis and re-run.
"""

from __future__ import annotations

import argparse
import base64
import datetime as _dt
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple

try:
    import redis  # type: ignore
except Exception as e:  # pragma: no cover
    print(
        "Missing dependency: redis (redis-py). Install with:\n"
        "  python3 -m pip install --user redis\n",
        file=sys.stderr,
    )
    raise


def _utc_now_iso() -> str:
    return _dt.datetime.now(tz=_dt.timezone.utc).isoformat()


def _pick_free_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _encode_bytes(data: Optional[bytes]) -> Optional[Dict[str, Any]]:
    if data is None:
        return None
    try:
        s = data.decode("utf-8")
        return {"encoding": "utf-8", "data": s}
    except UnicodeDecodeError:
        return {"encoding": "base64", "data": _b64(data)}


def _encode_key(key: bytes) -> Dict[str, Any]:
    # JSON object keys must be strings; we store keys as objects to be binary-safe.
    return _encode_bytes(key) or {"encoding": "base64", "data": ""}


def _encode_scalar(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bytes):
        return _encode_bytes(v)
    if isinstance(v, (str, int, float, bool)):
        return v
    # Fallback: attempt to stringify safely.
    return str(v)


def _redis_server_version(redis_server: str) -> Optional[str]:
    try:
        p = subprocess.run(
            [redis_server, "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        return (p.stdout or "").strip() or None
    except FileNotFoundError:
        return None


def _start_redis_for_rdb(
    *,
    redis_server: str,
    rdb_path: str,
    port: int,
    work_dir: str,
) -> subprocess.Popen:
    # Redis expects the dump file name to match --dbfilename in --dir.
    dbfilename = os.path.basename(rdb_path)
    target_rdb = os.path.join(work_dir, dbfilename)
    shutil.copy2(rdb_path, target_rdb)

    cmd = [
        redis_server,
        "--dir",
        work_dir,
        "--dbfilename",
        dbfilename,
        "--appendonly",
        "no",
        "--save",
        "",  # disable snapshots while we're exporting
        "--bind",
        "127.0.0.1",
        "--protected-mode",
        "no",
        "--port",
        str(port),
        "--loglevel",
        "warning",
    ]

    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait_for_redis_ready(r: "redis.Redis", timeout_s: float = 20.0) -> None:
    start = time.time()
    last_err: Optional[Exception] = None
    while time.time() - start < timeout_s:
        try:
            if r.ping():
                return
        except Exception as e:
            last_err = e
        time.sleep(0.2)
    raise RuntimeError(f"Redis did not become ready in {timeout_s}s. Last error: {last_err}")


def _collect_db_indexes(r: "redis.Redis") -> List[int]:
    # Prefer INFO keyspace which includes only DBs with keys.
    try:
        info = r.info("keyspace")
        dbs: List[int] = []
        for k in info.keys():
            if isinstance(k, str) and k.startswith("db"):
                try:
                    dbs.append(int(k[2:]))
                except ValueError:
                    pass
        return sorted(set(dbs)) or [0]
    except Exception:
        return [0]


def _scan_keys(r: "redis.Redis", count: int = 1000) -> List[bytes]:
    cursor = 0
    keys: List[bytes] = []
    while True:
        cursor, batch = r.scan(cursor=cursor, count=count)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys


def _read_key_entry(r: "redis.Redis", key: bytes) -> Dict[str, Any]:
    key_type = r.type(key)
    if isinstance(key_type, bytes):
        key_type_str = key_type.decode("utf-8", errors="replace")
    else:
        key_type_str = str(key_type)

    ttl_ms: Optional[int]
    try:
        pttl = int(r.pttl(key))
        ttl_ms = None if pttl < 0 else pttl
    except Exception:
        ttl_ms = None

    entry: Dict[str, Any] = {
        "key": _encode_key(key),
        "type": key_type_str,
        "ttl_ms": ttl_ms,
    }

    try:
        if key_type_str == "string":
            entry["value"] = _encode_bytes(r.get(key))
        elif key_type_str == "hash":
            m = r.hgetall(key)
            # Preserve binary safety by storing items as an array.
            entry["value"] = [
                {"field": _encode_bytes(f), "value": _encode_bytes(v)} for f, v in m.items()
            ]
        elif key_type_str == "list":
            vals = r.lrange(key, 0, -1)
            entry["value"] = [_encode_bytes(v) for v in vals]
        elif key_type_str == "set":
            vals = list(r.smembers(key))
            # Sort deterministically by raw bytes.
            vals.sort()
            entry["value"] = [_encode_bytes(v) for v in vals]
        elif key_type_str == "zset":
            vals = r.zrange(key, 0, -1, withscores=True)
            entry["value"] = [{"member": _encode_bytes(m), "score": s} for m, s in vals]
        elif key_type_str == "stream":
            # XRANGE can be expensive; still better than dumping raw encoding for most cases.
            items = r.xrange(key, min="-", max="+")
            entry["value"] = [
                {
                    "id": _encode_scalar(item_id),
                    "fields": [{"field": _encode_bytes(f), "value": _encode_bytes(v)} for f, v in fields],
                }
                for item_id, fields in items
            ]
        else:
            # Unknown/module types: store raw DUMP payload so data isn't lost.
            payload = r.dump(key)
            entry["value"] = {
                "encoding": "redis-dump-base64",
                "data": _b64(payload) if payload is not None else None,
            }
    except Exception as e:
        entry["error"] = str(e)

    return entry


def export_rdb_to_json(*, rdb_path: str, out_path: str, redis_server: str) -> None:
    if not os.path.exists(rdb_path):
        raise FileNotFoundError(rdb_path)

    port = _pick_free_port()
    redis_ver = _redis_server_version(redis_server)
    if redis_ver is None:
        raise RuntimeError(
            f"Could not run redis-server at '{redis_server}'. "
            "Install Redis or pass --redis-server /path/to/redis-server"
        )

    with tempfile.TemporaryDirectory(prefix="rdb_to_json_") as tmp:
        proc = _start_redis_for_rdb(redis_server=redis_server, rdb_path=rdb_path, port=port, work_dir=tmp)
        try:
            r = redis.Redis(host="127.0.0.1", port=port, db=0, decode_responses=False)
            try:
                _wait_for_redis_ready(r, timeout_s=20.0)
            except Exception:
                # Try to surface redis-server stderr for the common "RDB version" mismatch case.
                try:
                    proc.poll()
                    err = (proc.stderr.read() if proc.stderr else "")  # type: ignore[union-attr]
                    out = (proc.stdout.read() if proc.stdout else "")  # type: ignore[union-attr]
                except Exception:
                    err, out = "", ""
                raise RuntimeError(
                    "Failed to start redis-server or load the RDB.\n\n"
                    f"redis-server --version:\n{redis_ver}\n\n"
                    "redis-server output:\n"
                    f"{out}{err}\n\n"
                    "If you see something like \"Can't handle RDB format version X\", "
                    "you need a newer redis-server to load this dump."
                )

            meta = {
                "source_rdb": os.path.abspath(rdb_path),
                "exported_at_utc": _utc_now_iso(),
                "redis_server_version": redis_ver,
            }

            result: Dict[str, Any] = {"meta": meta, "db": {}}
            dbs = _collect_db_indexes(r)
            for dbi in dbs:
                r.execute_command("SELECT", dbi)
                entries = []
                for key in _scan_keys(r):
                    entries.append(_read_key_entry(r, key))
                result["db"][str(dbi)] = {"entries": entries}

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
        finally:
            # Ensure the temporary redis-server is stopped.
            try:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Export Redis RDB file to JSON.")
    p.add_argument("--rdb", default="dump.rdb", help="Path to RDB file (default: dump.rdb)")
    p.add_argument("--out", default="dump.json", help="Output JSON file (default: dump.json)")
    p.add_argument(
        "--redis-server",
        default="redis-server",
        help="Path to redis-server binary (default: redis-server)",
    )
    args = p.parse_args(argv)

    export_rdb_to_json(rdb_path=args.rdb, out_path=args.out, redis_server=args.redis_server)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



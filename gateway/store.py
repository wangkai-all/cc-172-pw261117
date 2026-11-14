"""SQLite 持久化：样本、坏包/噪声、配置版本、通道状态、事件、元数据。

- 样本以 (channel, epoch, seq) 唯一，重复采样 INSERT OR IGNORE 不重复入库；
- 每条样本记录原始字节(hex)、解析值、套用的配置版本号与换算结果；
- channel_state 保存各通道最近 (epoch, seq)，进程重启后续传去重与回绕判定；
- meta 保存全局字节流偏移等标量，重启后错误位置仍然单调。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts            REAL NOT NULL,
    channel       INTEGER NOT NULL,
    epoch         INTEGER NOT NULL,
    seq           INTEGER NOT NULL,
    raw_hex       TEXT NOT NULL,
    raw_value     INTEGER NOT NULL,
    scale         REAL NOT NULL,
    unit          TEXT NOT NULL,
    scaled        REAL NOT NULL,
    config_version INTEGER NOT NULL,
    stream_offset INTEGER NOT NULL,
    flags         TEXT NOT NULL DEFAULT '',
    UNIQUE (channel, epoch, seq)
);
CREATE INDEX IF NOT EXISTS idx_samples_id ON samples(id);

CREATE TABLE IF NOT EXISTS errors (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts            REAL NOT NULL,
    kind          TEXT NOT NULL,       -- noise / bad_eof / bad_checksum / truncated
    stream_offset INTEGER NOT NULL,
    length        INTEGER NOT NULL,
    raw_hex       TEXT NOT NULL,
    detail        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS config_versions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              REAL NOT NULL,
    channel         INTEGER NOT NULL,
    scale           REAL NOT NULL,
    unit            TEXT NOT NULL,
    effective_epoch INTEGER NOT NULL,
    effective_seq   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_config_ch ON config_versions(channel, effective_epoch, effective_seq);

CREATE TABLE IF NOT EXISTS channel_state (
    channel   INTEGER PRIMARY KEY,
    epoch     INTEGER NOT NULL,
    last_seq  INTEGER NOT NULL,
    updated   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      REAL NOT NULL,
    kind    TEXT NOT NULL,   -- wrap / restart / duplicate / connect / disconnect / pause / resume / config
    channel INTEGER,
    detail  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

DEFAULT_CHANNELS = (0, 1)


class Store:
    def __init__(self, path: str, channels=DEFAULT_CHANNELS):
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        with self._lock, self._conn:
            self._conn.executescript(SCHEMA)
            for ch in channels:
                # 每通道兜底配置：倍率 1.0，单位 counts，自 (0,0) 起生效
                cur = self._conn.execute(
                    "SELECT COUNT(*) c FROM config_versions WHERE channel=?", (ch,))
                if cur.fetchone()["c"] == 0:
                    self._conn.execute(
                        "INSERT INTO config_versions(ts,channel,scale,unit,effective_epoch,effective_seq)"
                        " VALUES(?,?,?,?,?,?)",
                        (time.time(), ch, 1.0, "counts", 0, 0))

    # ---------- meta ----------
    def get_meta(self, key: str, default=None):
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row["value"]) if row else default

    def set_meta(self, key: str, value) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, json.dumps(value)))

    # ---------- 样本 ----------
    def insert_sample(self, *, channel, epoch, seq, raw: bytes, raw_value,
                      scale, unit, scaled, config_version, stream_offset, flags="") -> bool:
        """返回 True=新入库，False=重复采样被忽略。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO samples"
                "(ts,channel,epoch,seq,raw_hex,raw_value,scale,unit,scaled,"
                " config_version,stream_offset,flags)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (time.time(), channel, epoch, seq, raw.hex(), raw_value,
                 scale, unit, scaled, config_version, stream_offset, flags))
            return cur.rowcount == 1

    def recent_samples(self, limit=200, channel=None) -> list[dict]:
        sql = "SELECT * FROM samples"
        args: list = []
        if channel is not None:
            sql += " WHERE channel=?"
            args.append(channel)
        sql += " ORDER BY id DESC LIMIT ?"
        args.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [dict(r) for r in rows]

    def iter_samples(self):
        with self._lock:
            rows = self._conn.execute("SELECT * FROM samples ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    def count_samples(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) c FROM samples").fetchone()["c"]

    def sample_exists(self, channel: int, epoch: int, seq: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM samples WHERE channel=? AND epoch=? AND seq=?",
                (channel, epoch, seq)).fetchone()
        return row is not None

    # ---------- 错误（坏包/噪声/截断） ----------
    def insert_error(self, *, kind, stream_offset, raw: bytes, detail="") -> dict:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO errors(ts,kind,stream_offset,length,raw_hex,detail)"
                " VALUES(?,?,?,?,?,?)",
                (time.time(), kind, stream_offset, len(raw), raw.hex(), detail))
            row = self._conn.execute(
                "SELECT * FROM errors WHERE id=?", (cur.lastrowid,)).fetchone()
        return dict(row)

    def recent_errors(self, limit=100) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM errors ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def iter_errors(self):
        with self._lock:
            rows = self._conn.execute("SELECT * FROM errors ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    # ---------- 配置版本 ----------
    def add_config(self, *, channel, scale, unit, effective_epoch, effective_seq) -> dict:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO config_versions"
                "(ts,channel,scale,unit,effective_epoch,effective_seq)"
                " VALUES(?,?,?,?,?,?)",
                (time.time(), channel, scale, unit, effective_epoch, effective_seq))
            row = self._conn.execute(
                "SELECT * FROM config_versions WHERE id=?", (cur.lastrowid,)).fetchone()
        return dict(row)

    def resolve_config(self, channel: int, epoch: int, seq: int) -> dict:
        """按 (epoch, seq) 字典序取 生效点 <= 样本点 的最新配置。

        只依赖样本自身序号，与处理时刻无关——同一段缓冲数据无论
        何时被处理，解析出的配置版本都一致。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM config_versions WHERE channel=?"
                " AND (effective_epoch < ? OR (effective_epoch = ? AND effective_seq <= ?))"
                " ORDER BY effective_epoch DESC, effective_seq DESC, id DESC LIMIT 1",
                (channel, epoch, epoch, seq)).fetchone()
        if row is None:  # 通道首次出现且没有兜底配置
            return {"id": 0, "channel": channel, "scale": 1.0, "unit": "counts",
                    "effective_epoch": 0, "effective_seq": 0}
        return dict(row)

    def list_configs(self, channel=None) -> list[dict]:
        sql = "SELECT * FROM config_versions"
        args: list = []
        if channel is not None:
            sql += " WHERE channel=?"
            args.append(channel)
        sql += " ORDER BY id"
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [dict(r) for r in rows]

    # ---------- 通道状态 ----------
    def get_channel_state(self, channel: int):
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM channel_state WHERE channel=?", (channel,)).fetchone()
        return dict(row) if row else None

    def all_channel_states(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM channel_state ORDER BY channel").fetchall()
        return [dict(r) for r in rows]

    def set_channel_state(self, channel: int, epoch: int, last_seq: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO channel_state(channel,epoch,last_seq,updated) VALUES(?,?,?,?)"
                " ON CONFLICT(channel) DO UPDATE SET epoch=excluded.epoch,"
                " last_seq=excluded.last_seq, updated=excluded.updated",
                (channel, epoch, last_seq, time.time()))

    # ---------- 事件 ----------
    def insert_event(self, kind: str, channel, detail: str) -> dict:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO events(ts,kind,channel,detail) VALUES(?,?,?,?)",
                (time.time(), kind, channel, detail))
            row = self._conn.execute(
                "SELECT * FROM events WHERE id=?", (cur.lastrowid,)).fetchone()
        return dict(row)

    def recent_events(self, limit=100) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def iter_events(self):
        with self._lock:
            rows = self._conn.execute("SELECT * FROM events ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    # ---------- 统计 ----------
    def stats(self) -> dict:
        with self._lock:
            ok = self._conn.execute("SELECT COUNT(*) c FROM samples").fetchone()["c"]
            errs = self._conn.execute(
                "SELECT kind, COUNT(*) c, COALESCE(SUM(length),0) b FROM errors GROUP BY kind"
            ).fetchall()
            dup = self._conn.execute(
                "SELECT COUNT(*) c FROM events WHERE kind='duplicate'").fetchone()["c"]
        by_kind = {r["kind"]: {"count": r["c"], "bytes": r["b"]} for r in errs}
        return {
            "samples": ok,
            "duplicates": dup,
            "bad_packets": sum(v["count"] for k, v in by_kind.items() if k != "noise"),
            "noise_segments": by_kind.get("noise", {}).get("count", 0),
            "noise_bytes": by_kind.get("noise", {}).get("bytes", 0),
            "errors_by_kind": by_kind,
        }

    def close(self):
        with self._lock:
            self._conn.close()

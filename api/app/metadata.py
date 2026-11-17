"""API 元数据库（SQLite，WAL）。

只存：原片元数据、密文 blob 路径、信封 DEK（密文）、主密钥 id、状态。
绝不存：明文 DEK、明文原片、主密钥材料。
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
import uuid
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS images (
    image_id        TEXT PRIMARY KEY,
    filename        TEXT,
    content_type    TEXT,
    status          TEXT NOT NULL CHECK (status IN
                        ('receiving','ready','failed')),
    master_key_id   TEXT NOT NULL,
    wrapped_dek     TEXT NOT NULL,           -- 十六进制信封，始终为密文
    blob_path       TEXT NOT NULL,
    plain_size      INTEGER,
    enc_size        INTEGER,
    sha256          TEXT,
    idem_token      TEXT UNIQUE,
    error           TEXT,
    created_at      REAL NOT NULL,
    finalized_at    REAL
);
CREATE INDEX IF NOT EXISTS idx_images_mk ON images(master_key_id);
CREATE INDEX IF NOT EXISTS idx_images_status ON images(status);

CREATE TABLE IF NOT EXISTS rotation_jobs (
    job_id          TEXT PRIMARY KEY,
    state           TEXT NOT NULL CHECK (state IN
                        ('migrating','destroying','completed','failed')),
    old_key_id      TEXT NOT NULL,
    new_key_id      TEXT NOT NULL,
    total           INTEGER NOT NULL DEFAULT 0,
    migrated        INTEGER NOT NULL DEFAULT 0,
    failed          INTEGER NOT NULL DEFAULT 0,
    error           TEXT,
    started_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    finished_at     REAL
);

-- 增量交接流：原片发布为 ready 的同一事务内入流，seq 即实际发布顺序。
-- image_id 唯一：幂等重放、主密钥改包都不会产生第二条交接记录。
CREATE TABLE IF NOT EXISTS handoff_stream (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    image_id        TEXT NOT NULL UNIQUE,
    sha256          TEXT NOT NULL,
    plain_size      INTEGER NOT NULL,
    published_at    REAL NOT NULL
);

-- 单一固定消费者的已确认游标（单行）
CREATE TABLE IF NOT EXISTS handoff_cursor (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    confirmed_seq   INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO handoff_cursor (id, confirmed_seq) VALUES (1, 0);

-- 领取记录：一页连续 seq + 限时令牌；确认进度随令牌持久化
CREATE TABLE IF NOT EXISTS handoff_claims (
    token           TEXT PRIMARY KEY,
    start_seq       INTEGER NOT NULL,
    end_seq         INTEGER NOT NULL,
    issued_at       REAL NOT NULL,
    expires_at      REAL NOT NULL,
    superseded      INTEGER NOT NULL DEFAULT 0,
    confirmed_upto  INTEGER,
    confirmed_at    REAL
);
CREATE INDEX IF NOT EXISTS idx_handoff_claims_open
    ON handoff_claims(superseded, confirmed_upto);
"""

# 交接确认业务错误：携带 HTTP 状态码与错误码，由 API 层转成响应
class ClaimError(Exception):
    def __init__(self, status_code: int, code: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message

TERMINAL_STATES = ("completed", "failed")


class MetadataDB:
    """每线程独立连接（SQLite 连接不跨线程复用），写操作串行化。"""

    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._db_path = db_path
        self._write_lock = threading.RLock()
        self._local = threading.local()
        conn = self._connect()
        conn.executescript(SCHEMA)
        # 交接流回填：为本功能上线前已 ready 的原片补登交接记录，
        # 按原发布时间排序；INSERT OR IGNORE 保证重启幂等、只补一次。
        conn.execute(
            "INSERT OR IGNORE INTO handoff_stream "
            "(image_id, sha256, plain_size, published_at) "
            "SELECT image_id, sha256, plain_size, finalized_at FROM images "
            "WHERE status='ready' ORDER BY finalized_at, image_id")
        conn.commit()
        conn.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=30,
                               check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None:
            c = self._connect()
            self._local.conn = c
        return c

    # ---- 原片 -----------------------------------------------------------
    def create_image(self, image_id: str, filename: str | None,
                     content_type: str | None, master_key_id: str,
                     wrapped_dek: str, blob_path: str,
                     idem_token: str | None) -> str:
        with self._write_lock:
            if idem_token:
                existing = self.conn.execute(
                    "SELECT image_id FROM images WHERE idem_token=?",
                    (idem_token,)).fetchone()
                if existing:
                    return existing["image_id"]
            now = time.time()
            self.conn.execute(
                "INSERT INTO images (image_id, filename, content_type, status, "
                "master_key_id, wrapped_dek, blob_path, idem_token, created_at) "
                "VALUES (?,?,?, 'receiving', ?,?,?,?,?)",
                (image_id, filename, content_type, master_key_id,
                 wrapped_dek, blob_path, idem_token, now))
            self.conn.commit()
            return image_id

    def get_image(self, image_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM images WHERE image_id=?", (image_id,)).fetchone()
        return dict(row) if row else None

    def get_by_token(self, idem_token: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM images WHERE idem_token=?", (idem_token,)).fetchone()
        return dict(row) if row else None

    def finalize_image(self, image_id: str, plain_size: int, enc_size: int,
                       sha256: str) -> None:
        with self._write_lock:
            now = time.time()
            # 发布状态与交接记录同一事务落盘：要么一起生效，要么一起回滚。
            # seq 由本事务分配，故交接顺序严格等于实际发布顺序；
            # 慢上传开始得再早，也只在完成发布时才进入流尾，不会被游标漏掉。
            self.conn.execute(
                "UPDATE images SET status='ready', plain_size=?, enc_size=?, "
                "sha256=?, finalized_at=?, error=NULL WHERE image_id=?",
                (plain_size, enc_size, sha256, now, image_id))
            # INSERT OR IGNORE：同一 image_id 的重复 finalize（如幂等重放
            # 竞争窗口）不会产生第二条交接记录；主密钥改包不经过本函数。
            self.conn.execute(
                "INSERT OR IGNORE INTO handoff_stream "
                "(image_id, sha256, plain_size, published_at) VALUES (?,?,?,?)",
                (image_id, sha256, plain_size, now))
            self.conn.commit()

    def fail_image(self, image_id: str, error: str) -> None:
        with self._write_lock:
            # 失败行释放幂等令牌，允许客户端用同一令牌重新发起上传
            self.conn.execute(
                "UPDATE images SET status='failed', error=?, finalized_at=?, "
                "idem_token=NULL WHERE image_id=?",
                (error[:500], time.time(), image_id))
            self.conn.commit()

    def list_images(self, limit: int = 100, offset: int = 0) -> list[dict]:
        rows = self.conn.execute(
            "SELECT image_id, filename, status, master_key_id, plain_size, "
            "sha256, created_at, finalized_at FROM images "
            "ORDER BY created_at DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
        return [dict(r) for r in rows]

    def distinct_key_ids(self) -> list[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT master_key_id FROM images").fetchall()
        return [r["master_key_id"] for r in rows]

    def count_under_key(self, key_id: str,
                        only_migratable: bool = False) -> int:
        sql = "SELECT COUNT(*) AS n FROM images WHERE master_key_id=?"
        if only_migratable:
            # 仅统计会阻塞销毁的行：ready（必须 rewrap）与在途 receiving；
            # failed 行永不会被下载，无需有效信封，不阻塞销毁。
            sql += " AND status IN ('ready','receiving')"
        return self.conn.execute(sql, (key_id,)).fetchone()["n"]

    def iter_under_key(self, key_id: str) -> Iterator[dict]:
        rows = self.conn.execute(
            "SELECT * FROM images WHERE master_key_id=? "
            "AND status IN ('ready','receiving') ORDER BY created_at",
            (key_id,)).fetchall()
        for r in rows:
            yield dict(r)

    def rewrap_row(self, image_id: str, expect_old_key: str,
                   new_key_id: str, new_wrapped: str) -> bool:
        """条件更新：仅当该行仍绑定旧 MK 时改包，返回是否实际更新。

        这样迁移与"迟到的在途上传最终化"互不覆盖：后者若已自行改绑新 MK，
        迁移跳过它即可。
        """
        with self._write_lock:
            cur = self.conn.execute(
                "UPDATE images SET master_key_id=?, wrapped_dek=? "
                "WHERE image_id=? AND master_key_id=?",
                (new_key_id, new_wrapped, image_id, expect_old_key))
            self.conn.commit()
            return cur.rowcount > 0

    def mark_stale_receiving_failed(self) -> int:
        """进程重启后，所有 receiving 都是上次崩溃的在途上传，
        不允许晚发布（防止它依赖已销毁的旧 MK 才完成）。"""
        with self._write_lock:
            cur = self.conn.execute(
                "UPDATE images SET status='failed', "
                "error='interrupted by service restart', finalized_at=? "
                "WHERE status='receiving'", (time.time(),))
            self.conn.commit()
            return cur.rowcount

    # ---- 轮换作业 -------------------------------------------------------
    def active_rotation(self) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM rotation_jobs WHERE state NOT IN ('completed','failed')"
        ).fetchone()
        return dict(row) if row else None

    def create_rotation(self, old_key_id: str, new_key_id: str,
                        total: int) -> str:
        with self._write_lock:
            if self.active_rotation() is not None:
                raise RuntimeError("rotation already in progress")
            job_id = "rot-" + uuid.uuid4().hex
            now = time.time()
            self.conn.execute(
                "INSERT INTO rotation_jobs (job_id, state, old_key_id, "
                "new_key_id, total, migrated, failed, started_at, updated_at) "
                "VALUES (?, 'migrating', ?,?,?, 0, 0, ?, ?)",
                (job_id, old_key_id, new_key_id, total, now, now))
            self.conn.commit()
            return job_id

    def has_rotation_for(self, old_key_id: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM rotation_jobs WHERE old_key_id=? LIMIT 1",
            (old_key_id,)).fetchone() is not None

    def get_rotation(self, job_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM rotation_jobs WHERE job_id=?", (job_id,)).fetchone()
        return dict(row) if row else None

    def latest_rotation(self) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM rotation_jobs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    def bump_rotation(self, job_id: str, migrated: int = 0, failed: int = 0,
                      state: str | None = None, error: str | None = None) -> None:
        with self._write_lock:
            sets = ["updated_at=?"]
            params: list[Any] = [time.time()]
            if migrated:
                sets.append("migrated = migrated + ?")
                params.append(migrated)
            if failed:
                sets.append("failed = failed + ?")
                params.append(failed)
            if state:
                sets.append("state=?")
                params.append(state)
            if error is not None:
                sets.append("error=?")
                params.append(error[:500])
            if state in TERMINAL_STATES:
                sets.append("finished_at=?")
                params.append(time.time())
            params.append(job_id)
            self.conn.execute(
                f"UPDATE rotation_jobs SET {', '.join(sets)} WHERE job_id=?",
                params)
            self.conn.commit()

    # ---- 增量交接（单一固定消费者） --------------------------------------
    def handoff_status(self) -> dict:
        cur = self.conn.execute(
            "SELECT confirmed_seq FROM handoff_cursor WHERE id=1").fetchone()
        total = self.conn.execute(
            "SELECT COUNT(*) AS n FROM handoff_stream").fetchone()["n"]
        claim = self.conn.execute(
            "SELECT * FROM handoff_claims WHERE confirmed_upto IS NULL "
            "AND superseded=0 ORDER BY issued_at DESC LIMIT 1").fetchone()
        confirmed = cur["confirmed_seq"]
        return {
            "confirmed_seq": confirmed,
            "published_total": total,
            "pending": max(total - confirmed, 0),
            "outstanding_claim": (self._claim_view(dict(claim))
                                  if claim else None),
        }

    def create_claim(self, limit: int, lease_seconds: float) -> dict | None:
        """从第一个未确认的 seq 起领取连续一页，签发限时令牌。

        单一固定消费者：新领取会把此前未确认的旧令牌置为 superseded，
        旧令牌从此不能再确认（到期重领必须换令牌）；未确认的原片会在
        新一页里原样重新交接，不会被跳过。无可交接内容时返回 None。
        """
        with self._write_lock:
            confirmed = self.conn.execute(
                "SELECT confirmed_seq FROM handoff_cursor WHERE id=1"
            ).fetchone()["confirmed_seq"]
            rows = self.conn.execute(
                "SELECT seq, image_id, sha256, plain_size, published_at "
                "FROM handoff_stream WHERE seq > ? ORDER BY seq LIMIT ?",
                (confirmed, limit)).fetchall()
            if not rows:
                return None
            now = time.time()
            self.conn.execute(
                "UPDATE handoff_claims SET superseded=1 "
                "WHERE confirmed_upto IS NULL AND superseded=0")
            token = "hc-" + uuid.uuid4().hex
            expires = now + lease_seconds
            self.conn.execute(
                "INSERT INTO handoff_claims "
                "(token, start_seq, end_seq, issued_at, expires_at) "
                "VALUES (?,?,?,?,?)",
                (token, rows[0]["seq"], rows[-1]["seq"], now, expires))
            self.conn.commit()
            return {
                "claim_token": token,
                "start_seq": rows[0]["seq"],
                "end_seq": rows[-1]["seq"],
                "lease_expires_at": expires,
                "items": [{"seq": r["seq"], "image_id": r["image_id"],
                           "sha256": r["sha256"], "size": r["plain_size"],
                           "published_at": r["published_at"]} for r in rows],
            }

    def get_claim(self, token: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM handoff_claims WHERE token=?", (token,)).fetchone()
        return self._claim_view(dict(row)) if row else None

    def confirm_claim(self, token: str, upto_seq: int) -> dict:
        """确认一页内已处理到的 seq；随令牌持久化，游标同步推进。

        确认回执丢失时，同一令牌 + 同一 upto_seq 的重试命中已持久化的
        确认记录，幂等返回当前进度；即使令牌此后已过期或被取代也如此。
        """
        with self._write_lock:
            row = self.conn.execute(
                "SELECT * FROM handoff_claims WHERE token=?",
                (token,)).fetchone()
            if row is None:
                raise ClaimError(404, "claim_not_found",
                                 "unknown claim token")
            if row["confirmed_upto"] is not None:
                if row["confirmed_upto"] == upto_seq:
                    return self._confirm_view(dict(row), idempotent=True)
                raise ClaimError(
                    409, "already_confirmed",
                    f"claim already confirmed upto seq {row['confirmed_upto']}")
            if row["superseded"]:
                raise ClaimError(
                    409, "claim_superseded",
                    "claim superseded by a newer claim; re-claim to continue")
            if time.time() > row["expires_at"]:
                raise ClaimError(
                    409, "lease_expired",
                    "claim lease expired; re-claim to get a new token")
            if not (row["start_seq"] <= upto_seq <= row["end_seq"]):
                raise ClaimError(
                    409, "upto_seq_out_of_range",
                    f"upto_seq must be within claimed page "
                    f"[{row['start_seq']}, {row['end_seq']}]")
            now = time.time()
            self.conn.execute(
                "UPDATE handoff_claims SET confirmed_upto=?, confirmed_at=? "
                "WHERE token=?", (upto_seq, now, token))
            self.conn.execute(
                "UPDATE handoff_cursor SET confirmed_seq="
                "MAX(confirmed_seq, ?) WHERE id=1", (upto_seq,))
            self.conn.commit()
            row = self.conn.execute(
                "SELECT * FROM handoff_claims WHERE token=?",
                (token,)).fetchone()
            return self._confirm_view(dict(row), idempotent=False)

    def _confirm_view(self, row: dict, idempotent: bool) -> dict:
        confirmed = self.conn.execute(
            "SELECT confirmed_seq FROM handoff_cursor WHERE id=1"
        ).fetchone()["confirmed_seq"]
        return {"token": row["token"],
                "confirmed_upto": row["confirmed_upto"],
                "confirmed_seq": confirmed,
                "confirmed_at": row["confirmed_at"],
                "idempotent": idempotent}

    @staticmethod
    def _claim_view(row: dict) -> dict:
        if row["confirmed_upto"] is not None:
            state = "confirmed"
        elif row["superseded"]:
            state = "superseded"
        elif time.time() > row["expires_at"]:
            state = "expired"
        else:
            state = "outstanding"
        return {"token": row["token"], "start_seq": row["start_seq"],
                "end_seq": row["end_seq"], "issued_at": row["issued_at"],
                "expires_at": row["expires_at"], "state": state,
                "confirmed_upto": row["confirmed_upto"],
                "confirmed_at": row["confirmed_at"]}

    def close(self):
        c = getattr(self._local, "conn", None)
        if c:
            c.close()

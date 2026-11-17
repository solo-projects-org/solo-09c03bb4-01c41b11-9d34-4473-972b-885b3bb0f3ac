"""增量交接 API：发布入流、领取/确认/续领、重启恢复、慢上传不漏。

语义对照：
  * 发布为 ready 才入流，按实际发布顺序交接；失败上传不入流；
    幂等重放与主密钥改包不重复交接
  * 一次领取连续一页 + 限时令牌；处理后确认进度
  * 未确认/重启后可续领；到期重领换新令牌，旧令牌不能跳过新入流原片
  * 确认回执丢失：同令牌重试仍返回已确认进度（幂等）
  * 慢上传完成后才入流尾，不会被游标漏掉
"""
import sqlite3
import threading
import time

import httpx

from conftest import (TOKEN, Service, _download, _free_port, _upload,
                      _wait_rotation, API_DIR, KEY_DIR)


def _claim(client: httpx.Client, limit: int = 100,
           lease_seconds: float | None = None) -> httpx.Response:
    body: dict = {"limit": limit}
    if lease_seconds is not None:
        body["lease_seconds"] = lease_seconds
    return client.post("/handoff/claims", json=body)


def _confirm(client: httpx.Client, token: str, upto_seq: int) -> httpx.Response:
    return client.post(f"/handoff/claims/{token}/confirm",
                       json={"upto_seq": upto_seq})


def test_publish_order_page_and_confirm(stack):
    client = stack["client"]
    payloads = {}
    for i in range(3):
        data = f"slide-{i}".encode() * 50
        r = _upload(client, data, image_id=f"ho-{i}")
        assert r.status_code == 201, r.text
        payloads[f"ho-{i}"] = (r.json()["sha256"], len(data))

    # 第一页只要 2 条：按发布顺序交接，字段含编号/SHA-256/大小
    r = _claim(client, limit=2)
    assert r.status_code == 201, r.text
    page1 = r.json()
    assert page1["claim_token"]
    assert [it["image_id"] for it in page1["items"]] == ["ho-0", "ho-1"]
    assert [it["seq"] for it in page1["items"]] == [1, 2]
    for it in page1["items"]:
        assert it["sha256"] == payloads[it["image_id"]][0]
        assert it["size"] == payloads[it["image_id"]][1]
    assert page1["start_seq"] == 1 and page1["end_seq"] == 2

    # 确认进度（含第一页末尾），游标推进
    r = _confirm(client, page1["claim_token"], 2)
    assert r.status_code == 200, r.text
    assert r.json()["confirmed_seq"] == 2 and r.json()["idempotent"] is False

    # 第二页续领剩余一条；再领为空（不给令牌）
    r = _claim(client, limit=10)
    page2 = r.json()
    assert [it["image_id"] for it in page2["items"]] == ["ho-2"]
    assert _confirm(client, page2["claim_token"], 3).status_code == 200
    r = _claim(client)
    assert r.status_code == 200
    assert r.json()["claim_token"] is None and r.json()["items"] == []
    st = client.get("/handoff/status").json()
    assert st["confirmed_seq"] == 3 and st["pending"] == 0


def test_failed_and_rejected_uploads_not_in_stream(stack):
    client = stack["client"]
    _upload(client, b"ok", image_id="ho-ok")
    # 1) 超限请求在入口被拒（413），不产生任何记录
    env = dict(stack["api"]._env)
    env["MAX_UPLOAD_BYTES"] = "10"
    stack["api"].stop()
    stack["api"]._env = env
    stack["api"].start()
    r = _upload(client, b"x" * 100, image_id="ho-too-big")
    assert r.status_code == 413
    assert client.get("/images/ho-too-big/meta").status_code == 404
    # 2) 崩溃残留的在途上传：重启后判 failed，不得入流
    conn = sqlite3.connect(stack["data_dir"] / "metadata.db")
    mk = client.get("/keys/status").json()["current_key_id"]
    conn.execute(
        "INSERT INTO images (image_id, status, master_key_id, wrapped_dek, "
        "blob_path, created_at) VALUES ('ho-zombie','receiving',?,?,?,?)",
        (mk, "00" * 64, str(stack["blob_dir"] / "ho-zombie.msef"), time.time()))
    conn.commit()
    conn.close()
    stack["api"].restart()
    assert client.get("/images/ho-zombie/meta").json()["status"] == "failed"

    r = _claim(client, limit=10)
    items = r.json()["items"]
    assert [it["image_id"] for it in items] == ["ho-ok"]
    st = client.get("/handoff/status").json()
    assert st["published_total"] == 1


def test_idempotent_replay_not_duplicated_in_stream(stack):
    client = stack["client"]
    h = {"Content-Type": "application/octet-stream",
         "X-Idempotency-Token": "ho-idem-1"}
    r1 = client.post("/images", content=b"replay-body", headers=h)
    r2 = client.post("/images", content=b"replay-body", headers=h)
    assert r1.status_code == 201 and r2.status_code == 200
    assert r2.json()["image_id"] == r1.json()["image_id"]
    st = client.get("/handoff/status").json()
    assert st["published_total"] == 1
    r = _claim(client, limit=10)
    assert len(r.json()["items"]) == 1


def test_rotation_rewrap_not_duplicated_in_stream(stack):
    client = stack["client"]
    _upload(client, b"rot-stream", image_id="ho-rot")
    job = client.post("/rotations", json={"confirm": True}).json()
    assert _wait_rotation(client, job["job_id"])["state"] == "completed"
    # 改包只换信封，交接流仍只有一条
    st = client.get("/handoff/status").json()
    assert st["published_total"] == 1
    r = _claim(client, limit=10)
    items = r.json()["items"]
    assert [it["image_id"] for it in items] == ["ho-rot"]
    assert _download(client, "ho-rot") == b"rot-stream"


def test_expired_lease_reclaim_requires_new_token(stack):
    client = stack["client"]
    _upload(client, b"lease-1", image_id="ho-lease-1")
    r = _claim(client, limit=10, lease_seconds=1)
    token1 = r.json()["claim_token"]
    time.sleep(1.2)  # 等租约到期
    # 到期后旧令牌确认被拒
    r = _confirm(client, token1, 1)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_expired"
    # 重领：同一批内容、新令牌；旧令牌被取代
    r = _claim(client, limit=10)
    page = r.json()
    assert page["claim_token"] != token1
    assert [it["image_id"] for it in page["items"]] == ["ho-lease-1"]
    r = _confirm(client, token1, 1)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "claim_superseded"
    # 新令牌确认成功
    assert _confirm(client, page["claim_token"], 1).status_code == 200


def test_old_token_cannot_skip_newly_arrived(stack):
    client = stack["client"]
    _upload(client, b"a1", image_id="ho-a1")
    _upload(client, b"a2", image_id="ho-a2")
    page1 = _claim(client, limit=2, lease_seconds=1).json()
    assert [it["image_id"] for it in page1["items"]] == ["ho-a1", "ho-a2"]
    token1 = page1["claim_token"]
    time.sleep(1.2)
    # 租约到期期间又有新原片入流
    _upload(client, b"a3", image_id="ho-a3")
    # 重领：从第一个未确认位置起连续一页，旧页内容 + 新到的一片都不能少
    page2 = _claim(client, limit=10).json()
    assert [it["image_id"] for it in page2["items"]] == ["ho-a1", "ho-a2", "ho-a3"]
    assert page2["start_seq"] == 1
    # 旧令牌已死，不能用它确认（也就无法跳过新入流的 ho-a3）
    assert _confirm(client, token1, 2).status_code == 409
    assert _confirm(client, page2["claim_token"], 3).status_code == 200
    st = client.get("/handoff/status").json()
    assert st["confirmed_seq"] == 3 and st["pending"] == 0


def test_confirm_receipt_lost_retry_same_token(stack):
    client = stack["client"]
    _upload(client, b"rc-1", image_id="ho-rc-1")
    _upload(client, b"rc-2", image_id="ho-rc-2")
    page = _claim(client, limit=10).json()
    token = page["claim_token"]
    r1 = _confirm(client, token, 2)
    assert r1.status_code == 200 and r1.json()["idempotent"] is False
    # 回执丢失 -> 同令牌同进度重试：返回已确认进度，幂等
    r2 = _confirm(client, token, 2)
    assert r2.status_code == 200
    assert r2.json()["idempotent"] is True
    assert r2.json()["confirmed_seq"] == 2
    # 同令牌不同进度 -> 冲突，不悄悄改进度
    r3 = _confirm(client, token, 1)
    assert r3.status_code == 409
    assert r3.json()["detail"]["code"] == "already_confirmed"
    # 按令牌查询领取/确认状态
    view = client.get(f"/handoff/claims/{token}").json()
    assert view["state"] == "confirmed" and view["confirmed_upto"] == 2
    # 重启后重试仍查得到（确认已持久化）
    stack["api"].restart()
    r4 = _confirm(client, token, 2)
    assert r4.status_code == 200 and r4.json()["idempotent"] is True
    assert r4.json()["confirmed_seq"] == 2


def test_unconfirmed_reclaim_after_api_restart(stack):
    client = stack["client"]
    for i in range(3):
        _upload(client, f"rs-{i}".encode(), image_id=f"ho-rs-{i}")
    page1 = _claim(client, limit=2).json()
    assert [it["image_id"] for it in page1["items"]] == ["ho-rs-0", "ho-rs-1"]
    # 未确认即重启：游标未动，续领拿到同一批（新令牌）
    stack["api"].restart()
    page2 = _claim(client, limit=10).json()
    assert page2["claim_token"] != page1["claim_token"]
    assert [it["image_id"] for it in page2["items"]] == \
        ["ho-rs-0", "ho-rs-1", "ho-rs-2"]
    assert _confirm(client, page1["claim_token"], 2).status_code == 409
    assert _confirm(client, page2["claim_token"], 3).status_code == 200
    st = client.get("/handoff/status").json()
    assert st["confirmed_seq"] == 3 and st["pending"] == 0


def test_partial_confirm_reclaims_remainder(stack):
    client = stack["client"]
    for i in range(3):
        _upload(client, f"pc-{i}".encode(), image_id=f"ho-pc-{i}")
    page = _claim(client, limit=3).json()
    # 只处理完前两条就确认到 seq=2
    assert _confirm(client, page["claim_token"], 2).status_code == 200
    page2 = _claim(client, limit=10).json()
    assert [it["image_id"] for it in page2["items"]] == ["ho-pc-2"]
    assert page2["start_seq"] == 3
    # 越界确认被拒
    r = _confirm(client, page2["claim_token"], 99)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "upto_seq_out_of_range"


def test_slow_upload_not_missed_by_cursor(stack):
    """慢上传开始得早、完成得晚：入流位置在发布时刻，游标不会漏掉它。"""
    client = stack["client"]

    def slow_body():
        yield b"slow-head"
        time.sleep(2.0)
        yield b"slow-tail"

    result: dict = {}

    def do_slow_upload():
        r = client.put("/images/ho-slow", content=slow_body(),
                       headers={"Content-Type": "application/octet-stream"},
                       timeout=30)
        result["status"] = r.status_code

    t = threading.Thread(target=do_slow_upload)
    t.start()
    time.sleep(0.5)  # 慢上传已在途（receiving），但尚未发布
    _upload(client, b"fast", image_id="ho-fast")

    # 此时领取：只有已发布的 ho-fast；慢上传未完成不入流
    page1 = _claim(client, limit=10).json()
    assert [it["image_id"] for it in page1["items"]] == ["ho-fast"]
    assert _confirm(client, page1["claim_token"], 1).status_code == 200

    t.join()
    assert result["status"] == 201
    # 慢上传完成后出现在流尾，续领即可拿到，不会被已确认的游标漏掉
    page2 = _claim(client, limit=10).json()
    assert [it["image_id"] for it in page2["items"]] == ["ho-slow"]
    assert page2["items"][0]["seq"] == 2
    assert _confirm(client, page2["claim_token"], 2).status_code == 200
    assert _download(client, "ho-slow") == b"slow-head" + b"slow-tail"


def test_backfill_preexisting_ready_images(tmp_path):
    """功能上线前已 ready 的原片：建流时按发布时间回填，重启不重复。"""
    key_db = tmp_path / "keys"
    api_data = tmp_path / "api"
    key_db.mkdir()
    api_data.mkdir()
    db_file = api_data / "metadata.db"
    # 预置旧库：只有 images 表（无交接表），两条 ready 原片
    conn = sqlite3.connect(db_file)
    conn.execute(
        "CREATE TABLE images (image_id TEXT PRIMARY KEY, filename TEXT, "
        "content_type TEXT, status TEXT NOT NULL, master_key_id TEXT NOT NULL, "
        "wrapped_dek TEXT NOT NULL, blob_path TEXT NOT NULL, "
        "plain_size INTEGER, enc_size INTEGER, sha256 TEXT, idem_token TEXT, "
        "error TEXT, created_at REAL NOT NULL, finalized_at REAL)")
    for i, ts in ((0, 1000.0), (1, 1001.0)):
        conn.execute(
            "INSERT INTO images (image_id, status, master_key_id, wrapped_dek, "
            "blob_path, plain_size, sha256, created_at, finalized_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (f"legacy-{i}", "ready", "mk-legacy", "00" * 64,
             f"/data/blobs/legacy-{i}.msef", 10 + i, "ab" * 32, ts, ts))
    conn.commit()
    conn.close()

    kport, aport = _free_port(), _free_port()
    key_svc = Service("keystore", KEY_DIR, kport, {
        "KEYSTORE_DB": str(key_db / "keystore.db"),
        "KEY_SERVICE_TOKEN": TOKEN,
    })
    api_svc = Service("api", API_DIR, aport, {
        "METADATA_DB": str(db_file),
        "BLOB_DIR": str(api_data / "blobs"),
        "KEY_SERVICE_URL": f"http://127.0.0.1:{kport}",
        "KEY_SERVICE_TOKEN": TOKEN,
    })
    key_svc.start()
    api_svc.start()
    try:
        client = httpx.Client(base_url=api_svc.base, timeout=60)
        page = _claim(client, limit=10).json()
        assert [it["image_id"] for it in page["items"]] == \
            ["legacy-0", "legacy-1"]
        assert [it["seq"] for it in page["items"]] == [1, 2]
        # 重启不回填第二次
        api_svc.restart()
        st = client.get("/handoff/status").json()
        assert st["published_total"] == 2
        client.close()
    finally:
        api_svc.stop()
        key_svc.stop()

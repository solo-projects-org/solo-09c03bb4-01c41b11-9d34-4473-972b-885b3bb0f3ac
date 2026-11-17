# 显微成像原片加密数仓（单实例 API + 独立密钥服务）

显微仪器原片（raw image）的接收、流式加密存储与按号读取。每件原片使用
**独立数据密钥（DEK）**，DEK 由独立持久化的**密钥服务**用主密钥（MK）以
AES-256-GCM 信封包裹；支持主密钥在线轮换：只迁移密钥信封、**不重写图像正文**，
迁移核实完成后再销毁旧主密钥。

## 架构

```
仪器/客户端
   │  HTTP（octet-stream 流式上传 / 按号下载）
   ▼
┌──────────────────────────────┐        内部 HTTP（共享令牌）     ┌──────────────────────────┐
│ image-api (FastAPI, 单实例)   │  ────────────────────────────▶  │ keystore (FastAPI)        │
│  - 原片流式分帧加解密（内存）   │     generate/unwrap/rewrap     │  - 主密钥生命周期          │
│  - SQLite 元数据（只存密文信封）│     rotate/destroy/status      │  - DEK 包裹/解包/改包       │
│  - 轮换作业状态机 + 后台线程    │                                 │  - SQLite 持久卷（MK 本体） │
│  - blob：密文原片 .msef        │                                  └──────────────────────────┘
└──────────────┬───────────────┘                                            │
               │ 卷 api-data                    卷 keystore-data ◀──────────┘
        /data/metadata.db
        /data/blobs/*.msef（密文）
```

**安全边界**

| 资产 | 存在位置 | 不存在位置 |
|---|---|---|
| 主密钥材料 | 仅密钥服务进程内存与其持久卷 SQLite | API 进程/API 卷/日志 |
| 明文 DEK | 仅单次请求的 API 进程内存（请求结束即释放） | 任何磁盘 |
| 明文原片 | 仅请求/响应的内存流（multipart 假脱机已禁用） | 任何磁盘、任何临时文件 |
| 信封 DEK（密文） | API 的 SQLite `images.wrapped_dek` | — |
| 原片密文 | API 卷 `/data/blobs/*.msef`（AES-256-GCM 分帧） | — |

销毁主密钥 = 密钥库中密钥材料被零覆写并置 `destroyed`，密码学意义不可恢复；
对已销毁 MK 的 `unwrap/rewrap` 返回 `410 key_destroyed`。

## 快速启动

```bash
cp .env.example .env          # 修改 KEY_SERVICE_TOKEN 为强随机值
docker compose up --build
```

- API Swagger：<http://localhost:8000/docs>
- 密钥服务 Swagger：<http://localhost:8001/docs>（仅绑定 127.0.0.1）
- 健康检查：`GET /health`（两服务均有）

## 常用接口（API）

| 方法/路径 | 说明 |
|---|---|
| `POST /images` | 流式上传（随机编号）；`X-Idempotency-Token` 幂等；`X-Filename` 可选 |
| `PUT /images/{image_id}` | 按调用方编号上传 |
| `GET /images/{image_id}` | 按号下载，流式解密、逐帧 GCM 验签、SHA-256 校验 |
| `GET /images/{image_id}/meta` | 元数据/状态（不返回信封密文） |
| `GET /images` | 列表 |
| `POST /rotations` | 发起主密钥轮换（body `{"confirm": true}`），返回 202 + job_id |
| `GET /rotations/{job_id}` | 轮换进度（state/migrated/failed/progress_percent） |
| `GET /rotations/latest` | 最近一次轮换 |
| `GET /keys/status` | 代理密钥服务只读状态 |
| `POST /handoff/claims` | 领取一页待交接原片（连续 seq + 限时令牌）；无待交接时返回空页 |
| `POST /handoff/claims/{token}/confirm` | 确认本页已处理到的 seq（同令牌重试幂等） |
| `GET /handoff/claims/{token}` | 按令牌查询领取/确认状态 |
| `GET /handoff/status` | 交接游标、已发布/待处理计数、当前未完成的领取 |

上传请使用原始 `application/octet-stream`（`curl --data-binary @file`），
避免 multipart 带来的额外内存占用。

```bash
# 上传（按号）
curl -sS -X PUT --data-binary @slide_001.czi \
  -H 'Content-Type: application/octet-stream' \
  -H 'X-Filename: slide_001.czi' \
  http://localhost:8000/images/slide-001

# 下载并逐字节校验
curl -sS http://localhost:8000/images/slide-001 -o /tmp/slide_001.czi
sha256sum slide_001.czi /tmp/slide_001.czi

# 发起轮换 / 查进度
JOB=$(curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"confirm":true}' http://localhost:8000/rotations | python3 -c 'import sys,json;print(json.load(sys.stdin)["job_id"])')
watch -n1 "curl -sS http://localhost:8000/rotations/$JOB"
```

## 增量交接（显微图像分析程序）

单一固定消费者按"实际发布顺序"持续领取新入仓原片，断线/重启后从未处理的
位置继续：

1. **发布即入流**：密文写完、原片发布为 `ready` 的**同一事务**内，
   `(image_id, sha256, size)` 追加入 `handoff_stream`，自增 `seq` 即发布
   顺序。失败上传不入流；幂等重放与主密钥改包（rewrap）不会产生重复交接；
   慢上传只在完成发布时才进入流尾，不会被已确认的游标漏掉。
2. **领取**：`POST /handoff/claims`（body `{"limit": N, "lease_seconds": S}`，
   均可选）返回从第一个未确认位置起的**连续一页**（含每件的
   `seq/image_id/sha256/size`）与**限时令牌**及到期时间。没有待交接内容时
   返回空页、不签发令牌。
3. **取数处理**：按页内 `image_id` 调 `GET /images/{image_id}` 下载处理。
4. **确认**：`POST /handoff/claims/{token}/confirm`，body
   `{"upto_seq": S}`（S 须在本页范围内，可部分确认）。确认进度随令牌
   持久化，游标同步推进。
5. **续领**：未确认（含租约到期、消费者或 API 重启）可再次领取——从第一
   个未确认位置重新发页并签发**新令牌**，旧令牌同时被取代、不能再确认，
   因此旧令牌无法跳过新入流的原片。
6. **回执丢失**：确认已落盘但响应丢失时，用**同一令牌 + 同一 upto_seq**
   重试仍返回已确认进度（`idempotent: true`），即使该令牌此后已过期或
   被取代；`GET /handoff/claims/{token}` 与 `GET /handoff/status` 也可查。

```bash
# 领取一页（100 条以内，租约 300s）
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"limit": 100}' http://localhost:8000/handoff/claims
# 处理完后确认到 seq=42
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"upto_seq": 42}' http://localhost:8000/handoff/claims/<token>/confirm
```

## 轮换语义（需求对照）

1. `POST /rotations` 先在密钥服务生成并切换新 MK——**新入湖立即使用新 MK**；
   本地建作业（状态持久化），未完成时再次发起返回 `409 rotation_in_progress`。
2. 后台常驻线程逐行把旧 MK 包裹的 DEK 调密钥服务 `rewrap` 改包到新 MK，
   **只更新元数据里的 64 字节信封，blob 密文正文逐字节不动**。
3. 销毁前两次零残留核验（扫描后 + 销毁前），确认没有 `ready/receiving` 行
   仍依赖旧 MK，才调用销毁；销毁后再用只读 `status` 核实，**不提前报完成**。
4. **在途下载**：下载开始时只向密钥服务解包一次 DEK，之后帧解密完全在 API
   进程内存完成；即使下载很慢、期间旧 MK 被销毁，下载仍完整成功。
5. **在途上传**：上传期间发生轮换时，finalize 会先 rewrap 改绑当前 MK 再发布；
   若旧 MK 已被销毁（极端窗口）则返回 `409 old_key_destroyed` 并判失败，
   **绝不晚发布不可读原片**。进程崩溃留下的 `receiving` 在重启时一律判失败。
6. **慢密钥服务不阻塞**：迁移在独立线程、按批让出；在线上传/下载照常完成。
7. **回执丢失**：`destroy` 网络层失败/连接被重置时，API 自动重试并以只读状态
   核实销毁是否真实生效；rewrap 带幂等令牌，重放返回同一结果。
8. **进程重启接续**：轮换作业状态在 SQLite；API 重启后自动驱动未完成作业到
   终态；若崩溃发生在“MK 已切换、作业未建”窗口，启动时自动对账补建作业。

## 故障注入（演练用，密钥服务环境变量）

| 变量 | 作用 |
|---|---|
| `KEY_OP_DELAY=0.3` | 每个密钥操作人为延迟 0.3s，模拟慢密钥服务 |
| `KEY_DESTROY_DROP_RECEIPT=true` | 销毁成功落盘后故意不回完整 HTTP 响应，模拟销毁后丢回执 |

```bash
KEY_DESTROY_DROP_RECEIPT=true docker compose up --build keystore
```

## 自测

```bash
# tests/ 下为真实 uvicorn 子进程的端到端测试（33 项，含离线加密单测）
python -m pytest tests/ -q
```

覆盖：逐字节往返（空文件/跨帧/多帧、密文布局尺寸）、信封 AAD/错钥拒绝、
明文不落盘（blob/元数据库/临时文件三处核查）、幂等上传与密钥操作幂等、
篡改 GCM 失败、完整轮换（信封迁移+正文不动+二次轮换仍不动+销毁后可读）、
进度与并发轮换拒绝、慢密钥服务并发、在途下载跨销毁、销毁丢回执自动核实、
双服务重启可读性、轮换中途重启接续、崩溃在途上传不晚发布、
跨越轮换的上传自动改绑；增量交接（发布顺序、失败/重放/改包不入流、
限时令牌续领、旧令牌不跳过新片、确认回执丢失幂等、重启续领、
慢上传不漏、历史原片回填）。

## 限制与生产化建议（见 docs/ 下说明）

单实例 + SQLite 面向单机/单节点部署；扩成多实例需把轮换协调改为分布式锁、
blob 存储换成对象存储，并在密钥服务前加 mTLS/审计与 HSM/KMS 托管根密钥。

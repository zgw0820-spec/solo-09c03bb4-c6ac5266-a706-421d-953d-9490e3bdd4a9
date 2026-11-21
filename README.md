# ZNS 索引回收服务

录像索引在顺序写盘上反复更新，写满的区混着现行索引和旧副本。本服务是一个**单实例**的
FastAPI 回收服务：按块号（LBA）读写 4 KiB 索引块；用容量固定的文件模拟 ZNS 写区
（只能追加或整区复位，并留一个区周转）；值班员可逐个回收含旧副本的满区——迁出有效块、
整区复位、腾出的容量继续写入。搬迁期间读写照常；进程中断后从持久卷恢复，已确认的
写入保留，未完成的回收可重试。

## 实现说明

### 存储模型（模拟 ZNS 约束）

- 每个写区是 `DATA_DIR/zones/zone_<i>.zn` 一个文件，容量固定（`ZONE_CAPACITY_BYTES`）。
- 只允许两种写操作，代码里没有第三条路径：
  - **追加**：`pwrite` 到文件末尾（写指针 == 文件长度，启动时有断言校验）；
  - **整区复位**：`ftruncate(0)`。
- 区状态机：`FREE`（空）→ `ACTIVE`（追加目标）→ `FULL`（写满封口）→ 回收后回到 `FREE`。
- **留一个区周转**：`RESERVED_FREE_ZONES=1` 时，用户写不允许占用最后一个空区
  （再写返回 **507**）；只有 GC 搬迁可以使用它，保证回收永远有处可去。

### 持久化与崩溃恢复

- 确认顺序：**追加数据并 `fsync` 区文件 → 追加 journal 记录并 `fsync` → 更新内存映射 →
  应答客户端**。因此"已确认的写入"必然落在持久卷上。
- `journal.log`：每条 JSON 行记录一次映射变更（`write`/`relocate`/`trim`）或区事件
  （`activate`/`seal`/`reset`/`gc_begin`/`gc_done`），带单调序号。
- `snapshot.json`：定期（`JOURNAL_CHECKPOINT_OPS` 次操作）或干净退出时做快照
  （临时文件 + `rename`），随后才清空 journal；序号 ≤ 快照的记录重放时跳过，保证幂等。
- 启动恢复 = 读快照 + 重放 journal + **截掉区文件里"已追加但未确认"的尾巴**
  （文件比已确认写指针长的部分直接 `ftruncate` 掉——未确认的写入不生效）。
- 恢复时若发现 `gc_begin` 没有配对的 `gc_done`，说明回收中途崩溃：`/gc/status` 的
  `interrupted_zone` 会指出该区（仍是 `FULL`，数据完好），值班员重新 `POST /gc` 即可，
  已搬走的块有 journal 记录不会重搬。

### 回收（GC）流程

`POST /gc`（可指定区号，缺省选"垃圾字节最多"的满区）：

1. **逐块迁出有效块**：读旧块 → 追加到当前 `ACTIVE` 区（不够就滚动到空区，GC 可用
   保留区）→ 写 `relocate` journal → **CAS 换装映射**。每块一个关键段，块间让出事件
   循环，搬迁期间正常读写不阻塞。
2. **等待复位条件**：区内有效块数为 0（映射不再指向它）**且**在途读者数为 0
   （条件变量等待）。
3. **整区复位**：写 `reset` journal → `ftruncate(0)` → 区回到 `FREE`，容量可继续写。

### 搬迁中覆写：旧副本为何不生效

所有"改映射 + 写 journal"的关键段由同一把 `asyncio.Lock` 串行化。搬迁线程在同一把锁内
完成"读旧块 → 追加副本 → 仅当映射仍指向旧位置才换装（CAS）"。因此并发的覆写只有两种
时序：

- 覆写先被处理 → 旧位置已从该区的有效集合移除，GC 轮到这块时**跳过不搬**；
- 覆写后被处理 → 映射指向更新的副本，GC 搬来的旧副本成为垃圾，等下轮回收。

两种情况下旧副本都不会重新生效（自测场景 2 专门演练：搬迁途中覆写同一批块，
最终读回全部是最新版本）。

### 读与复位的互斥

读请求在锁内"查映射 + 读者计数 +1"，锁外读数据（区文件在复位前不可变），结束后计数
−1 并 `notify`。复位方在条件变量上等 `readers == 0`。由于复位前映射已不再指向该区，
不会有新读者进入，只有已在途的读者需要等（自测场景 4 用 `READ_DELAY_MS` 放大窗口验证）。

## API 一览（Swagger UI: `/docs`）

| 方法/路径 | 说明 |
| --- | --- |
| `PUT /blocks/{lba}` | 写 4 KiB 块（body 必须恰好 4096 字节）；写满返回 507 |
| `POST /blocks/{lba}/generate` | 演示辅助：按种子生成内容写入（Swagger 里好操作） |
| `GET /blocks/{lba}` | 读块（二进制），响应头带 `X-Zone/X-Offset/X-Generation` |
| `GET /blocks/{lba}/info` | 查块的物理位置与代数 |
| `GET /blocks/{lba}/verify?seed=` | 演示辅助：校验内容是否匹配种子 |
| `DELETE /blocks/{lba}` | trim（旧副本成为可回收垃圾） |
| `GET /zones` | 各区状态：写指针/有效块/垃圾字节/在途读者 |
| `POST /gc` | 回收一个满区：`{"zone": 0}` 或 `{}` 自动选区 |
| `GET /gc/status` | GC 状态；`interrupted_zone` 表示崩溃后待重试的回收 |
| `GET /stats` | 容量、映射、journal 总览 |
| `POST /checkpoint` | 立即做快照并清空 journal |

## 运行

### Docker Compose（推荐）

```bash
docker compose up --build -d
# Swagger: http://localhost:8000/docs
```

compose 配置：4 个区 × 128 KiB（32 块/区），1 个保留区，数据在命名卷 `zns-data` 上。

### 本地

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt   # 自测还需 requirements-dev.txt
DATA_DIR=./data .venv/bin/uvicorn app.main:app --port 8000
```

### 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `./data`（镜像内 `/data`） | 持久卷目录 |
| `ZONE_COUNT` | `4` | 写区数量 |
| `ZONE_CAPACITY_BYTES` | `1048576`（镜像内 `131072`） | 每区容量，须为 4096 的倍数 |
| `RESERVED_FREE_ZONES` | `1` | 为 GC 周转保留的空区数 |
| `JOURNAL_CHECKPOINT_OPS` | `1024` | 多少次操作做一次快照 |
| `GC_STEP_DELAY_MS` / `READ_DELAY_MS` | `0` | **测试钩子**：放大并发窗口，生产保持 0 |

## 在 Swagger / curl 里试写和回收

```bash
# 1) 写 64 块（zone0、zone1 写满；Swagger 里可用 POST /blocks/{lba}/generate 逐块试）
for i in $(seq 0 63); do
  curl -s -X POST localhost:8000/blocks/$i/generate -H 'Content-Type: application/json' -d '{"seed":"v1"}' >/dev/null
done
# 2) 覆写 0..31 → zone0 全变旧副本，zone2 被写满
for i in $(seq 0 31); do
  curl -s -X POST localhost:8000/blocks/$i/generate -H 'Content-Type: application/json' -d '{"seed":"v2"}' >/dev/null
done
# 3) 再写 → 507（保留区不开放给用户写）
curl -s -X POST localhost:8000/blocks/999/generate -H 'Content-Type: application/json' -d '{"seed":"x"}'
# 4) GET /zones：zone0 FULL、valid_blocks=0、stale_bytes=131072
curl -s localhost:8000/zones | jq
# 5) 回收 zone0（纯垃圾，moved_blocks=0，整区复位）
curl -s -X POST localhost:8000/gc -H 'Content-Type: application/json' -d '{"zone":0}'
# 6) 腾出的容量继续写
for i in $(seq 64 95); do
  curl -s -X POST localhost:8000/blocks/$i/generate -H 'Content-Type: application/json' -d '{"seed":"v1"}' >/dev/null
done
# 7) 校验数据（Swagger 里 GET /blocks/{lba}/verify 也行）
curl -s "localhost:8000/blocks/0/verify?seed=v2"    # → match: true（覆写后的新版本）
```

崩溃恢复演练：

```bash
docker compose kill                 # SIGKILL，模拟进程中断
docker compose up -d                # 从命名卷恢复
curl -s "localhost:8000/blocks/0/verify?seed=v2"   # 已确认的写入还在
curl -s localhost:8000/gc/status    # 若杀在 GC 中途，interrupted_zone 会指出待重试的区
```

搬迁中覆写演练：把 compose 里 `GC_STEP_DELAY_MS` 调成 `200` 后重启，在一个满区上发起
`POST /gc`，同时（Swagger 另一个标签页或 curl 循环）覆写该区的块，结束后逐个
`verify`——读回的始终是覆写后的版本。

## 自测

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python tests/selftest.py
```

自测会以子进程启动真实服务（uvicorn，临时数据目录），跑 5 个场景。本次交付前的实际输出
（带 `*` 的数字与时序相关，每次运行略有差异）：

```
[basic_gc_space] env=默认
  PASS 基本读写/覆写/trim/错误输入/演示辅助接口
  PASS 纯垃圾区回收（moved=0，整区复位）
  PASS 混合区回收（自动选区+搬迁 156 块），数据一致
  PASS 写满后保留区不开放给用户写（507，用户容量 765 块）
  PASS trim+回收后容量恢复，/stats 与映射一致
[overwrite_during_gc] env={'GC_STEP_DELAY_MS': 30}
  PASS 搬迁中覆写：moved=20 skipped=36*，读回全部是最新版本
[crash_restart] env={'GC_STEP_DELAY_MS': 30}
  PASS SIGKILL 重启后已确认写入保留（快照+journal 重放）
  PASS GC 中途被杀：数据完整，/gc/status 报告可重试
  PASS 重试回收完成（补搬 50* 块），数据一致
  PASS 二次崩溃恢复（快照与 journal 混合重放）
[read_blocks_reset] env={'GC_STEP_DELAY_MS': 5, 'READ_DELAY_MS': 500}
  PASS 复位等待在途读者（GC 耗时 0.44s*），在途读数据正确
[uncommitted_tail] env=默认
  PASS 未确认的追加尾巴被截除，后续写入复用该位置

全部场景通过，耗时 61.1s
```

另外手工验证了 GC 接口错误分支（无满区 400 / 指定非 FULL 区 400 / 非法区号 400 /
负 LBA 422）、`docker-compose.yml` 语法，以及"配置与数据卷不一致时拒绝启动"的保护。

## 遗留事项 / 已知限制

1. **单实例单进程**；4 KiB 级同步 I/O 在事件循环线程内执行（保证关键段原子性），
   吞吐有限。生产化应把 I/O 移到线程池/io_uring，或做批量+组提交（当前每块写 =
   2 次 fsync：区文件 + journal）。
2. 快照是全量 JSON：映射很大时 checkpoint 的耗时和空间会上升；journal 无分片。
3. GC 由值班员手动逐区触发，没有水位自动触发；选区策略是"垃圾字节最多"的简单贪心，
   未考虑磨损均衡。
4. 模拟而非真实设备：区是文件系统上的普通文件，未接 libzbd/真实 ZNS 盘；
   "追加/复位"约束由代码路径保证，而非设备强制。
5. 搬迁中覆写会产生"搬过去即失效"的垃圾块，空间短暂放大，靠后续 GC 回收。
6. 区数量/容量启动后不可变；改配置需清空数据卷重建。
7. 无鉴权与多租户；`generate/verify` 接口和 `GC_STEP_DELAY_MS`/`READ_DELAY_MS`
   是演示/自测钩子，生产部署应移除或保持为 0。
8. 本交付环境没有 Docker 守护进程，镜像未实际构建；`docker-compose.yml` 与
   `Dockerfile` 经静态校验，等价路径（同一 uvicorn 命令、同一组环境变量、同样的
   卷布局与 SIGKILL 重启）已在本地以子进程方式完整验证（见自测场景 3、5）。

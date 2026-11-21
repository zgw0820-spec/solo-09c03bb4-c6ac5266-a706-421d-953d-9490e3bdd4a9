# ZNS 索引回收服务

录像索引在顺序写盘上反复更新，写满的区混着现行索引和旧副本。本服务是一个**单实例**的
FastAPI 回收服务：按块号（LBA）读写 4 KiB 索引块；用容量固定的文件模拟 ZNS 写区
（只能追加或整区复位，并留一个区周转）；值班员可逐个回收含旧副本的满区——迁出有效块、
整区复位、腾出的容量继续写入。搬迁期间读写照常；进程中断后从持久卷恢复，已确认的
写入保留，未完成的回收可重试。**录像索引盘快满时还能边录边扩容**：`POST /zones/expand`
输入扩容后的写区总数，新区在后台逐个备妥（旧区照常读写和回收），全部备妥后一次开放，
未启用的区绝不参与分配，中断后可续做。

## 实现说明

### 存储模型（模拟 ZNS 约束）

- 每个写区是 `DATA_DIR/zones/zone_<i>.zn` 一个文件，容量固定（`ZONE_CAPACITY_BYTES`）。
- 只允许两种写操作，代码里没有第三条路径：
  - **追加**：`pwrite` 到文件末尾（写指针 == 文件长度，启动时有断言校验）；
  - **整区复位**：`ftruncate(0)`。
- 区状态机：`FREE`（空）→ `ACTIVE`（追加目标）→ `FULL`（写满封口）→ 回收后回到 `FREE`。
- **留一个区周转**：`RESERVED_FREE_ZONES=1` 时，用户写不允许占用最后一个空区
  （再写返回 **507**）；只有 GC 搬迁可以使用它，保证回收永远有处可去。

### 在线扩容（边录边加写区）

`POST /zones/expand`，入参为**扩容后的写区总数**（`target_zone_count`），只增不减。
新区沿用原有单区容量（`ZONE_CAPACITY_BYTES`）与周转空区预留数（`RESERVED_FREE_ZONES`），
这两个参数扩容期间不变。两阶段提交：

1. **准备阶段（PREPARING）**：逐个创建 `zone_<i>.zn` 空文件并 `fsync`（含目录项）。
   每个新区一个关键段、段间让出事件循环，因此旧区的读写和 GC **全程不阻塞**。
   新区处于 `PREPARING` 状态：写入分配（`_ensure_active_locked`）与回收选区
   （`_pick_victim_locked`）都只在已启用区切片 `[:enabled_count]` 内进行，
   **未启用的区绝不参与分配或回收**。
2. **一次开放（commit）**：所有新区备妥后，在同一个关键段内写 `expand_commit`
   journal 并把新区批量翻成 `FREE`，`enabled_count` 一次性推进——对外没有"半开"状态，
   之后写入分配和 GC 即可使用新区。

journal 新增三类事件保证崩溃语义（与正常路径共用快照+重放恢复）：

| 事件 | 含义 | 崩溃后的恢复行为 |
| --- | --- | --- |
| `expand_begin` | 扩容开始，记录 `from/target` | 重放重建 `PREPARING` 区；`/zones/expand/status` 报 `pending_target` |
| `expand_commit` | 新区一次开放 | 重放把 `PREPARING` 批量转 `FREE`，推进已启用区数 |
| `expand_abort` | 运行期准备失败，回滚未启用区 | 重放删除未启用区；残留孤儿文件启动时统一清理 |

- **准备失败/中断**：运行期失败（如磁盘满）会写 `expand_abort` 并关闭、删除所有未启用区，
  旧区与数据完全不受影响；commit 前进程被杀则在重启后保留 `PREPARING` 区和 `pending_target`，
  用**同一目标**再次调用即可续做（已建好的区只校验不重建），换其它目标返回 **409**。
- **重复提交同一目标**：已达成时直接 `noop` 返回，不会多加任何写区。
- **原配置重启**：`ZONE_COUNT` 只表示建卷时的初始区数；数据卷上的已启用区数只增不减，
  用原配置（甚至更小的 `ZONE_COUNT`，只要不大于已启用区数）重启，已启用的区和已确认
  写入全部保留。配置值大于卷上已启用区数时拒绝启动并提示改用扩容 API。

### 持久化与崩溃恢复

- 确认顺序：**追加数据并 `fsync` 区文件 → 追加 journal 记录并 `fsync` → 更新内存映射 →
  应答客户端**。因此"已确认的写入"必然落在持久卷上。
- `journal.log`：每条 JSON 行记录一次映射变更（`write`/`relocate`/`trim`）、区事件
  （`activate`/`seal`/`reset`/`gc_begin`/`gc_done`）或扩容事件
  （`expand_begin`/`expand_commit`/`expand_abort`），带单调序号。
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
| `GET /zones` | 各区状态：写指针/有效块/垃圾字节/在途读者；`enabled=false` 表示 PREPARING 未启用 |
| `POST /zones/expand` | 在线扩容到 `{"target_zone_count": N}`：备妥新区后一次开放；同值 no-op，缩容 400，冲突 409 |
| `GET /zones/expand/status` | 扩容状态：已启用区数、在途目标、PREPARING 区清单 |
| `POST /gc` | 回收一个满区：`{"zone": 0}` 或 `{}` 自动选区（只选已启用区） |
| `GET /gc/status` | GC 状态；`interrupted_zone` 表示崩溃后待重试的回收 |
| `GET /stats` | 容量、映射、journal 总览 |
| `POST /checkpoint` | 立即做快照并清空 journal |

## 运行

### Docker Compose（推荐）

```bash
docker compose up --build -d
# Swagger: http://localhost:8000/docs
```

compose 配置：初始 4 个区 × 128 KiB（32 块/区），1 个保留区，数据在命名卷 `zns-data` 上。
运行期需要更多写区时，在 Swagger（`POST /zones/expand`）或 curl 里在线扩容即可，
无需改 compose、无需停机，扩容结果随命名卷持久化。

### 本地

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt   # 自测还需 requirements-dev.txt
DATA_DIR=./data .venv/bin/uvicorn app.main:app --port 8000
```

### 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `./data`（镜像内 `/data`） | 持久卷目录 |
| `ZONE_COUNT` | `4` | **初始**写区数量；运行期用 `POST /zones/expand` 扩容后以数据卷为准，配置值不得大于卷上已启用区数 |
| `ZONE_CAPACITY_BYTES` | `1048576`（镜像内 `131072`） | 每区容量，须为 4096 的倍数；扩容新区沿用同一容量 |
| `RESERVED_FREE_ZONES` | `1` | 为 GC 周转保留的空区数；扩容新区沿用同一预留数 |
| `JOURNAL_CHECKPOINT_OPS` | `1024` | 多少次操作做一次快照 |
| `GC_STEP_DELAY_MS` / `READ_DELAY_MS` | `0` | **测试钩子**：放大并发窗口，生产保持 0 |
| `EXPAND_CRASH_DURING_PREPARE` / `EXPAND_CRASH_BEFORE_COMMIT` | `0` | **测试钩子**：分别在扩容"准备中途"/"commit 前"主动退出进程，演练中断续做；生产保持 0 |

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

在线扩容演练（Swagger 里对应 `POST /zones/expand`，填 `target_zone_count` 即可）：

```bash
# 1) 扩容前：GET /stats 的 zone_count=4；GET /zones 只有 zone_0..3
curl -s localhost:8000/zones/expand/status | jq
# 2) 扩容到 6 个写区（准备期间旧区读写/回收照常，新区先是 PREPARING，commit 后一次变 FREE）
curl -s -X POST localhost:8000/zones/expand -H 'Content-Type: application/json' \
  -d '{"target_zone_count":6}' | jq
# 3) 查看实际区数：GET /zones 返回 6 个区且全部 enabled=true，stats.zone_count=6
curl -s localhost:8000/zones | jq '.zones | length, .[].enabled'
# 4) 重复提交同一目标 → no-op，new_zones=0，不会多加区
curl -s -X POST localhost:8000/zones/expand -H 'Content-Type: application/json' -d '{"target_zone_count":6}'
# 5) 缩容被拒绝（400）；准备中断后换目标会被拒绝（409），需用同一目标续做
curl -s -X POST localhost:8000/zones/expand -H 'Content-Type: application/json' -d '{"target_zone_count":5}'
```

扩容中断演练（compose 里给 `EXPAND_CRASH_BEFORE_COMMIT=1` 加一行后重启）：

```bash
curl -s -X POST localhost:8000/zones/expand -H 'Content-Type: application/json' -d '{"target_zone_count":8}'
# 进程在 commit 前退出；docker compose up -d 拉起后：
curl -s localhost:8000/zones/expand/status   # enabled=旧值，preparing_zones 列出未启用区
# 未启用区不参与分配；同目标再调一次即续做完成
curl -s -X POST localhost:8000/zones/expand -H 'Content-Type: application/json' -d '{"target_zone_count":8}'
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

自测会以子进程启动真实服务（uvicorn，临时数据目录），跑 7 个场景。本次交付前的实际输出
（带 `*` 的数字与时序相关，每次运行略有差异）：

```
[basic_gc_space] env=默认
  PASS 基本读写/覆写/trim/错误输入/演示辅助接口
  PASS 纯垃圾区回收（moved=0，整区复位）
  PASS 混合区回收（自动选区+搬迁 156 块），数据一致
  PASS 写满后保留区不开放给用户写（507，用户容量 765 块）
  PASS trim+回收后容量恢复，/stats 与映射一致
[overwrite_during_gc] env={'GC_STEP_DELAY_MS': 30}
  PASS 搬迁中覆写：moved=16 skipped=40*，读回全部是最新版本
[crash_restart] env={'GC_STEP_DELAY_MS': 30}
  PASS SIGKILL 重启后已确认写入保留（快照+journal 重放）
  PASS GC 中途被杀：数据完整，/gc/status 报告可重试
  PASS 重试回收完成（补搬 46* 块），数据一致
  PASS 二次崩溃恢复（快照与 journal 混合重放）
[read_blocks_reset] env={'GC_STEP_DELAY_MS': 5, 'READ_DELAY_MS': 500}
  PASS 复位等待在途读者（GC 耗时 0.35s*），在途读数据正确
[uncommitted_tail] env=默认
  PASS 未确认的追加尾巴被截除，后续写入复用该位置
[online_expand] env=默认
  PASS 扩容 4→6：新区沿用原容量/预留数，准备期旧区写入不中断
  PASS 扩容后实际区数=6，容量与周转预留数沿用原配置
  PASS 重复目标 no-op 不加区；缩容 400；非法目标 422
  PASS 二次扩容到 8，新区承接写入，数据一致
  PASS 扩容后 GC 正常（回收满区并复位），数据一致
  PASS 原配置重启：8 个已启用区与全部已确认写入保留
[expand_interrupt] env=默认
  PASS 启用前中断：PREPARING 区不参与分配，同目标续做完成
  PASS 写入新区后 SIGKILL：8 区与新区上的确认写入全部保留

全部场景通过
```

另外手工/脚本验证了：扩容运行期准备失败（只读目录）触发 `expand_abort` 回滚、未启用区
被清理且旧区读写照常、回滚后可重新扩容；扩容接口错误分支（缩容 400 / 在途目标冲突 409 /
准备失败 507 / 目标 < 2 返回 422）；以及"配置 ZONE_COUNT 大于数据卷已启用区数时拒绝启动"
的保护。

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
6. 扩容只支持**增加写区数**（在线），不支持缩容，也不能在运行期改变单区容量或预留区数
   （这两项是新区严格沿用的不变量）；改变容量仍需清空数据卷重建。
7. 无鉴权与多租户；`generate/verify` 接口和 `GC_STEP_DELAY_MS`/`READ_DELAY_MS`/
   `EXPAND_CRASH_*` 是演示/自测钩子，生产部署应移除或保持为 0。
8. 本交付环境没有 Docker 守护进程，镜像未实际构建；`docker-compose.yml` 与
   `Dockerfile` 经静态校验（YAML 解析、环境变量与卷布局核对），等价路径（同一 uvicorn
   命令、同一组环境变量、同样的卷布局、SIGKILL/`os._exit` 重启）已在本地以子进程方式
   完整验证（见自测场景 3、5、6、7 及扩容中断手工脚本）。

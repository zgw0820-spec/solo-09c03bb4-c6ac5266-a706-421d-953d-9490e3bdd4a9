"""ZNS（分区命名空间）写区模拟与垃圾回收核心。

存储模型
--------
每个写区（zone）是 ``DATA_DIR/zones/zone_<i>.zn`` 的一个文件，语义上只允许两种写操作：

* **追加**：在写指针处扩展文件（``pwrite`` 到文件末尾，等价于 ZNS 的 sequential write）；
* **整区复位**：``ftruncate(0)``，等价于 ZNS 的 Zone Reset。

代码中不提供任何"原地覆写/部分删除"的路径，借此模拟 ZNS 硬盘的约束。

持久化与崩溃恢复
----------------
* 每个被确认的写/搬迁/复位都会先落数据（``fsync`` 区文件），再追加一条 JSON 行到
  ``journal.log``（``fsync``），最后才更新内存映射并应答客户端。
* 崩溃恢复 = 读快照（``snapshot.json``）+ 按序号重放 journal；区文件若比已确认的
  写指针长，说明存在"已追加但未确认"的尾巴，直接截掉——未确认的写入不生效。
* 快照定期（按操作数）或干净退出时写入：先写临时文件再 ``rename``，随后才清空
  journal；journal 记录带单调序号，快照外的记录才会被重放，保证幂等。

并发与 GC
---------
* 单实例、单事件循环；所有"改映射/写 journal"的关键段由一把 ``asyncio.Lock`` 串行化。
* 读请求在锁内完成"查映射 + 读者计数 +1"，锁外读数据（区文件在复位前不可变），
  结束时计数 -1 并 ``notify``。复位某区前必须满足：有效块数为 0 **且** 在途读者为 0
  （条件变量等待）。
* 搬迁（relocate）采用 CAS：在同一把锁内完成"读旧块 → 追加到目标区 → 写 journal →
  仅当映射仍指向旧位置才换装新映射"。因此与搬迁并发的覆写只有两种结果：
  覆写先发生 → 该块被跳过不搬；覆写后发生 → 映射指向更新的副本，搬来的旧副本
  成为垃圾等待下轮回收。**旧副本永远不会重新生效。**
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass

from .config import BLOCK_SIZE, Config

FREE = "FREE"      # 空区（已复位，可启用）
ACTIVE = "ACTIVE"  # 当前追加写的目标区
FULL = "FULL"      # 写满被封口的区（等待回收）


class NoSpaceError(Exception):
    """没有可供写入的区（保留区只许 GC 使用）。"""


class NoGcCandidateError(Exception):
    """没有可回收的满区。"""


class GcBusyError(Exception):
    """已有 GC 在进行。"""


@dataclass
class Loc:
    zone: int
    off: int


class Zone:
    __slots__ = ("id", "path", "fd", "state", "write_ptr", "valid", "readers")

    def __init__(self, zid: int, path: str) -> None:
        self.id = zid
        self.path = path
        self.fd = -1
        self.state = FREE
        self.write_ptr = 0          # 已确认的字节数（== 文件长度）
        self.valid: set[int] = set()  # 当前有效块对应的 LBA 集合
        self.readers = 0            # 在途读请求数


class ZnsStorage:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.zones_dir = os.path.join(cfg.data_dir, "zones")
        self.journal_path = os.path.join(cfg.data_dir, "journal.log")
        self.snapshot_path = os.path.join(cfg.data_dir, "snapshot.json")
        self.zones = [
            Zone(i, os.path.join(self.zones_dir, f"zone_{i}.zn"))
            for i in range(cfg.zone_count)
        ]
        self.mapping: dict[int, Loc] = {}        # LBA -> 物理位置
        self.generations: dict[int, int] = {}    # LBA -> 代数（每写一次 +1，仅用于观测）
        self.active: int | None = None
        self.op_seq = 0                          # journal 记录序号（单调递增）
        self._snap_seq = 0                       # 快照覆盖到的序号
        self.ops_since_ckpt = 0
        self._jfd = -1
        self._lock = asyncio.Lock()
        self._cond = asyncio.Condition(self._lock)  # 与 _lock 同一把锁，用于读者计数等待
        self._gc_lock = asyncio.Lock()
        self.gc_running = False
        self.gc_interrupted_zone: int | None = None  # 恢复时发现的有始无终的 GC
        self.last_gc_report: dict | None = None

    # ------------------------------------------------------------------
    # 启动恢复 / 关闭
    # ------------------------------------------------------------------

    def recover(self) -> None:
        """从持久卷恢复：快照 + journal 重放 + 截掉未确认的追加尾巴。"""
        os.makedirs(self.zones_dir, exist_ok=True)
        for z in self.zones:
            z.fd = os.open(z.path, os.O_RDWR | os.O_CREAT, 0o644)
        self._fsync_dir(self.zones_dir)

        self._load_snapshot()
        self._replay_journal()
        self._rebuild_valid_sets()

        cap = self.cfg.zone_capacity
        for z in self.zones:
            size = os.fstat(z.fd).st_size
            if z.state == FREE:
                # 复位已记入 journal 但 truncate 可能没来得及做：幂等补齐
                if size != 0:
                    os.ftruncate(z.fd, 0)
                    os.fsync(z.fd)
            elif size > z.write_ptr:
                # 崩溃时"已追加但未确认"的尾巴：截掉，未确认的写入不生效
                os.ftruncate(z.fd, z.write_ptr)
                os.fsync(z.fd)
            elif size < z.write_ptr:
                raise RuntimeError(
                    f"zone {z.id} 数据损坏：文件长度 {size} < 已确认写指针 {z.write_ptr}"
                )
            if z.state == ACTIVE and z.write_ptr >= cap:
                # 崩溃发生在"写满"与"封口记录"之间：按写指针推导封口
                z.state = FULL
                if self.active == z.id:
                    self.active = None

        if self.gc_interrupted_zone is not None:
            z = self.zones[self.gc_interrupted_zone]
            if z.state != FULL:
                # 复位其实已完成（或区已被复用），只是 gc_done 没来得及落盘
                self.gc_interrupted_zone = None

        self._jfd = os.open(self.journal_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        self._checkpoint_locked()  # 启动后顺手压缩一次 journal

    def close(self) -> None:
        """干净退出：做一次快照压缩并关闭文件（SIGKILL 时不会走到，用于崩溃演练）。"""
        try:
            self._checkpoint_locked()
        finally:
            if self._jfd >= 0:
                os.close(self._jfd)
                self._jfd = -1
            for z in self.zones:
                if z.fd >= 0:
                    os.close(z.fd)
                    z.fd = -1

    # ------------------------------------------------------------------
    # 快照与 journal
    # ------------------------------------------------------------------

    def _load_snapshot(self) -> None:
        if not os.path.exists(self.snapshot_path):
            return
        with open(self.snapshot_path, "r", encoding="utf-8") as f:
            snap = json.load(f)
        if (snap.get("zone_count"), snap.get("zone_capacity")) != (
            self.cfg.zone_count,
            self.cfg.zone_capacity,
        ):
            raise RuntimeError(
                "ZONE_COUNT/ZONE_CAPACITY_BYTES 与数据卷不一致；请恢复原配置或清空数据卷"
            )
        self._snap_seq = snap["seq"]
        self.op_seq = snap["seq"]
        self.active = snap["active"]
        self.gc_interrupted_zone = snap.get("gc_interrupted_zone")
        self.mapping = {int(k): Loc(v[0], v[1]) for k, v in snap["mapping"].items()}
        self.generations = {int(k): v for k, v in snap["generations"].items()}
        for z, zs in zip(self.zones, snap["zones"]):
            z.state = zs["state"]
            z.write_ptr = zs["write_ptr"]

    def _replay_journal(self) -> None:
        if not os.path.exists(self.journal_path):
            return
        with open(self.journal_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    break  # 撕裂的尾巴：该记录未被确认，丢弃
                seq = rec.get("seq", 0)
                if seq <= self._snap_seq:
                    continue  # 已包含在快照里（崩溃发生在快照与清空 journal 之间）
                self._apply_record(rec)
                self.op_seq = max(self.op_seq, seq)

    def _apply_record(self, rec: dict) -> None:
        op = rec["op"]
        if op in ("write", "relocate"):
            lba = rec["lba"]
            self.mapping[lba] = Loc(rec["zone"], rec["off"])
            self.generations[lba] = rec["gen"]
            self.zones[rec["zone"]].write_ptr += BLOCK_SIZE
        elif op == "trim":
            self.mapping.pop(rec["lba"], None)
            self.generations[rec["lba"]] = rec["gen"]
        elif op == "activate":
            z = self.zones[rec["zone"]]
            z.state = ACTIVE
            z.write_ptr = 0
            self.active = z.id
        elif op == "seal":
            z = self.zones[rec["zone"]]
            z.state = FULL
            if self.active == z.id:
                self.active = None
        elif op == "reset":
            z = self.zones[rec["zone"]]
            z.state = FREE
            z.write_ptr = 0
            if self.active == z.id:
                self.active = None
        elif op == "gc_begin":
            self.gc_interrupted_zone = rec["zone"]
        elif op == "gc_done":
            self.gc_interrupted_zone = None

    def _rebuild_valid_sets(self) -> None:
        for z in self.zones:
            z.valid.clear()
        for lba, loc in self.mapping.items():
            self.zones[loc.zone].valid.add(lba)

    def _journal_locked(self, rec: dict) -> None:
        """追加一条 journal 记录并 fsync（调用方须持有 _lock）。"""
        self.op_seq += 1
        rec["seq"] = self.op_seq
        os.write(self._jfd, (json.dumps(rec, separators=(",", ":")) + "\n").encode())
        os.fsync(self._jfd)
        self.ops_since_ckpt += 1

    def _maybe_checkpoint_locked(self) -> None:
        if self.ops_since_ckpt >= self.cfg.journal_checkpoint_ops:
            self._checkpoint_locked()

    def _checkpoint_locked(self) -> None:
        """写快照（临时文件 + rename）然后清空 journal（调用方须持有 _lock 或处于停机阶段）。"""
        snap = {
            "seq": self.op_seq,
            "zone_count": self.cfg.zone_count,
            "zone_capacity": self.cfg.zone_capacity,
            "active": self.active,
            "gc_interrupted_zone": self.gc_interrupted_zone,
            "mapping": {str(k): [v.zone, v.off] for k, v in self.mapping.items()},
            "generations": {str(k): v for k, v in self.generations.items()},
            "zones": [{"state": z.state, "write_ptr": z.write_ptr} for z in self.zones],
        }
        tmp = self.snapshot_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snap, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.snapshot_path)
        self._fsync_dir(self.cfg.data_dir)
        os.ftruncate(self._jfd, 0)
        os.fsync(self._jfd)
        self.ops_since_ckpt = 0

    @staticmethod
    def _fsync_dir(path: str) -> None:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    # ------------------------------------------------------------------
    # 内部工具（均须持有 _lock）
    # ------------------------------------------------------------------

    def _append_data_locked(self, z: Zone, data: bytes) -> None:
        """向区尾追加一个块并 fsync。这是唯一的"写数据"入口，保证只追加。"""
        assert len(data) == BLOCK_SIZE
        size = os.fstat(z.fd).st_size
        assert size == z.write_ptr, f"zone {z.id}: 文件长度 {size} != 写指针 {z.write_ptr}"
        n = os.pwrite(z.fd, data, z.write_ptr)
        assert n == BLOCK_SIZE
        os.fsync(z.fd)

    def _update_mapping_locked(self, lba: int, loc: Loc, gen: int) -> None:
        old = self.mapping.get(lba)
        if old is not None:
            self.zones[old.zone].valid.discard(lba)
        self.mapping[lba] = loc
        self.generations[lba] = gen
        self.zones[loc.zone].valid.add(lba)

    def _seal_if_full_locked(self, z: Zone) -> None:
        if z.write_ptr >= self.cfg.zone_capacity and z.state == ACTIVE:
            self._journal_locked({"op": "seal", "zone": z.id})
            z.state = FULL
            if self.active == z.id:
                self.active = None

    def _ensure_active_locked(self, for_user: bool) -> Zone:
        """返回可追加的 ACTIVE 区；必要时启用一个 FREE 区（滚动写指针）。

        用户写（for_user=True）不允许占用最后的保留空区——那是 GC 的周转空间；
        GC 搬迁（for_user=False）可以使用保留空区。
        """
        if self.active is not None:
            z = self.zones[self.active]
            if z.write_ptr < self.cfg.zone_capacity:
                return z
            self._seal_if_full_locked(z)
        free = [z for z in self.zones if z.state == FREE]
        reserved = self.cfg.reserved_free_zones if for_user else 0
        if len(free) <= reserved:
            raise NoSpaceError()
        z = free[0]
        self._journal_locked({"op": "activate", "zone": z.id})
        z.state = ACTIVE
        z.write_ptr = 0
        self.active = z.id
        return z

    # ------------------------------------------------------------------
    # 块读写
    # ------------------------------------------------------------------

    async def write_block(self, lba: int, data: bytes) -> dict:
        """确认顺序：追加数据并 fsync → 写 journal 并 fsync → 改内存映射 → 应答。"""
        async with self._lock:
            z = self._ensure_active_locked(for_user=True)
            off = z.write_ptr
            self._append_data_locked(z, data)
            gen = self.generations.get(lba, 0) + 1
            self._journal_locked({"op": "write", "lba": lba, "gen": gen, "zone": z.id, "off": off})
            self._update_mapping_locked(lba, Loc(z.id, off), gen)
            z.write_ptr += BLOCK_SIZE
            self._seal_if_full_locked(z)
            self._maybe_checkpoint_locked()
            return {"lba": lba, "zone": z.id, "offset": off, "generation": gen}

    async def read_block(self, lba: int) -> tuple[bytes, dict] | None:
        """查映射（锁内）→ 计数+1 → 锁外读数据 → 计数-1 并通知等待中的复位。"""
        async with self._lock:
            loc = self.mapping.get(lba)
            if loc is None:
                return None
            z = self.zones[loc.zone]
            z.readers += 1
            gen = self.generations[lba]
        try:
            if self.cfg.read_delay_ms:  # 测试钩子：放大"读进行中"的窗口
                await asyncio.sleep(self.cfg.read_delay_ms / 1000)
            data = os.pread(z.fd, BLOCK_SIZE, loc.off)
            if len(data) != BLOCK_SIZE:
                raise IOError(f"zone {z.id} 偏移 {loc.off} 短读 {len(data)} 字节")
            return data, {"lba": lba, "zone": loc.zone, "offset": loc.off, "generation": gen}
        finally:
            async with self._cond:
                z.readers -= 1
                self._cond.notify_all()

    async def trim_block(self, lba: int) -> bool:
        async with self._lock:
            loc = self.mapping.pop(lba, None)
            if loc is None:
                return False
            gen = self.generations.get(lba, 0) + 1
            self.generations[lba] = gen
            self._journal_locked({"op": "trim", "lba": lba, "gen": gen})
            self.zones[loc.zone].valid.discard(lba)
            self._maybe_checkpoint_locked()
            return True

    async def get_info(self, lba: int) -> dict | None:
        async with self._lock:
            loc = self.mapping.get(lba)
            if loc is None:
                return None
            return {
                "lba": lba,
                "zone": loc.zone,
                "offset": loc.off,
                "generation": self.generations.get(lba, 0),
            }

    # ------------------------------------------------------------------
    # 垃圾回收
    # ------------------------------------------------------------------

    async def gc(self, zone_id: int | None = None) -> dict:
        """回收一个满区：迁出有效块 → 等待在途读者 → 整区复位。单并发。"""
        if self._gc_lock.locked():
            raise GcBusyError()
        async with self._gc_lock:
            return await self._gc_inner(zone_id)

    def _pick_victim_locked(self, zone_id: int | None) -> int:
        if zone_id is not None:
            z = self.zones[zone_id]
            if z.state != FULL:
                raise NoGcCandidateError(f"zone {zone_id} 状态为 {z.state}，只有 FULL 区可回收")
            return zone_id
        candidates = [z for z in self.zones if z.state == FULL]
        if not candidates:
            raise NoGcCandidateError("没有 FULL 状态的区可回收")
        # 默认策略：旧副本（垃圾字节）最多的区优先
        victim = max(candidates, key=lambda z: (z.write_ptr - len(z.valid) * BLOCK_SIZE, -len(z.valid)))
        return victim.id

    async def _gc_inner(self, zone_id: int | None) -> dict:
        async with self._lock:
            victim_id = self._pick_victim_locked(zone_id)
            self.gc_running = True
            self._journal_locked({"op": "gc_begin", "zone": victim_id})
            self.gc_interrupted_zone = None
        moved = 0
        skipped = 0
        t0 = time.monotonic()
        try:
            # 第一阶段：逐个迁出有效块。每块一个关键段，块间让出事件循环，
            # 搬迁期间正常的读写请求照常进行。
            while True:
                async with self._lock:
                    lbas = list(self.zones[victim_id].valid)
                if not lbas:
                    break
                for lba in lbas:
                    async with self._lock:
                        loc = self.mapping.get(lba)
                        if loc is None or loc.zone != victim_id:
                            skipped += 1  # 搬迁期间被覆写/删除：旧副本已失效，不搬
                            continue
                        data = os.pread(self.zones[victim_id].fd, BLOCK_SIZE, loc.off)
                        zt = self._ensure_active_locked(for_user=False)
                        off = zt.write_ptr
                        self._append_data_locked(zt, data)
                        gen = self.generations[lba]
                        self._journal_locked({
                            "op": "relocate", "lba": lba, "gen": gen,
                            "zone": zt.id, "off": off,
                            "from_zone": victim_id, "from_off": loc.off,
                        })
                        # CAS：仅当映射仍指向旧位置才换装。锁内比较+换装是原子的，
                        # 因此并发的覆写不可能被搬来的旧副本覆盖回去。
                        if self.mapping.get(lba) == loc:
                            self._update_mapping_locked(lba, Loc(zt.id, off), gen)
                            zt.write_ptr += BLOCK_SIZE
                            self._seal_if_full_locked(zt)
                            moved += 1
                        else:  # 防御性分支：当前实现下不可达
                            skipped += 1
                        self._maybe_checkpoint_locked()
                    if self.cfg.gc_step_delay_ms:  # 测试钩子：放大并发窗口
                        await asyncio.sleep(self.cfg.gc_step_delay_ms / 1000)
                    else:
                        await asyncio.sleep(0)

            # 第二阶段：复位。此时区内已无有效块（映射不再指向它），
            # 只需等在途的读者结束，即可整区复位。
            async with self._cond:
                zv = self.zones[victim_id]
                while zv.readers > 0:
                    await self._cond.wait()
                self._journal_locked({"op": "reset", "zone": victim_id})
                os.ftruncate(zv.fd, 0)
                os.fsync(zv.fd)
                zv.state = FREE
                zv.write_ptr = 0
                if self.active == victim_id:
                    self.active = None
                self._journal_locked({"op": "gc_done", "zone": victim_id})
                self.gc_interrupted_zone = None
                self._maybe_checkpoint_locked()

            report = {
                "victim_zone": victim_id,
                "moved_blocks": moved,
                "skipped_blocks": skipped,
                "freed_bytes": self.cfg.zone_capacity,
                "duration_sec": round(time.monotonic() - t0, 3),
                "finished": True,
            }
            self.last_gc_report = report
            return report
        finally:
            self.gc_running = False

    # ------------------------------------------------------------------
    # 观测
    # ------------------------------------------------------------------

    def zone_stats(self) -> list[dict]:
        return [
            {
                "id": z.id,
                "state": z.state,
                "active": z.id == self.active,
                "write_ptr": z.write_ptr,
                "capacity": self.cfg.zone_capacity,
                "valid_blocks": len(z.valid),
                "stale_bytes": z.write_ptr - len(z.valid) * BLOCK_SIZE,
                "in_flight_readers": z.readers,
            }
            for z in self.zones
        ]

    def gc_status(self) -> dict:
        return {
            "running": self.gc_running,
            "interrupted_zone": self.gc_interrupted_zone,
            "last_report": self.last_gc_report,
        }

    def stats(self) -> dict:
        used = sum(z.write_ptr for z in self.zones)
        valid = len(self.mapping) * BLOCK_SIZE
        return {
            "block_size": BLOCK_SIZE,
            "zone_count": self.cfg.zone_count,
            "zone_capacity": self.cfg.zone_capacity,
            "total_capacity": self.cfg.zone_capacity * self.cfg.zone_count,
            "reserved_free_zones": self.cfg.reserved_free_zones,
            "used_bytes": used,
            "valid_bytes": valid,
            "stale_bytes": used - valid,
            "live_blocks": len(self.mapping),
            "free_zones": sum(1 for z in self.zones if z.state == FREE),
            "op_seq": self.op_seq,
            "journal_bytes": os.path.getsize(self.journal_path) if os.path.exists(self.journal_path) else 0,
            "gc": self.gc_status(),
        }

    async def checkpoint(self) -> dict:
        async with self._lock:
            self._checkpoint_locked()
        return {"checkpointed": True, "op_seq": self.op_seq}

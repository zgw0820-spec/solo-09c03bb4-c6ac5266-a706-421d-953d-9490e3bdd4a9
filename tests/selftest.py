#!/usr/bin/env python3
"""端到端自测：以子进程方式启动被测服务（uvicorn），使用临时数据目录。

场景：
  1. basic_gc_space      —— 基本读写/覆写/trim/错误输入；写满触发 507；回收腾空后继续写
  2. overwrite_during_gc —— 搬迁进行中覆写同一些块，搬来的旧副本不得生效
  3. crash_restart       —— SIGKILL 后已确认写入保留；GC 中途被杀可重试；再次崩溃恢复
  4. read_blocks_reset   —— 复位必须等待在途读者结束
  5. uncommitted_tail    —— 未确认的追加尾巴重启后被截除
  6. online_expand       —— 在线扩容：准备期旧区照常写、PREPARING 不参与分配、
                            一次开放、幂等 no-op、扩容后容量与保留区语义、GC 兼容
  7. expand_interrupt    —— 扩容启用前中断 / 写入新区后再中断：续做、回滚与持久化

运行：.venv/bin/python tests/selftest.py
"""
from __future__ import annotations

import hashlib
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BLOCK = 4096
ZONE_BLOCKS = 256
ZONE_CAP = ZONE_BLOCKS * BLOCK
PORT = 18101
BASE = f"http://127.0.0.1:{PORT}"


def payload(lba: int, ver: int) -> bytes:
    h = hashlib.sha256(f"{lba}:{ver}".encode()).digest()
    return (h * (BLOCK // len(h) + 1))[:BLOCK]


class Server:
    def __init__(self, data_dir: str, **env):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        e = dict(os.environ)
        e.update({
            "DATA_DIR": data_dir,
            "ZONE_COUNT": "4",
            "ZONE_CAPACITY_BYTES": str(ZONE_CAP),
            "RESERVED_FREE_ZONES": "1",
            "GC_STEP_DELAY_MS": "0",
            "READ_DELAY_MS": "0",
            "JOURNAL_CHECKPOINT_OPS": "200",  # 小阈值，让快照/重放路径被充分踩到
        })
        e.update({k: str(v) for k, v in env.items()})
        self._log = open(os.path.join(data_dir, "server.log"), "ab")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(PORT)],
            cwd=ROOT, env=e, stdout=self._log, stderr=subprocess.STDOUT,
        )
        self._wait_ready()

    def _wait_ready(self, timeout: float = 30.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                self._log.close()
                with open(os.path.join(self.data_dir, "server.log"), "rb") as f:
                    tail = f.read()[-4000:].decode(errors="replace")
                raise RuntimeError(f"服务进程提前退出 (rc={self.proc.returncode})\n{tail}")
            try:
                if httpx.get(f"{BASE}/health", timeout=2).status_code == 200:
                    return
            except httpx.TransportError:
                time.sleep(0.2)
        raise RuntimeError("服务启动超时")

    def kill(self):
        """SIGKILL：模拟进程崩溃（不走任何清理）。"""
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()
        self._log.close()

    def stop(self):
        if self.proc.poll() is not None:
            self._log.close()
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self._log.close()


def client() -> httpx.Client:
    return httpx.Client(base_url=BASE, timeout=120)


def put(c: httpx.Client, lba: int, ver: int):
    r = c.put(f"/blocks/{lba}", content=payload(lba, ver))
    assert r.status_code == 200, f"写 lba={lba} 失败: {r.status_code} {r.text}"


def put_many(c: httpx.Client, expected: dict, lbas, ver: int):
    for lba in lbas:
        put(c, lba, ver)
        expected[lba] = ver


def trim_many(c: httpx.Client, expected: dict, lbas):
    for lba in lbas:
        r = c.delete(f"/blocks/{lba}")
        assert r.status_code == 200, f"trim lba={lba} 失败: {r.status_code}"
        expected.pop(lba, None)


def verify_all(c: httpx.Client, expected: dict):
    for lba, ver in expected.items():
        r = c.get(f"/blocks/{lba}")
        assert r.status_code == 200, f"读 lba={lba} 失败: {r.status_code}"
        assert r.content == payload(lba, ver), f"lba={lba} 数据不是最新版本（旧副本生效了？）"


def zones(c: httpx.Client) -> list[dict]:
    return c.get("/zones").json()["zones"]


def zone_state(c: httpx.Client, zid: int) -> str:
    return [z for z in zones(c) if z["id"] == zid][0]["state"]


def wait_gc_running(c: httpx.Client, timeout: float = 10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if c.get("/gc/status").json()["running"]:
            return
        time.sleep(0.05)
    raise AssertionError("GC 没有开始运行")


def user_writable_blocks(c: httpx.Client) -> int:
    """当前还能为用户写接受多少块（保留区不算）。"""
    n = 0
    free = 0
    for z in zones(c):
        if z["state"] == "ACTIVE":
            n += (z["capacity"] - z["write_ptr"]) // BLOCK
        elif z["state"] == "FREE":
            free += 1
    n += max(0, free - 1) * ZONE_BLOCKS  # RESERVED_FREE_ZONES=1
    return n


# ---------------------------------------------------------------- 场景 1

def scenario_basic_gc_space(c: httpx.Client, srv: "Server", data_dir: str, env: dict):
    expected: dict[int, int] = {}
    # 基本读写 / 覆写 / trim / 错误输入
    put_many(c, expected, range(0, 10), 1)
    verify_all(c, expected)
    put(c, 5, 2)
    put(c, 5, 3)
    expected[5] = 3
    verify_all(c, expected)
    info = c.get("/blocks/5/info").json()
    assert info["generation"] == 3, info
    r = c.get("/blocks/5")
    assert r.headers["x-zone"] == str(info["zone"]) and r.headers["x-generation"] == "3"
    assert c.put("/blocks/7", content=b"short").status_code == 422
    assert c.get("/blocks/999999").status_code == 404
    assert c.delete("/blocks/9").status_code == 200
    assert c.get("/blocks/9").status_code == 404
    assert c.delete("/blocks/9").status_code == 404
    expected.pop(9)
    assert c.post("/blocks/1000/generate", json={"seed": "abc"}).status_code == 200
    assert c.get("/blocks/1000/verify", params={"seed": "abc"}).json()["match"] is True
    assert c.get("/blocks/1000/verify", params={"seed": "xyz"}).json()["match"] is False
    assert c.delete("/blocks/1000").status_code == 200
    print("  PASS 基本读写/覆写/trim/错误输入/演示辅助接口")

    # 写 300 块（zone0 满，zone1 有 44 块），再把 zone0 全部覆写成旧副本
    put_many(c, expected, range(10, 300), 1)
    put_many(c, expected, range(0, 256), 2)   # zone1 满后滚动到 zone2；zone0 变成纯垃圾
    z0 = [z for z in zones(c) if z["id"] == 0][0]
    assert z0["state"] == "FULL" and z0["valid_blocks"] == 0 and z0["stale_bytes"] == ZONE_CAP, z0

    # 回收纯垃圾区：无需搬迁，直接复位
    rep = c.post("/gc", json={"zone": 0}).json()
    assert rep["moved_blocks"] == 0 and rep["finished"], rep
    assert zone_state(c, 0) == "FREE"
    print("  PASS 纯垃圾区回收（moved=0，整区复位）")

    # 继续写满 zone2 并滚动到 zone0；再覆写 100 块让 zone2 混入旧副本
    put_many(c, expected, range(300, 600), 1)
    put_many(c, expected, range(300, 400), 2)
    z2 = [z for z in zones(c) if z["id"] == 2][0]
    assert z2["state"] == "FULL" and z2["valid_blocks"] == 156 and z2["stale_bytes"] == 100 * BLOCK, z2

    # 自动挑选 victim：应选中垃圾最多的 zone2，156 个有效块被搬走
    rep = c.post("/gc", json={}).json()
    assert rep["victim_zone"] == 2 and rep["moved_blocks"] == 156, rep
    assert zone_state(c, 2) == "FREE"
    verify_all(c, expected)
    print("  PASS 混合区回收（自动选区+搬迁 156 块），数据一致")

    # 把保留区之外全部写满：可写块数从 /zones 现算，写满后下一块应得 507
    n = user_writable_blocks(c)
    assert n > 0
    put_many(c, expected, range(600, 600 + n), 1)
    assert user_writable_blocks(c) == 0
    r = c.put("/blocks/9000", content=payload(9000, 1))
    assert r.status_code == 507, f"预期 507，实际 {r.status_code}: {r.text}"
    assert len([z for z in zones(c) if z["state"] == "FREE"]) == 1
    print(f"  PASS 写满后保留区不开放给用户写（507，用户容量 {len(expected)} 块）")

    # trim 刚写入的那批块制造垃圾 → 回收 zone3 → 容量恢复，可继续写
    trim_many(c, expected, range(600, 600 + n))
    z3 = [z for z in zones(c) if z["id"] == 3][0]
    assert z3["state"] == "FULL" and z3["stale_bytes"] == n * BLOCK, z3
    moved_before = z3["valid_blocks"]
    rep = c.post("/gc", json={"zone": 3}).json()
    assert rep["moved_blocks"] == moved_before, rep
    assert zone_state(c, 3) == "FREE"
    put_many(c, expected, range(768, 800), 1)
    verify_all(c, expected)
    st = c.get("/stats").json()
    assert st["live_blocks"] == len(expected), (st["live_blocks"], len(expected))
    assert st["valid_bytes"] == st["live_blocks"] * BLOCK
    print("  PASS trim+回收后容量恢复，/stats 与映射一致")


# ---------------------------------------------------------------- 场景 2

def scenario_overwrite_during_gc(c: httpx.Client, srv: "Server", data_dir: str, env: dict):
    expected: dict[int, int] = {}
    put_many(c, expected, range(0, 300), 1)      # zone0 满，zone1 有 44 块
    put_many(c, expected, range(0, 200), 2)      # zone0 剩 56 个有效块（lba 200..255）
    assert zone_state(c, 0) == "FULL"

    result: dict = {}

    def run_gc():
        try:
            r = client().post("/gc", json={"zone": 0})
            result["status"] = r.status_code
            result["rep"] = r.json()
        except Exception as e:  # noqa: BLE001
            result["error"] = repr(e)

    t = threading.Thread(target=run_gc)
    t.start()
    wait_gc_running(c)
    time.sleep(0.3)  # GC 正在逐块搬迁（56 块 × 30ms ≈ 1.7s）
    put_many(c, expected, range(200, 256), 3)  # 覆写正被搬迁的那些块
    t.join()

    assert "error" not in result, result
    assert result["status"] == 200, result
    rep = result["rep"]
    assert rep["moved_blocks"] + rep["skipped_blocks"] == 56, rep
    assert rep["skipped_blocks"] > 0, f"预期有块因覆写被跳过: {rep}"
    assert zone_state(c, 0) == "FREE"
    verify_all(c, expected)  # 关键断言：搬来的旧副本没有生效
    print(f"  PASS 搬迁中覆写：moved={rep['moved_blocks']} skipped={rep['skipped_blocks']}，读回全部是最新版本")


# ---------------------------------------------------------------- 场景 3

def scenario_crash_restart(c: httpx.Client, srv: "Server", data_dir: str, env: dict):
    expected: dict[int, int] = {}
    put_many(c, expected, range(0, 300), 1)
    put_many(c, expected, range(0, 200), 2)

    extra_servers: list[Server] = []
    try:
        # —— 崩溃（SIGKILL）：已确认的写入必须保留 ——
        srv.kill()
        srv2 = Server(data_dir, **env)
        extra_servers.append(srv2)
        c2 = client()
        verify_all(c2, expected)
        assert c2.get("/stats").json()["live_blocks"] == 300
        print("  PASS SIGKILL 重启后已确认写入保留（快照+journal 重放）")

        # —— GC 中途崩溃：未完成的回收可重试 ——
        def run_gc():
            try:
                client().post("/gc", json={"zone": 0})
            except Exception:  # noqa: BLE001 进程被杀，连接断开是预期
                pass

        t = threading.Thread(target=run_gc)
        t.start()
        wait_gc_running(c2)
        time.sleep(0.3)  # 搬了一部分（56 块 × 30ms），尚未到复位阶段
        srv2.kill()
        extra_servers.remove(srv2)
        t.join(timeout=10)

        srv3 = Server(data_dir, **env)
        extra_servers.append(srv3)
        c3 = client()
        st = c3.get("/gc/status").json()
        assert st["interrupted_zone"] == 0, f"应报告 zone0 的回收未完成: {st}"
        assert zone_state(c3, 0) == "FULL", "被杀时未到复位阶段，zone0 应仍是 FULL"
        verify_all(c3, expected)  # 无论搬没搬走，数据都必须完整
        print("  PASS GC 中途被杀：数据完整，/gc/status 报告可重试")

        rep = c3.post("/gc", json={"zone": 0}).json()
        assert rep["finished"] and rep["moved_blocks"] > 0, rep
        assert zone_state(c3, 0) == "FREE"
        assert c3.get("/gc/status").json()["interrupted_zone"] is None
        verify_all(c3, expected)
        print(f"  PASS 重试回收完成（补搬 {rep['moved_blocks']} 块），数据一致")

        # —— 再写一批跨快照的数据，二次崩溃验证 ——
        put_many(c3, expected, range(1000, 1250), 1)
        srv3.kill()
        extra_servers.remove(srv3)
        srv4 = Server(data_dir, **env)
        extra_servers.append(srv4)
        verify_all(client(), expected)
        print("  PASS 二次崩溃恢复（快照与 journal 混合重放）")
    finally:
        for s in extra_servers:
            s.stop()


# ---------------------------------------------------------------- 场景 4

def scenario_read_blocks_reset(c: httpx.Client, srv: "Server", data_dir: str, env: dict):
    expected: dict[int, int] = {}
    put_many(c, expected, range(0, 260), 1)   # zone0 满，zone1 有 4 块
    put_many(c, expected, range(0, 255), 2)   # zone0 只剩 lba 255 一个有效块
    assert zone_state(c, 0) == "FULL"

    read_result: dict = {}

    def slow_read():
        try:
            r = client().get("/blocks/255")  # READ_DELAY_MS=500，读会持有读者计数 ~0.5s
            read_result["status"] = r.status_code
            read_result["data"] = r.content
        except Exception as e:  # noqa: BLE001
            read_result["error"] = repr(e)

    t = threading.Thread(target=slow_read)
    t.start()
    time.sleep(0.2)  # 确保读请求已在途（持有 zone0 的读者计数）
    t0 = time.monotonic()
    rep = c.post("/gc", json={"zone": 0}).json()
    dur = time.monotonic() - t0
    t.join()

    assert "error" not in read_result, read_result
    assert read_result["status"] == 200
    assert read_result["data"] == payload(255, 1), "在途读必须拿到正确数据"
    assert rep["moved_blocks"] == 1, rep
    assert dur >= 0.2, f"复位应等待在途读者结束（耗时 {dur:.3f}s 过短）"
    assert zone_state(c, 0) == "FREE"
    # 抽查若干块（该实例读路径带 500ms 测试延迟，不做全量校验）
    for lba, ver in [(0, 2), (100, 2), (254, 2), (255, 1), (259, 1)]:
        assert c.get(f"/blocks/{lba}").content == payload(lba, ver), f"lba={lba}"
    print(f"  PASS 复位等待在途读者（GC 耗时 {dur:.2f}s），在途读数据正确")


# ---------------------------------------------------------------- 场景 5

def scenario_uncommitted_tail(c: httpx.Client, srv: "Server", data_dir: str, env: dict):
    """崩溃点落在"数据已追加、journal 未写"之间：重启后未确认的尾巴必须被截掉。"""
    expected: dict[int, int] = {}
    put_many(c, expected, range(10), 1)
    srv.stop()

    # 模拟崩溃现场：区文件尾部多出一个从未写进 journal 的块
    zone0 = os.path.join(data_dir, "zones", "zone_0.zn")
    with open(zone0, "ab") as f:
        f.write(b"\xaa" * BLOCK)
    assert os.path.getsize(zone0) == 11 * BLOCK

    extra: list[Server] = []
    try:
        srv2 = Server(data_dir, **env)
        extra.append(srv2)
        c2 = client()
        verify_all(c2, expected)  # 已确认的 10 块完好
        z0 = [z for z in zones(c2) if z["id"] == 0][0]
        assert z0["write_ptr"] == 10 * BLOCK, z0
        assert os.path.getsize(zone0) == 10 * BLOCK, "未确认的尾巴应被截掉"
        # 新写入应复用被截掉的位置，而不是跟在垃圾尾巴后面
        put(c2, 100, 1)
        expected[100] = 1
        info = c2.get("/blocks/100/info").json()
        assert info["zone"] == 0 and info["offset"] == 10 * BLOCK, info
        verify_all(c2, expected)
        print("  PASS 未确认的追加尾巴被截除，后续写入复用该位置")
    finally:
        for s in extra:
            s.stop()


# ---------------------------------------------------------------- 场景 6

def scenario_online_expand(c: httpx.Client, srv: "Server", data_dir: str, env: dict):
    expected: dict[int, int] = {}
    put_many(c, expected, range(0, 40), 1)  # zone0 满，zone1 有 8 块

    # 准备期并发写：扩容期间旧区读写照常（每个新区一个关键段，块间让出循环）
    written_during: dict[int, int] = {}

    def keep_writing():
        for lba in range(2000, 2030):
            try:
                put(client(), lba, 1)
                written_during[lba] = 1
            except Exception:  # noqa: BLE001 扩容极快，个别失败不影响主断言
                pass

    t = threading.Thread(target=keep_writing)
    t.start()
    rep = c.post("/zones/expand", json={"target_zone_count": 6}).json()
    t.join()
    assert rep["status"] == "expanded" and rep["resumed"] is False and rep["new_zones"] == 2, rep
    expected.update(written_during)
    print("  PASS 扩容 4→6：新区沿用原容量/预留数，准备期旧区写入不中断")

    # 实际区数：/zones 6 个、全部 enabled、无 PREPARING
    zs = zones(c)
    assert len(zs) == 6 and all(z["enabled"] for z in zs), zs
    st = c.get("/stats").json()
    assert st["zone_count"] == 6 and st["total_zone_count"] == 6
    assert st["total_capacity"] == 6 * ZONE_CAP and st["reserved_free_zones"] == 1
    assert c.get("/zones/expand/status").json()["preparing_zones"] == []
    print("  PASS 扩容后实际区数=6，容量与周转预留数沿用原配置")

    # 幂等：重复同一目标 no-op，不多加区
    rep = c.post("/zones/expand", json={"target_zone_count": 6}).json()
    assert rep["status"] == "noop" and rep["new_zones"] == 0, rep
    assert len(zones(c)) == 6
    # 缩容拒绝
    assert c.post("/zones/expand", json={"target_zone_count": 5}).status_code == 400
    # 非法目标（< 2）被 pydantic 挡下
    assert c.post("/zones/expand", json={"target_zone_count": 1}).status_code == 422
    print("  PASS 重复目标 no-op 不加区；缩容 400；非法目标 422")

    # 继续扩容到 8，并写入直到新区承接分配
    rep = c.post("/zones/expand", json={"target_zone_count": 8}).json()
    assert rep["new_zones"] == 2 and rep["target_zone_count"] == 8, rep
    # 回收已有的纯垃圾/混合满区，给后续滚动写入腾出空间（GC 与扩容共存验证）
    for zid in [z["id"] for z in zones(c) if z["state"] == "FULL"]:
        r = c.post("/gc", json={"zone": zid})
        assert r.status_code == 200, r.text
    # 持续写直到触及 zone6/zone7（8 区扣 1 保留，容量充足）
    touched = set()
    lba = 3000
    for _ in range(6 * ZONE_BLOCKS):
        r = c.put(f"/blocks/{lba}", content=payload(lba, 1))
        if r.status_code == 507:
            break
        assert r.status_code == 200, r.text
        touched.add(r.json()["zone"])
        expected[lba] = 1
        lba += 1
    assert touched & {6, 7}, f"新区未承接写入: {sorted(touched)}"
    verify_all(c, expected)
    print(f"  PASS 二次扩容到 8，新区承接写入（触及区 {sorted(touched)}），数据一致")

    # GC 兼容：再回收一个满区（含新区编号），复位后回到 FREE
    full_now = [z["id"] for z in zones(c) if z["state"] == "FULL"]
    assert full_now, "持续写后应存在满区"
    victim = full_now[0]
    rep = c.post("/gc", json={"zone": victim}).json()
    assert rep["finished"] and zone_state(c, victim) == "FREE", rep
    verify_all(c, expected)
    print(f"  PASS 扩容后 GC 正常（回收 zone{victim} 并复位），数据一致")

    # 原配置（ZONE_COUNT=4）重启：已启用的 8 个区与已确认写入全部保留
    srv.stop()
    srv2 = Server(data_dir, **env)
    try:
        c2 = client()
        st = c2.get("/stats").json()
        assert st["zone_count"] == 8 and st["total_zone_count"] == 8, st
        verify_all(c2, expected)
        print("  PASS 原配置重启：8 个已启用区与全部已确认写入保留")
    finally:
        srv2.stop()


# ---------------------------------------------------------------- 场景 7

def scenario_expand_interrupt(c: httpx.Client, srv: "Server", data_dir: str, env: dict):
    # 本场景自行管理服务生命周期（带崩溃钩子的专用实例）
    srv.stop()
    extra: list["Server"] = []
    try:
        # —— 中断一：启用前（prepare 中途）崩溃 ——
        srv_p = Server(data_dir, EXPAND_CRASH_DURING_PREPARE=1, **env)
        extra.append(srv_p)
        cp = client()
        put_many(cp, {}, range(0, 12), 1)
        try:
            cp.post("/zones/expand", json={"target_zone_count": 6})
        except httpx.HTTPError:
            pass  # 进程 os._exit，连接断开是预期
        srv_p.kill()
        extra.remove(srv_p)

        srv_r = Server(data_dir, **env)
        extra.append(srv_r)
        cr = client()
        st = cr.get("/stats").json()
        assert st["zone_count"] == 4, st  # 已启用区数不变
        preparing = st["preparing_zones"]
        assert preparing and all(z >= 4 for z in preparing), st
        assert st["expand"]["pending_target"] == 6
        # 未启用区绝不参与分配：新写仍落在 [0,4)
        for lba in range(100, 110):
            info = cr.post(f"/blocks/{lba}/generate", json={"seed": "x"}).json()
            assert info["zone"] < 4, info
        # 冲突目标 409；同目标续做成功
        assert cr.post("/zones/expand", json={"target_zone_count": 8}).status_code == 409
        rep = cr.post("/zones/expand", json={"target_zone_count": 6}).json()
        assert rep["status"] == "expanded" and rep["resumed"] is True, rep
        assert cr.get("/stats").json()["zone_count"] == 6
        print(f"  PASS 启用前中断：PREPARING={preparing} 不参与分配，同目标续做完成")

        # —— 中断二：写入新区后再中断（commit 前崩溃 + 启用后 SIGKILL）——
        # 先制造一次"备妥未启用"崩溃
        srv_r.stop()
        extra.remove(srv_r)
        srv_q = Server(data_dir, EXPAND_CRASH_BEFORE_COMMIT=1, **env)
        extra.append(srv_q)
        cq = client()
        try:
            cq.post("/zones/expand", json={"target_zone_count": 8})
        except httpx.HTTPError:
            pass
        srv_q.kill()
        extra.remove(srv_q)

        srv_s = Server(data_dir, **env)
        extra.append(srv_s)
        cs = client()
        st = cs.get("/stats").json()
        assert st["zone_count"] == 6 and st["preparing_zones"] == [6, 7], st
        # 续做启用
        rep = cs.post("/zones/expand", json={"target_zone_count": 8}).json()
        assert rep["resumed"] is True and rep["target_zone_count"] == 8, rep
        # 回收满区腾出滚动空间，再写入直到新区 6/7 承接
        for zid in [z["id"] for z in zones(cs) if z["state"] == "FULL"]:
            assert cs.post("/gc", json={"zone": zid}).status_code == 200
        expected_new: dict[int, int] = {}
        touched = set()
        lba = 5000
        for _ in range(6 * ZONE_BLOCKS):
            r = cs.put(f"/blocks/{lba}", content=payload(lba, 7))
            if r.status_code == 507:
                break
            touched.add(r.json()["zone"])
            expected_new[lba] = 7
            lba += 1
        assert touched & {6, 7}, touched
        # 已确认写入落盘后 SIGKILL
        srv_s.kill()
        extra.remove(srv_s)

        srv_t = Server(data_dir, **env)
        extra.append(srv_t)
        ct = client()
        st = ct.get("/stats").json()
        assert st["zone_count"] == 8 and not st["preparing_zones"], st
        for lba, ver in expected_new.items():
            assert ct.get(f"/blocks/{lba}").content == payload(lba, ver), lba
        # 早前的写入也还在
        for lba in range(0, 12):
            assert ct.get(f"/blocks/{lba}").status_code == 200
        print(f"  PASS 写入新区后 SIGKILL：8 区与新区上的 {len(expected_new)} 个确认写入全部保留")
    finally:
        for s in extra:
            s.stop()


# ---------------------------------------------------------------- main

def run_scenario(name: str, env: dict, fn):
    print(f"[{name}] env={env or '默认'}")
    data_dir = tempfile.mkdtemp(prefix=f"zns-test-{name}-")
    srv = Server(data_dir, **env)
    try:
        fn(client(), srv, data_dir, env)
    finally:
        srv.stop()
        shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    t0 = time.monotonic()
    run_scenario("basic_gc_space", {}, scenario_basic_gc_space)
    run_scenario("overwrite_during_gc", {"GC_STEP_DELAY_MS": 30}, scenario_overwrite_during_gc)
    run_scenario("crash_restart", {"GC_STEP_DELAY_MS": 30}, scenario_crash_restart)
    run_scenario("read_blocks_reset", {"GC_STEP_DELAY_MS": 5, "READ_DELAY_MS": 500}, scenario_read_blocks_reset)
    run_scenario("uncommitted_tail", {}, scenario_uncommitted_tail)
    run_scenario("online_expand", {}, scenario_online_expand)
    run_scenario("expand_interrupt", {}, scenario_expand_interrupt)
    print(f"\n全部场景通过，耗时 {time.monotonic() - t0:.1f}s")

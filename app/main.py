"""ZNS 索引回收服务（单实例）——FastAPI 接口层。

按块号（LBA）读写 4 KiB 索引块；写满的区混着现行索引与旧副本，
值班员通过 POST /gc 逐区回收，腾出容量继续写入。
Swagger UI: /docs
"""
from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Path, Query, Request, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from .config import BLOCK_SIZE, Config
from .storage import (
    ExpansionConflictError,
    ExpansionFailedError,
    ExpansionInProgressError,
    GcBusyError,
    NoGcCandidateError,
    NoSpaceError,
    ZnsStorage,
)


def _seeded_block(seed: str) -> bytes:
    """由种子生成确定性的 4 KiB 内容（演示/自测辅助）。"""
    h = hashlib.sha256(seed.encode()).digest()
    return (h * (BLOCK_SIZE // len(h) + 1))[:BLOCK_SIZE]


@asynccontextmanager
async def lifespan(app: FastAPI):
    storage = ZnsStorage(Config())
    storage.recover()  # 从持久卷恢复：已确认的写入保留，未完成的回收可重试
    app.state.storage = storage
    yield
    storage.close()  # 干净退出才走这里；SIGKILL 用于演练崩溃恢复


app = FastAPI(
    title="ZNS 索引回收服务",
    version="1.0.0",
    description="顺序写盘上的录像索引：4 KiB 块读写、满区回收、搬迁与复位、崩溃恢复。",
    lifespan=lifespan,
)


def _st(request: Request) -> ZnsStorage:
    return request.app.state.storage


@app.get("/", include_in_schema=False)
async def index():
    return RedirectResponse("/docs")


@app.get("/health", summary="健康检查")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------- 块读写

@app.put(
    "/blocks/{lba}",
    summary="写入一个 4 KiB 索引块（body 必须恰好 4096 字节）",
    responses={507: {"description": "无可用写区（保留区仅供 GC），请先回收"}},
)
async def put_block(request: Request, lba: int = Path(ge=0, description="块号 LBA")):
    data = await request.body()
    if len(data) != BLOCK_SIZE:
        raise HTTPException(422, f"块必须恰好 {BLOCK_SIZE} 字节，收到 {len(data)} 字节")
    try:
        return await _st(request).write_block(lba, data)
    except NoSpaceError:
        raise HTTPException(507, "没有可写的区：保留空区仅供 GC 周转，请先 POST /gc 回收满区")


class GenerateReq(BaseModel):
    seed: str = Field(examples=["cam-01/2026-11-20"])


@app.post("/blocks/{lba}/generate", summary="演示辅助：用种子生成内容并写入该块")
async def generate_block(body: GenerateReq, request: Request, lba: int = Path(ge=0)):
    data = _seeded_block(f"{lba}:{body.seed}")
    try:
        return await _st(request).write_block(lba, data)
    except NoSpaceError:
        raise HTTPException(507, "没有可写的区：保留空区仅供 GC 周转，请先 POST /gc 回收满区")


@app.get(
    "/blocks/{lba}",
    summary="读取一个 4 KiB 索引块",
    responses={404: {"description": "该块从未写入或已被 trim"}},
)
async def get_block(request: Request, lba: int = Path(ge=0)):
    res = await _st(request).read_block(lba)
    if res is None:
        raise HTTPException(404, "块不存在")
    data, info = res
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "X-Zone": str(info["zone"]),
            "X-Offset": str(info["offset"]),
            "X-Generation": str(info["generation"]),
        },
    )


@app.get("/blocks/{lba}/info", summary="查看块的物理位置与代数")
async def block_info(request: Request, lba: int = Path(ge=0)):
    info = await _st(request).get_info(lba)
    if info is None:
        raise HTTPException(404, "块不存在")
    return info


@app.get("/blocks/{lba}/verify", summary="演示辅助：校验块内容是否匹配种子")
async def verify_block(request: Request, lba: int = Path(ge=0), seed: str = Query(...)):
    res = await _st(request).read_block(lba)
    if res is None:
        raise HTTPException(404, "块不存在")
    data, info = res
    return {"lba": lba, "match": data == _seeded_block(f"{lba}:{seed}"), **info}


@app.delete("/blocks/{lba}", summary="trim 一个块（其旧副本成为可回收垃圾）")
async def delete_block(request: Request, lba: int = Path(ge=0)):
    if not await _st(request).trim_block(lba):
        raise HTTPException(404, "块不存在")
    return {"lba": lba, "trimmed": True}


# ---------------------------------------------------------------- 区与回收

@app.get("/zones", summary="列出所有写区状态（写指针/有效块/垃圾字节/在途读者）")
async def list_zones(request: Request):
    st = _st(request)
    return {
        "block_size": BLOCK_SIZE,
        "zone_capacity": st.cfg.zone_capacity,
        "enabled_zone_count": st.enabled_count,
        "zones": st.zone_stats(),
    }


class ExpandReq(BaseModel):
    target_zone_count: int = Field(
        ge=2,
        description="扩容后的写区（已启用区）总数；不能小于当前已启用区数，重复提交相同值不会多加区",
    )


@app.post(
    "/zones/expand",
    summary="在线扩容：准备新区（旧区照常读写/回收）→ 全部备妥后一次开放给写入分配",
    responses={
        400: {"description": "目标区数小于当前已启用区数（不支持缩容），或准备失败后未启用区已回滚"},
        409: {"description": "已有扩容在进行，或目标与崩溃遗留的在途扩容目标冲突"},
        507: {"description": "新区准备失败（如磁盘空间不足）；未启用区不参与分配，旧区不受影响"},
    },
)
async def expand_zones(body: ExpandReq, request: Request):
    st = _st(request)
    try:
        return await st.expand(body.target_zone_count)
    except ExpansionInProgressError as e:
        raise HTTPException(409, str(e))
    except ExpansionConflictError as e:
        raise HTTPException(409, str(e))
    except ExpansionFailedError as e:
        raise HTTPException(507, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.get("/zones/expand/status", summary="扩容状态：已启用区数、在途目标与 PREPARING 区清单")
async def expand_status(request: Request):
    return _st(request).expand_status()


class GcReq(BaseModel):
    zone: int | None = Field(
        default=None,
        description="要回收的满区编号；缺省则自动挑选旧副本最多的满区",
    )


@app.post(
    "/gc",
    summary="回收一个满区：迁出有效块 → 等在途读者 → 整区复位",
    responses={
        400: {"description": "没有可回收的满区，或指定区不是 FULL"},
        409: {"description": "已有 GC 在进行"},
        507: {"description": "没有空区可接纳迁出的块"},
    },
)
async def run_gc(body: GcReq, request: Request):
    st = _st(request)
    if body.zone is not None and not 0 <= body.zone < st.enabled_count:
        raise HTTPException(400, f"区编号须在已启用区 [0, {st.enabled_count}) 内")
    try:
        return await st.gc(body.zone)
    except GcBusyError:
        raise HTTPException(409, "已有 GC 在进行，稍后再试")
    except NoGcCandidateError as e:
        raise HTTPException(400, str(e))
    except NoSpaceError:
        raise HTTPException(507, "没有空区可接纳迁出的有效块")


@app.get("/gc/status", summary="GC 状态（含崩溃后待重试的回收）")
async def gc_status(request: Request):
    return _st(request).gc_status()


@app.get("/stats", summary="容量、映射与 journal 总览")
async def stats(request: Request):
    return _st(request).stats()


@app.post("/checkpoint", summary="立即做一次快照并清空 journal")
async def checkpoint(request: Request):
    return await _st(request).checkpoint()

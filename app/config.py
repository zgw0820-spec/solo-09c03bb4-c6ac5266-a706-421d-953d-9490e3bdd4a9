"""运行配置（全部来自环境变量，带默认值）。"""
from __future__ import annotations

import os

BLOCK_SIZE = 4096  # 索引块固定 4 KiB


class Config:
    def __init__(self) -> None:
        self.data_dir = os.environ.get("DATA_DIR", "./data")
        self.zone_count = int(os.environ.get("ZONE_COUNT", "4"))
        self.zone_capacity = int(os.environ.get("ZONE_CAPACITY_BYTES", str(1024 * 1024)))
        # 始终为 GC 周转保留的空区数量（用户写不允许占用最后这几个空区）
        self.reserved_free_zones = int(os.environ.get("RESERVED_FREE_ZONES", "1"))
        # 测试钩子：GC 每搬一块后的停顿（毫秒），用于放大并发窗口；生产应为 0
        self.gc_step_delay_ms = int(os.environ.get("GC_STEP_DELAY_MS", "0"))
        # 测试钩子：每次读请求持有读者计数的额外时长（毫秒）；生产应为 0
        self.read_delay_ms = int(os.environ.get("READ_DELAY_MS", "0"))
        # journal 累积多少条操作后做一次快照压缩
        self.journal_checkpoint_ops = int(os.environ.get("JOURNAL_CHECKPOINT_OPS", "1024"))

        if self.zone_capacity % BLOCK_SIZE != 0:
            raise ValueError("ZONE_CAPACITY_BYTES 必须是 4096 的整数倍")
        if self.zone_count < 2:
            raise ValueError("ZONE_COUNT 至少为 2")
        if not 0 <= self.reserved_free_zones < self.zone_count:
            raise ValueError("RESERVED_FREE_ZONES 必须在 [0, ZONE_COUNT) 内")

    @property
    def blocks_per_zone(self) -> int:
        return self.zone_capacity // BLOCK_SIZE

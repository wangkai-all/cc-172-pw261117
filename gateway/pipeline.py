"""处理管线：帧事件 → 回绕/重启识别 → 配置解析 → 去重入库 → 广播。

关键不变式：样本套用哪个配置版本只由 (channel, epoch, seq) 决定，
与样本何时被处理无关（暂停、积压在缓冲里再处理，结果一致）。
"""
from __future__ import annotations

import threading

from . import parser as P
from . import protocol
from .store import Store

# 序号分类
NEW = "new"          # 该通道首次出现
FORWARD = "forward"  # 正常递增
SAME = "same"        # 与上一序号相同（重发）
WRAP = "wrap"        # 自然回绕
RESTART = "restart"  # 设备重启（序号非回绕式变小）


def classify_seq(last_seq: int | None, seq: int) -> str:
    if last_seq is None:
        return NEW
    if seq == last_seq:
        return SAME
    if seq > last_seq:
        return FORWARD
    if last_seq >= protocol.WRAP_HIGH and seq <= protocol.WRAP_LOW:
        return WRAP
    return RESTART


class Pipeline:
    def __init__(self, store: Store, broadcast=None):
        self.store = store
        self._broadcast = broadcast or (lambda etype, payload: None)
        self._lock = threading.Lock()
        # 内存中的通道游标，启动时从库中恢复（重启保留状态）
        self._cursor: dict[int, dict] = {}
        for st in store.all_channel_states():
            self._cursor[st["channel"]] = {"epoch": st["epoch"], "last_seq": st["last_seq"]}
        # 自最近一次连接建立以来已见过哪些通道的包。
        # 设备重启的物理信号是连接断开：重连后序号回跳判重启；
        # 连接没断时的序号回跳且该序号已入库，判重发。
        self._seen_since_connect: set[int] = set()

    def on_connect(self) -> None:
        with self._lock:
            self._seen_since_connect.clear()

    def handle_events(self, events: list[dict]) -> None:
        for ev in events:
            if ev["kind"] == P.PACKET:
                self._handle_packet(ev)
            else:  # noise / bad_eof / bad_checksum / truncated
                row = self.store.insert_error(
                    kind=ev["kind"], stream_offset=ev["offset"],
                    raw=ev["raw"], detail=ev.get("detail", ""))
                self._broadcast("error", row)

    # ------------------------------------------------------------------
    def _handle_packet(self, ev: dict) -> None:
        ch, seq = ev["channel"], ev["seq"]
        with self._lock:
            cur = self._cursor.get(ch)
            last_seq = cur["last_seq"] if cur else None
            epoch = cur["epoch"] if cur else 0
            cls = classify_seq(last_seq, seq)
            fresh = ch not in self._seen_since_connect
            self._seen_since_connect.add(ch)
            # 判重：序号与游标相同，或连接未断时回跳到当前 epoch 已入库的序号 → 重发。
            # 注意必须先分类：回绕后的序号会与上一 epoch 的历史序号撞车，
            # 那是新样本而不是重发；重连后的回跳是设备重启，也不是重发。
            is_dup = (
                cls == SAME
                or (cls == FORWARD and self.store.sample_exists(ch, epoch, seq))
                or (cls == RESTART and not fresh
                    and self.store.sample_exists(ch, epoch, seq))
            )
            if is_dup:
                self._event("duplicate", ch,
                            f"duplicate sample epoch={epoch} seq={seq} ignored")
                self._broadcast("stats", self.store.stats())
                return
            flags = ""
            if cls == WRAP:
                epoch += 1
                flags = "wrap"
                self._event("wrap", ch,
                            f"seq wrapped {last_seq} -> {seq}, epoch {epoch - 1} -> {epoch}")
            elif cls == RESTART:
                epoch += 1
                flags = "restart"
                self._event("restart", ch,
                            f"device restart suspected: seq {last_seq} -> {seq}, "
                            f"epoch {epoch - 1} -> {epoch}")

            cfg = self.store.resolve_config(ch, epoch, seq)
            scaled = ev["value"] * cfg["scale"]
            inserted = self.store.insert_sample(
                channel=ch, epoch=epoch, seq=seq, raw=ev["raw"],
                raw_value=ev["value"], scale=cfg["scale"], unit=cfg["unit"],
                scaled=scaled, config_version=cfg["id"],
                stream_offset=ev["offset"], flags=flags)

            if not inserted:
                self._event("duplicate", ch,
                            f"duplicate sample epoch={epoch} seq={seq} ignored")
                self._broadcast("stats", self.store.stats())
                return

            self._cursor[ch] = {"epoch": epoch, "last_seq": seq}
            self.store.set_channel_state(ch, epoch, seq)

        self._broadcast("sample", {
            "channel": ch, "epoch": epoch, "seq": seq,
            "raw_hex": ev["raw"].hex(), "raw_value": ev["value"],
            "scale": cfg["scale"], "unit": cfg["unit"], "scaled": scaled,
            "config_version": cfg["id"], "stream_offset": ev["offset"],
            "flags": flags,
        })

    def _event(self, kind: str, channel, detail: str) -> None:
        row = self.store.insert_event(kind, channel, detail)
        self._broadcast("event", row)

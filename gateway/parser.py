"""增量分帧器：把字节流切成 样本帧 / 噪声段 / 坏包 / 截断包。

天然覆盖三种现场情况：
- 分片到达：内部缓冲凑不满 12 字节就继续等；
- 连续粘包：缓冲里有多少帧就连续解多少帧；
- 噪声字节：起始标记之前的字节记为噪声段，带流内绝对偏移。
"""
from __future__ import annotations

from . import protocol

# 事件类型
PACKET = "packet"          # 校验通过的样本帧
NOISE = "noise"            # 起始标记之前的噪声字节
BAD_EOF = "bad_eof"        # 结束标记错误
BAD_CHECKSUM = "bad_checksum"
TRUNCATED = "truncated"    # 连接结束时残留的半包


class StreamParser:
    """逐字节喂入，产出带绝对流偏移的事件。"""

    def __init__(self, base_offset: int = 0):
        self._buf = bytearray()
        self._base = base_offset  # buf[0] 对应的绝对流偏移
        # 已报告坏包覆盖到的绝对偏移：这些字节不再重复计为噪声
        self._suppress_until = -1

    @property
    def offset(self) -> int:
        """当前已消费到的绝对偏移（buf 末尾）。"""
        return self._base + len(self._buf)

    def _consume_noise(self, events: list[dict], n: int) -> None:
        """消耗 buf 前 n 字节噪声；落在已报告坏包范围内的部分不重复上报。"""
        start = self._base
        raw = bytes(self._buf[:n])
        del self._buf[:n]
        self._base += n
        cut = 0
        if start < self._suppress_until:
            cut = min(n, self._suppress_until - start)
        if cut < n:
            events.append({
                "kind": NOISE,
                "offset": start + cut,
                "raw": raw[cut:],
                "detail": f"{n - cut} noise byte(s) before start-of-frame",
            })

    def feed(self, data: bytes) -> list[dict]:
        self._buf.extend(data)
        return self._drain()

    def flush_eof(self) -> list[dict]:
        """连接关闭时调用：残留半包记为截断包，其余残留记为噪声。"""
        events = []
        if self._buf:
            kind = TRUNCATED if self._buf[:2] == protocol.SOF else NOISE
            events.append({
                "kind": kind,
                "offset": self._base,
                "raw": bytes(self._buf),
                "detail": f"connection closed with {len(self._buf)} pending byte(s)",
            })
            self._base += len(self._buf)
            self._buf.clear()
        return events

    def _drain(self) -> list[dict]:
        events: list[dict] = []
        while True:
            idx = self._buf.find(protocol.SOF)
            if idx < 0:
                # 没有起始标记：除最后 1 字节（可能是 0xAA 的一半）外都是噪声
                keep = 1 if self._buf[-1:] == protocol.SOF[:1] else 0
                n = len(self._buf) - keep
                if n > 0:
                    self._consume_noise(events, n)
                break
            if idx > 0:
                self._consume_noise(events, idx)
                continue
            # buf 以 SOF 开头，凑帧
            if len(self._buf) < protocol.FRAME_LEN:
                break  # 分片未到齐，等更多数据
            frame = bytes(self._buf[: protocol.FRAME_LEN])
            if not protocol.frame_eof_ok(frame):
                events.append({
                    "kind": BAD_EOF,
                    "offset": self._base,
                    "raw": frame,
                    "detail": f"end marker mismatch: {frame[10:12].hex()} != {protocol.EOF.hex()}",
                })
                self._suppress_until = self._base + protocol.FRAME_LEN
                del self._buf[:1]  # 前进 1 字节重扫，允许标记重叠
                self._base += 1
                continue
            if not protocol.frame_checksum_ok(frame):
                events.append({
                    "kind": BAD_CHECKSUM,
                    "offset": self._base,
                    "raw": frame,
                    "detail": (
                        f"checksum mismatch: got 0x{frame[9]:02X}, "
                        f"expect 0x{protocol.checksum(frame[2:9]):02X}"
                    ),
                })
                self._suppress_until = self._base + protocol.FRAME_LEN
                del self._buf[:1]
                self._base += 1
                continue
            pkt = protocol.decode_frame(frame)
            pkt.update({"kind": PACKET, "offset": self._base, "raw": frame})
            events.append(pkt)
            del self._buf[: protocol.FRAME_LEN]
            self._base += protocol.FRAME_LEN
        return events

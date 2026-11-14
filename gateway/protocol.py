"""报文编解码：帧布局、校验码。详见 doc/protocol.md。"""
from __future__ import annotations

import struct

SOF = b"\xAA\x55"
EOF = b"\x0D\x0A"
FRAME_LEN = 12  # SOF(2) + chan(1) + seq(2) + value(4) + crc(1) + EOF(2)

MAX_SEQ = 0xFFFF
# 回绕判定窗口：上一序号 >= WRAP_HIGH 且新序号 <= WRAP_LOW 视为自然回绕
WRAP_HIGH = 0xFFF0
WRAP_LOW = 0x000F


def checksum(payload: bytes) -> int:
    """payload 为 通道号+序号+原始值 共 7 字节，返回 1 字节校验码。"""
    return sum(payload) & 0xFF


def encode_frame(channel: int, seq: int, value: int) -> bytes:
    """按文档格式组帧。value 为 int32 原始计数。"""
    payload = struct.pack(">BHi", channel & 0xFF, seq & 0xFFFF, value)
    return SOF + payload + bytes([checksum(payload)]) + EOF


def decode_frame(frame: bytes) -> dict:
    """解出已校验通过的 12 字节帧。调用方需先验 EOF 与校验码。"""
    assert len(frame) == FRAME_LEN
    channel = frame[2]
    seq = struct.unpack(">H", frame[3:5])[0]
    value = struct.unpack(">i", frame[5:9])[0]
    return {"channel": channel, "seq": seq, "value": value}


def frame_eof_ok(frame: bytes) -> bool:
    return frame[10:12] == EOF


def frame_checksum_ok(frame: bytes) -> bool:
    return checksum(frame[2:9]) == frame[9]

"""串口网关模拟器：本地 TCP 模拟真实设备，无需硬件。

- 数据端口（默认 9100）：接受网关连接，持续上送采样报文；
- 控制端口（默认 9101）：行文本命令，触发各种现场情况：
    fragment  下一帧分片发送（多段到达）
    sticky    接下来 6 帧合并成一次写（粘包）
    noise     发送一撮噪声字节（含伪起始标记）
    bad       发送一个校验码错 + 一个结束标记错的坏包
    dup       重发最近 2 帧（重复采样）
    wrap      序号跳到 0xFFFE，几秒后自然回绕
    restart   序号直接归零（模拟设备重启）
    status    打印当前各通道序号
同样可以用 nc 验证：  echo noise | nc 127.0.0.1 9101
"""
from __future__ import annotations

import argparse
import asyncio
import math
import random

from . import protocol


class Device:
    def __init__(self, channels=(0, 1), interval=0.2, seed=20261115):
        self.channels = list(channels)
        self.interval = interval
        self.seq = {ch: 0 for ch in self.channels}
        self.rng = random.Random(seed)
        self.actions: asyncio.Queue[str] = asyncio.Queue()
        self.recent: list[bytes] = []  # 最近发送的帧，供 dup 重发

    def value_for(self, ch: int, seq: int) -> int:
        # 确定性的类信号原始计数
        return int(20000 * math.sin(seq / 8.0)) + 1000 * ch + (seq % 7)

    def next_frame(self, ch: int) -> bytes:
        seq = self.seq[ch]
        frame = protocol.encode_frame(ch, seq, self.value_for(ch, seq))
        self.seq[ch] = (seq + 1) & protocol.MAX_SEQ
        self.recent.append(frame)
        del self.recent[:-8]
        return frame

    def noise_bytes(self) -> bytes:
        n = self.rng.randint(3, 9)
        pool = [0x00, 0xFF, 0x13, 0x37, 0xAA, 0x55, 0xAA, 0x0D]
        return bytes(self.rng.choice(pool) for _ in range(n))


async def _write(writer: asyncio.StreamWriter, data: bytes):
    writer.write(data)
    await writer.drain()


async def sender_loop(dev: Device, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter, peer: str):
    print(f"[sim] gateway connected: {peer}")
    try:
        while True:
            # 消费一个待触发动作（没有则正常发送）
            try:
                action = dev.actions.get_nowait()
            except asyncio.QueueEmpty:
                action = None

            if action == "fragment":
                frame = dev.next_frame(dev.channels[0])
                cut1, cut2 = 3, 7
                for chunk in (frame[:cut1], frame[cut1:cut2], frame[cut2:]):
                    await _write(writer, chunk)
                    await asyncio.sleep(0.05)
                print("[sim] sent fragmented frame")
            elif action == "sticky":
                blob = b"".join(dev.next_frame(ch) for _ in range(3)
                                for ch in dev.channels)
                await _write(writer, blob)
                print(f"[sim] sent sticky blob of {len(blob)} bytes")
            elif action == "noise":
                await _write(writer, dev.noise_bytes())
                print("[sim] sent noise bytes")
            elif action == "bad":
                good = dev.next_frame(dev.channels[0])
                bad_crc = bytearray(good)
                bad_crc[9] = (bad_crc[9] + 1) & 0xFF
                bad_eof = bytearray(dev.next_frame(dev.channels[0]))
                bad_eof[10] = 0x00
                await _write(writer, bytes(bad_crc) + bytes(bad_eof))
                print("[sim] sent bad-checksum and bad-eof frames")
            elif action == "dup":
                if dev.recent:
                    await _write(writer, b"".join(dev.recent[-2:]))
                    print("[sim] re-sent last frames (duplicates)")
            elif action == "wrap":
                for ch in dev.channels:
                    dev.seq[ch] = 0xFFFE
                print("[sim] seq jumped to 0xFFFE, will wrap soon")
            elif action == "restart":
                # 真实设备重启：序号归零并断开连接（网关将重连）
                for ch in dev.channels:
                    dev.seq[ch] = 0
                print("[sim] device restarted, seq reset to 0, dropping connection")
                await _write(writer, b"")  # flush
                writer.close()
                return
            else:
                blob = b"".join(dev.next_frame(ch) for ch in dev.channels)
                await _write(writer, blob)

            await asyncio.sleep(dev.interval)
    except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
        pass
    finally:
        print(f"[sim] gateway disconnected: {peer}")
        try:
            writer.close()
        except Exception:
            pass


async def control_loop(dev: Device, reader: asyncio.StreamReader,
                       writer: asyncio.StreamWriter):
    try:
        while True:
            line = await reader.readline()
            if not line:
                return
            cmd = line.decode().strip().lower()
            known = {"fragment", "sticky", "noise", "bad", "dup", "wrap", "restart"}
            if cmd in known:
                dev.actions.put_nowait(cmd)
                writer.write(f"ok {cmd}\n".encode())
            elif cmd == "status":
                writer.write(f"seq={dev.seq}\n".encode())
            else:
                writer.write(b"unknown command\n")
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def run_simulator(port: int, control_port: int, channels, interval: float):
    dev = Device(channels=channels, interval=interval)
    data_srv = await asyncio.start_server(
        lambda r, w: sender_loop(dev, r, w, f"{w.get_extra_info('peername')}"),
        "127.0.0.1", port)
    ctrl_srv = await asyncio.start_server(
        lambda r, w: control_loop(dev, r, w), "127.0.0.1", control_port)
    print(f"[sim] data port {port}, control port {control_port}, "
          f"channels={list(channels)}, interval={interval}s")
    async with data_srv, ctrl_srv:
        await asyncio.gather(data_srv.serve_forever(), ctrl_srv.serve_forever())


def main(argv=None):
    ap = argparse.ArgumentParser(description="串口网关模拟器（本地 TCP）")
    ap.add_argument("--port", type=int, default=9100, help="数据端口")
    ap.add_argument("--control", type=int, default=9101, help="控制端口")
    ap.add_argument("--channels", type=int, nargs="+", default=[0, 1])
    ap.add_argument("--interval", type=float, default=0.2, help="发送间隔秒")
    args = ap.parse_args(argv)
    try:
        asyncio.run(run_simulator(args.port, args.control, args.channels, args.interval))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

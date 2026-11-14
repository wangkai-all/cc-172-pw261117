"""网关主程序：TCP 接入模拟器 → 解包 → 入库，并提供网页与 API。

- 暂停/恢复：暂停时字节积压在本地缓冲不解析，恢复后继续——
  用来验证「同一段缓冲数据不因处理时刻不同而套用不同配置」；
- 实时推送：SSE（/api/stream），事件类型 sample/error/event/config/state/stats；
- 状态持久化：样本、配置版本、通道游标、字节流偏移都在 SQLite，重启保留。
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import os
import queue
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .parser import StreamParser
from .pipeline import Pipeline
from .store import Store

STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")


class Hub:
    """线程安全的订阅广播：asyncio 线程发布，HTTP 线程消费。"""

    def __init__(self):
        self._subs: list[queue.Queue] = []
        self._lock = threading.Lock()

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=2000)
        with self._lock:
            self._subs.append(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._lock:
            if q in self._subs:
                self._subs.remove(q)

    def publish(self, etype: str, payload) -> None:
        msg = {"type": etype, "data": payload}
        with self._lock:
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(msg)
            except queue.Full:
                pass


class GatewayApp:
    def __init__(self, sim_host="127.0.0.1", sim_port=9100, control_port=9101,
                 http_port=8080, db_path="data/gateway.db", channels=(0, 1)):
        self.sim_addr = (sim_host, sim_port)
        self.control_addr = (sim_host, control_port)
        self.http_port = http_port
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.store = Store(db_path, channels=channels)
        self.hub = Hub()
        self.pipeline = Pipeline(self.store, broadcast=self.hub.publish)
        self.paused = False
        self.connected = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._httpd: ThreadingHTTPServer | None = None
        self._http_thread: threading.Thread | None = None
        self._stop_evt = asyncio.Event()

    # ------------------------------------------------------------------
    # asyncio 侧：TCP 接入
    # ------------------------------------------------------------------
    async def _async_main(self):
        while not self._stop_evt.is_set():
            try:
                reader, writer = await asyncio.open_connection(*self.sim_addr)
            except OSError:
                await asyncio.sleep(1.0)
                continue
            self.connected = True
            self.pipeline.on_connect()
            self._log_event("connect", None, f"connected to simulator {self.sim_addr}")
            self._publish_state()
            conn = asyncio.create_task(self._conn_loop(reader))
            stopper = asyncio.create_task(self._stop_evt.wait())
            try:
                await asyncio.wait([conn, stopper],
                                   return_when=asyncio.FIRST_COMPLETED)
            finally:
                stopper.cancel()
                if not conn.done():
                    conn.cancel()
                    try:
                        await conn
                    except asyncio.CancelledError:
                        pass
                self.connected = False
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
                self._log_event("disconnect", None, "simulator connection closed")
                self._publish_state()
            if not self._stop_evt.is_set():
                await asyncio.sleep(1.0)

    async def _conn_loop(self, reader: asyncio.StreamReader):
        base = self.store.get_meta("stream_offset", 0)
        parser = StreamParser(base_offset=base)
        held = bytearray()
        try:
            while not self._stop_evt.is_set():
                data = await reader.read(4096)
                if not data:
                    break
                held.extend(data)
                if self.paused:
                    continue  # 暂停：字节积压在缓冲里，不解析
                self._process(parser, held)
                held.clear()
        finally:
            if held:  # 连接收尾，剩余字节必须落库
                self._process(parser, held)
                held.clear()
            self.pipeline.handle_events(parser.flush_eof())
            self.store.set_meta("stream_offset", parser.offset)

    def _process(self, parser: StreamParser, data: bytearray):
        events = parser.feed(bytes(data))
        self.pipeline.handle_events(events)
        self.store.set_meta("stream_offset", parser.offset)
        self.hub.publish("stats", self.store.stats())

    def _log_event(self, kind, channel, detail):
        row = self.store.insert_event(kind, channel, detail)
        self.hub.publish("event", row)

    def _publish_state(self):
        self.hub.publish("state", self.status())

    # ------------------------------------------------------------------
    # 控制（HTTP 线程调用）
    # ------------------------------------------------------------------
    def pause(self):
        if self._loop:
            self._loop.call_soon_threadsafe(self._set_paused, True)

    def resume(self):
        if self._loop:
            self._loop.call_soon_threadsafe(self._set_paused, False)

    def _set_paused(self, value: bool):
        if self.paused == value:
            return
        self.paused = value
        self._log_event("pause" if value else "resume", None,
                        "ingestion paused; bytes buffered" if value
                        else "ingestion resumed; buffer drained")
        self._publish_state()

    def status(self) -> dict:
        return {
            "connected": self.connected,
            "paused": self.paused,
            "simulator": f"{self.sim_addr[0]}:{self.sim_addr[1]}",
            "stream_offset": self.store.get_meta("stream_offset", 0),
        }

    def trigger_simulator(self, cmd: str) -> str:
        """把触发命令转发给模拟器控制端口。"""
        with socket.create_connection(self.control_addr, timeout=3) as s:
            s.sendall((cmd.strip() + "\n").encode())
            return s.makefile().readline().strip()

    def add_config(self, channel: int, scale: float, unit: str,
                   effective_seq: int, effective_epoch: int | None = None) -> dict:
        if effective_epoch is None:
            st = self.store.get_channel_state(channel)
            effective_epoch = st["epoch"] if st else 0
        cfg = self.store.add_config(channel=channel, scale=scale, unit=unit,
                                    effective_epoch=effective_epoch,
                                    effective_seq=effective_seq)
        self._log_event("config", channel,
                        f"config v{cfg['id']}: scale={scale} unit={unit} "
                        f"effective from epoch={effective_epoch} seq={effective_seq}")
        self.hub.publish("config", cfg)
        return cfg

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def start(self):
        self._loop = asyncio.new_event_loop()

        def run_loop():
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._async_main())

        self._loop_thread = threading.Thread(target=run_loop, daemon=True, name="gw-loop")
        self._loop_thread.start()

        app = self

        class Handler(GatewayHTTPHandler):
            gateway = app

        self._httpd = ThreadingHTTPServer(("127.0.0.1", self.http_port), Handler)
        self._httpd.daemon_threads = True
        self._http_thread = threading.Thread(
            target=self._httpd.serve_forever, daemon=True, name="gw-http")
        self._http_thread.start()

    def stop(self):
        if self._loop and not self._loop.is_closed():
            try:
                self._loop.call_soon_threadsafe(self._stop_evt.set)
            except RuntimeError:
                pass
        if self._loop_thread:
            self._loop_thread.join(timeout=5)
        if self._loop:
            self._loop.close()
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()
        if self._http_thread:
            self._http_thread.join(timeout=5)
        self.store.close()

    def run_forever(self):
        self.start()
        print(f"[gw] web UI:  http://127.0.0.1:{self.http_port}")
        print(f"[gw] simulator: {self.sim_addr[0]}:{self.sim_addr[1]} "
              f"(control {self.control_addr[1]})")
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()


# ----------------------------------------------------------------------
# HTTP 层
# ----------------------------------------------------------------------
class GatewayHTTPHandler(BaseHTTPRequestHandler):
    gateway: GatewayApp = None  # 由 start() 注入
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # 静音
        pass

    # ---------- 工具 ----------
    def _send(self, body: bytes, ctype="application/json; charset=utf-8",
              status=200, extra=None):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, status=200):
        self._send(json.dumps(obj, ensure_ascii=False).encode(), status=status)

    def _read_body(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        return json.loads(self.rfile.read(n).decode())

    # ---------- GET ----------
    def do_GET(self):
        path, _, qs = self.path.partition("?")
        gw = self.gateway
        if path == "/":
            self._serve_static("index.html", "text/html; charset=utf-8")
        elif path.startswith("/static/"):
            name = os.path.basename(path)
            ctype = {"js": "application/javascript", "css": "text/css",
                     "html": "text/html"}.get(name.rsplit(".", 1)[-1], "text/plain")
            self._serve_static(name, ctype + "; charset=utf-8")
        elif path == "/api/state":
            self._json({
                "status": gw.status(),
                "stats": gw.store.stats(),
                "channels": gw.store.all_channel_states(),
                "configs": gw.store.list_configs(),
                "samples": gw.store.recent_samples(100),
                "errors": gw.store.recent_errors(50),
                "events": gw.store.recent_events(50),
            })
        elif path == "/api/stream":
            self._sse()
        elif path == "/api/export.csv":
            self._export_csv()
        elif path == "/api/export.json":
            self._json({
                "exported_at": time.time(),
                "samples": gw.store.iter_samples(),
                "errors": gw.store.iter_errors(),
                "configs": gw.store.list_configs(),
                "events": gw.store.iter_events(),
                "channel_states": gw.store.all_channel_states(),
            })
        else:
            self._json({"error": "not found"}, status=404)

    def _serve_static(self, name: str, ctype: str):
        fp = os.path.join(STATIC_DIR, name)
        if not os.path.isfile(fp):
            self._json({"error": "not found"}, status=404)
            return
        with open(fp, "rb") as f:
            self._send(f.read(), ctype)

    def _sse(self):
        q = self.gateway.hub.subscribe()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
            while True:
                try:
                    msg = q.get(timeout=15)
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    continue
                body = (f"event: {msg['type']}\n"
                        f"data: {json.dumps(msg['data'], ensure_ascii=False)}\n\n")
                self.wfile.write(body.encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            self.gateway.hub.unsubscribe(q)

    def _export_csv(self):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["id", "ts", "channel", "epoch", "seq", "raw_hex", "raw_value",
                    "scale", "unit", "scaled", "config_version", "stream_offset", "flags"])
        for s in self.gateway.store.iter_samples():
            w.writerow([s["id"], f"{s['ts']:.3f}", s["channel"], s["epoch"], s["seq"],
                        s["raw_hex"], s["raw_value"], s["scale"], s["unit"],
                        f"{s['scaled']:.6g}", s["config_version"],
                        s["stream_offset"], s["flags"]])
        self._send(buf.getvalue().encode(), "text/csv; charset=utf-8",
                   extra={"Content-Disposition": "attachment; filename=samples.csv"})

    # ---------- POST ----------
    def do_POST(self):
        gw = self.gateway
        try:
            body = self._read_body()
        except Exception:
            self._json({"error": "invalid json"}, status=400)
            return
        if self.path == "/api/control":
            action = body.get("action")
            if action == "pause":
                gw.pause()
            elif action == "resume":
                gw.resume()
            else:
                self._json({"error": "action must be pause|resume"}, status=400)
                return
            self._json({"ok": True, "paused": gw.paused})
        elif self.path == "/api/config":
            try:
                cfg = gw.add_config(
                    channel=int(body["channel"]),
                    scale=float(body["scale"]),
                    unit=str(body["unit"]).strip() or "counts",
                    effective_seq=int(body["effective_seq"]),
                    effective_epoch=(None if body.get("effective_epoch") in (None, "")
                                     else int(body["effective_epoch"])))
            except (KeyError, ValueError) as e:
                self._json({"error": f"bad config: {e}"}, status=400)
                return
            self._json({"ok": True, "config": cfg})
        elif self.path == "/api/simulate":
            cmd = str(body.get("cmd", "")).strip()
            try:
                reply = gw.trigger_simulator(cmd)
            except OSError as e:
                self._json({"error": f"simulator control unreachable: {e}"}, status=502)
                return
            self._json({"ok": True, "reply": reply})
        else:
            self._json({"error": "not found"}, status=404)


def main(argv=None):
    ap = argparse.ArgumentParser(description="串口采样网关（网页接入）")
    ap.add_argument("--sim-host", default="127.0.0.1")
    ap.add_argument("--sim-port", type=int, default=9100)
    ap.add_argument("--control-port", type=int, default=9101)
    ap.add_argument("--http-port", type=int, default=8080)
    ap.add_argument("--db", default="data/gateway.db")
    ap.add_argument("--channels", type=int, nargs="+", default=[0, 1])
    args = ap.parse_args(argv)
    GatewayApp(sim_host=args.sim_host, sim_port=args.sim_port,
               control_port=args.control_port, http_port=args.http_port,
               db_path=args.db, channels=tuple(args.channels)).run_forever()


if __name__ == "__main__":
    main()

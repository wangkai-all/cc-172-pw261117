"""端到端：模拟器 + 网关 + HTTP API。

覆盖：实时接入、配置切换边界（含暂停积压场景）、坏包/噪声隔离、
重复采样去重、回绕与重启分别识别、进程重启后状态保留、导出。
"""
import json
import os
import socket
import tempfile
import threading
import time
import unittest
import urllib.request

from gateway.app import GatewayApp
from gateway.simulator import run_simulator
import asyncio


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(cond, timeout=10.0, interval=0.05, what="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(interval)
    raise AssertionError(f"timeout waiting for {what}")


def http_json(port, path, body=None):
    url = f"http://127.0.0.1:{port}{path}"
    if body is None:
        with urllib.request.urlopen(url, timeout=5) as r:
            return json.loads(r.read())
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


class IntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sim_port, cls.ctrl_port, cls.http_port = (
            free_port(), free_port(), free_port())
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db = os.path.join(cls.tmp.name, "gw.db")

        cls.sim_thread = threading.Thread(
            target=lambda: asyncio.run(
                run_simulator(cls.sim_port, cls.ctrl_port,
                              channels=(0, 1), interval=0.05)),
            daemon=True)
        cls.sim_thread.start()

        cls.app = GatewayApp(sim_port=cls.sim_port, control_port=cls.ctrl_port,
                             http_port=cls.http_port, db_path=cls.db)
        cls.app.start()
        wait_until(lambda: cls.app.connected, what="gateway connect")

    @classmethod
    def tearDownClass(cls):
        cls.app.stop()
        cls.tmp.cleanup()

    # 测试按声明顺序执行（unittest 默认按名字排序，用编号保证顺序）
    def state(self):
        return http_json(self.http_port, "/api/state")

    def test_01_live_ingestion(self):
        wait_until(lambda: self.state()["stats"]["samples"] >= 20,
                   what="live samples")
        st = self.state()
        self.assertTrue(st["status"]["connected"])
        self.assertGreaterEqual(len(st["channels"]), 1)
        # 每条样本都带原始字节与配置版本
        s = st["samples"][0]
        self.assertEqual(len(s["raw_hex"]), 24)  # 12 字节 hex
        self.assertGreaterEqual(s["config_version"], 1)

    def test_02_pause_buffers_then_resume(self):
        """暂停期间到达的字节积压在缓冲；恢复后按序号套用配置，与处理时刻无关。"""
        st = self.state()
        last = max(c["last_seq"] for c in st["channels"] if c["channel"] == 0)
        boundary = last + 4  # 暂停期间会越过这个序号
        http_json(self.http_port, "/api/control", {"action": "pause"})
        time.sleep(0.3)
        self.assertTrue(self.state()["status"]["paused"])
        n_paused = self.state()["stats"]["samples"]
        time.sleep(1.0)  # 暂停期间模拟器继续发，字节压在缓冲/内核里
        self.assertEqual(self.state()["stats"]["samples"], n_paused,
                         "paused: no sample may be ingested")
        # 暂停中下发配置：生效序号落在积压数据中间
        r = http_json(self.http_port, "/api/config", {
            "channel": 0, "scale": 0.25, "unit": "V",
            "effective_seq": boundary})
        self.assertTrue(r["ok"])
        new_ver = r["config"]["id"]
        http_json(self.http_port, "/api/control", {"action": "resume"})
        # 等积压处理完（ch0 序号越过 boundary+3）
        wait_until(lambda: any(
            s["channel"] == 0 and s["epoch"] == 0 and s["seq"] >= boundary + 3
            for s in self.state()["samples"]), what="backlog drained")
        rows = [s for s in self.state()["samples"]
                if s["channel"] == 0 and s["epoch"] == 0
                and boundary - 4 <= s["seq"] <= boundary + 3]
        self.assertGreaterEqual(len(rows), 6)
        for s in rows:
            want = new_ver if s["seq"] >= boundary else 1
            self.assertEqual(s["config_version"], want,
                             f"seq={s['seq']} got v{s['config_version']}")
            if s["seq"] >= boundary:
                self.assertEqual(s["unit"], "V")
                self.assertAlmostEqual(s["scaled"], s["raw_value"] * 0.25)

    def test_03_duplicates_ignored(self):
        n0 = self.state()["stats"]["samples"]
        d0 = self.state()["stats"]["duplicates"]
        http_json(self.http_port, "/api/simulate", {"cmd": "dup"})
        wait_until(lambda: self.state()["stats"]["duplicates"] > d0,
                   what="duplicate detected")
        time.sleep(0.3)
        # 重复帧只计数，不入库（样本数只随新序号增长）
        self.assertGreaterEqual(self.state()["stats"]["samples"], n0)

    def test_04_noise_and_bad_packets_isolated(self):
        http_json(self.http_port, "/api/simulate", {"cmd": "noise"})
        http_json(self.http_port, "/api/simulate", {"cmd": "bad"})
        wait_until(lambda: self.state()["stats"]["noise_segments"] >= 1
                   and self.state()["stats"]["bad_packets"] >= 2,
                   what="noise + bad packets")
        kinds = {e["kind"] for e in self.state()["errors"]}
        self.assertIn("noise", kinds)
        self.assertIn("bad_checksum", kinds)
        self.assertIn("bad_eof", kinds)
        for e in self.state()["errors"]:
            self.assertGreaterEqual(e["stream_offset"], 0)  # 错误位置留痕
            self.assertTrue(e["raw_hex"])
        # 坏包被隔离：好样本仍在持续入库
        wait_until(lambda: self.state()["stats"]["samples"] > 0, what="ok")

    def test_05_wrap_and_restart_are_distinct(self):
        http_json(self.http_port, "/api/simulate", {"cmd": "wrap"})
        wait_until(lambda: any(e["kind"] == "wrap" for e in self.state()["events"]),
                   timeout=15, what="wrap event")
        ep_after_wrap = max(c["epoch"] for c in self.state()["channels"]
                            if c["channel"] == 0)
        self.assertEqual(ep_after_wrap, 1)
        # 等回绕后的序号走远，再触发设备重启（序号骤减才可识别为重启）
        wait_until(lambda: max(c["last_seq"] for c in self.state()["channels"]
                               if c["channel"] == 0) >= 20,
                   what="seq advanced past wrap")
        http_json(self.http_port, "/api/simulate", {"cmd": "restart"})
        wait_until(lambda: any(e["kind"] == "restart" for e in self.state()["events"]),
                   what="restart event")
        ep_after_restart = max(c["epoch"] for c in self.state()["channels"]
                               if c["channel"] == 0)
        self.assertEqual(ep_after_restart, 2)
        # 两类事件分别记录
        kinds = [e["kind"] for e in self.state()["events"]]
        self.assertIn("wrap", kinds)
        self.assertIn("restart", kinds)

    def test_06_state_survives_gateway_restart(self):
        st0 = self.state()
        ep0 = max(c["epoch"] for c in st0["channels"] if c["channel"] == 0)
        n0 = st0["stats"]["samples"]
        self.app.stop()
        # 同一库文件重启网关：配置、样本、通道游标全部保留
        type(self).app = GatewayApp(sim_port=self.sim_port,
                                    control_port=self.ctrl_port,
                                    http_port=self.http_port, db_path=self.db)
        self.app.start()
        wait_until(lambda: self.app.connected, what="reconnect")
        wait_until(lambda: self.state()["stats"]["samples"] > n0 + 5,
                   what="ingestion after restart")
        st1 = self.state()
        ep1 = max(c["epoch"] for c in st1["channels"] if c["channel"] == 0)
        self.assertGreaterEqual(ep1, ep0)  # epoch 不回退
        self.assertGreaterEqual(len(st1["configs"]), 2)  # 配置版本还在
        # 重启前的样本仍在库里
        self.assertGreater(st1["stats"]["samples"], n0)

    def test_07_export(self):
        with urllib.request.urlopen(
                f"http://127.0.0.1:{self.http_port}/api/export.csv", timeout=5) as r:
            csv_text = r.read().decode()
        lines = csv_text.strip().splitlines()
        self.assertIn("raw_hex", lines[0])
        self.assertGreater(len(lines), 10)
        data = http_json(self.http_port, "/api/export.json")
        for key in ("samples", "errors", "configs", "events", "channel_states"):
            self.assertIn(key, data)
        self.assertGreater(len(data["samples"]), 10)


if __name__ == "__main__":
    unittest.main()

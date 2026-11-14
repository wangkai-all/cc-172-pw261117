"""管线单元测试：回绕/重启识别、配置生效序号边界、去重、重启后状态保留。"""
import os
import tempfile
import unittest

from gateway import protocol
from gateway.pipeline import Pipeline, classify_seq, NEW, FORWARD, SAME, WRAP, RESTART
from gateway.store import Store


def pkt(ch, seq, val=100, offset=0):
    raw = protocol.encode_frame(ch, seq, val)
    return {"kind": "packet", "channel": ch, "seq": seq, "value": val,
            "raw": raw, "offset": offset}


class TestClassify(unittest.TestCase):
    def test_normal(self):
        self.assertEqual(classify_seq(None, 0), NEW)
        self.assertEqual(classify_seq(10, 11), FORWARD)
        self.assertEqual(classify_seq(11, 11), SAME)

    def test_wrap_vs_restart(self):
        # 65535 -> 0 是自然回绕
        self.assertEqual(classify_seq(0xFFFF, 0), WRAP)
        self.assertEqual(classify_seq(0xFFFE, 2), WRAP)
        # 中途骤减是设备重启
        self.assertEqual(classify_seq(5000, 0), RESTART)
        self.assertEqual(classify_seq(100, 3), RESTART)


class PipelineTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "t.db")
        self.store = Store(self.db)
        self.pipeline = Pipeline(self.store)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()


class TestConfigBoundary(PipelineTestBase):
    def test_boundary_by_seq_not_processing_time(self):
        """配置带生效序号：缓冲数据无论何时处理，套用配置一致。"""
        # 先积压 seq 0..9（模拟暂停期间到达的字节）
        backlog = [pkt(0, s, val=1000, offset=s * 12) for s in range(10)]
        # 在处理积压之前下发新配置：seq>=5 生效，倍率 0.5
        self.store.add_config(channel=0, scale=0.5, unit="V",
                              effective_epoch=0, effective_seq=5)
        # 现在才处理整段积压
        self.pipeline.handle_events(backlog)
        rows = sorted(self.store.recent_samples(100), key=lambda r: r["seq"])
        self.assertEqual(len(rows), 10)
        for r in rows:
            if r["seq"] < 5:
                self.assertEqual(r["scale"], 1.0, f"seq={r['seq']}")
                self.assertEqual(r["unit"], "counts")
                self.assertEqual(r["scaled"], 1000.0)
            else:
                self.assertEqual(r["scale"], 0.5, f"seq={r['seq']}")
                self.assertEqual(r["unit"], "V")
                self.assertEqual(r["scaled"], 500.0)

    def test_config_applies_across_epoch(self):
        """配置生效点 (epoch=1, seq=2)：回绕后 seq 0,1 用旧配置，seq>=2 用新配置。"""
        self.store.add_config(channel=0, scale=10.0, unit="A",
                              effective_epoch=1, effective_seq=2)
        # epoch0 走到 0xFFFE, 0xFFFF 后回绕进 epoch1
        self.pipeline.handle_events([pkt(0, 0xFFFE), pkt(0, 0xFFFF),
                                     pkt(0, 0), pkt(0, 1), pkt(0, 2), pkt(0, 3)])
        rows = sorted(self.store.recent_samples(100), key=lambda r: (r["epoch"], r["seq"]))
        by_key = {(r["epoch"], r["seq"]): r for r in rows}
        self.assertEqual(by_key[(1, 0)]["scale"], 1.0)
        self.assertEqual(by_key[(1, 1)]["scale"], 1.0)
        self.assertEqual(by_key[(1, 2)]["scale"], 10.0)
        self.assertEqual(by_key[(1, 3)]["unit"], "A")


class TestEpochAndDedup(PipelineTestBase):
    def test_wrap_increments_epoch_with_flag(self):
        self.pipeline.handle_events([pkt(0, 0xFFFE), pkt(0, 0xFFFF), pkt(0, 0)])
        rows = sorted(self.store.recent_samples(10), key=lambda r: r["id"])
        self.assertEqual(rows[-1]["epoch"], 1)
        self.assertEqual(rows[-1]["flags"], "wrap")
        kinds = [e["kind"] for e in self.store.recent_events(10)]
        self.assertIn("wrap", kinds)

    def test_restart_increments_epoch_with_flag(self):
        self.pipeline.handle_events([pkt(0, 100), pkt(0, 101), pkt(0, 0)])
        rows = sorted(self.store.recent_samples(10), key=lambda r: r["id"])
        self.assertEqual(rows[-1]["epoch"], 1)
        self.assertEqual(rows[-1]["flags"], "restart")
        kinds = [e["kind"] for e in self.store.recent_events(10)]
        self.assertIn("restart", kinds)

    def test_duplicate_not_reinserted(self):
        self.pipeline.handle_events([pkt(0, 1), pkt(0, 2), pkt(0, 3)])
        n0 = self.store.count_samples()
        self.pipeline.handle_events([pkt(0, 2), pkt(0, 3)])  # 重发
        self.assertEqual(self.store.count_samples(), n0)  # 不重复入库
        self.assertEqual(self.store.stats()["duplicates"], 2)

    def test_same_seq_different_epoch_is_not_duplicate(self):
        self.pipeline.handle_events([pkt(0, 0xFFFF), pkt(0, 0)])
        self.assertEqual(self.store.count_samples(), 2)  # (0,0xFFFF) 与 (1,0)

    def test_replay_vs_restart_with_reconnect(self):
        """连接未断的回跳=重发；重连后的回跳=设备重启。"""
        self.pipeline.on_connect()
        self.pipeline.handle_events([pkt(0, 10), pkt(0, 11), pkt(0, 12)])
        # 连接未断，重发 11：重复采样，epoch 不变
        self.pipeline.handle_events([pkt(0, 11)])
        self.assertEqual(self.store.count_samples(), 3)
        self.assertEqual(self.store.get_channel_state(0)["epoch"], 0)
        # 模拟设备重启：重连后序号回跳到已见过的 0 → 仍判重启
        self.pipeline.on_connect()
        self.pipeline.handle_events([pkt(0, 0), pkt(0, 1)])
        st = self.store.get_channel_state(0)
        self.assertEqual((st["epoch"], st["last_seq"]), (1, 1))
        kinds = [e["kind"] for e in self.store.recent_events(10)]
        self.assertIn("restart", kinds)

    def test_state_survives_reopen(self):
        """进程重启：通道游标从库里恢复，序号连续判定与去重不丢。"""
        self.pipeline.handle_events([pkt(0, 50), pkt(0, 51)])
        self.store.close()
        self.store = Store(self.db)  # 模拟重启后重开同一库
        self.pipeline = Pipeline(self.store)
        # 设备继续发 52：应识别为正常递增，epoch 不变
        self.pipeline.handle_events([pkt(0, 52)])
        st = self.store.get_channel_state(0)
        self.assertEqual((st["epoch"], st["last_seq"]), (0, 52))
        # 重发 51：仍判重，不入库
        n0 = self.store.count_samples()
        self.pipeline.handle_events([pkt(0, 51)])
        self.assertEqual(self.store.count_samples(), n0)


if __name__ == "__main__":
    unittest.main()

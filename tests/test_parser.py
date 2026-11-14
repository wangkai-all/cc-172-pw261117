"""分帧器单元测试：分片、粘包、噪声、坏包、截断、流偏移。"""
import unittest

from gateway import protocol
from gateway.parser import (
    StreamParser, PACKET, NOISE, BAD_EOF, BAD_CHECKSUM, TRUNCATED,
)


def frame(ch=0, seq=0, val=0):
    return protocol.encode_frame(ch, seq, val)


class TestParser(unittest.TestCase):
    def test_single_frame(self):
        p = StreamParser()
        evs = p.feed(frame(1, 42, -1234))
        self.assertEqual(len(evs), 1)
        self.assertEqual(evs[0]["kind"], PACKET)
        self.assertEqual(evs[0]["channel"], 1)
        self.assertEqual(evs[0]["seq"], 42)
        self.assertEqual(evs[0]["value"], -1234)
        self.assertEqual(evs[0]["offset"], 0)
        self.assertEqual(evs[0]["raw"], frame(1, 42, -1234))

    def test_fragmented_delivery(self):
        """一帧拆成 1 字节一段喂入，结果必须和一次性喂入相同。"""
        p = StreamParser()
        data = frame(0, 7, 999)
        evs = []
        for i in range(len(data)):
            evs += p.feed(data[i:i + 1])
        self.assertEqual(len(evs), 1)
        self.assertEqual(evs[0]["seq"], 7)
        self.assertEqual(evs[0]["value"], 999)

    def test_sticky_packets(self):
        """多帧粘在一次 TCP 读里，逐帧解出且偏移递增。"""
        p = StreamParser()
        blob = b"".join(frame(0, s, s * 10) for s in range(5))
        evs = p.feed(blob)
        self.assertEqual([e["seq"] for e in evs], [0, 1, 2, 3, 4])
        self.assertEqual([e["offset"] for e in evs],
                         [0, 12, 24, 36, 48])

    def test_noise_before_frame(self):
        p = StreamParser()
        evs = p.feed(b"\x00\xff\x13" + frame(0, 1, 5))
        self.assertEqual(evs[0]["kind"], NOISE)
        self.assertEqual(evs[0]["offset"], 0)
        self.assertEqual(evs[0]["raw"], b"\x00\xff\x13")
        self.assertEqual(evs[1]["kind"], PACKET)
        self.assertEqual(evs[1]["offset"], 3)

    def test_noise_with_partial_sof(self):
        """噪声里夹一个 0xAA（半个起始标记），不能吞掉后续真帧。"""
        p = StreamParser()
        evs = p.feed(b"\x11\xaa" + frame(0, 9, 1))
        kinds = [e["kind"] for e in evs]
        self.assertEqual(kinds, [NOISE, PACKET])
        self.assertEqual(evs[0]["raw"], b"\x11\xaa")

    def test_bad_checksum_isolated(self):
        p = StreamParser()
        bad = bytearray(frame(0, 3, 100))
        bad[9] ^= 0xFF  # 破坏校验码
        evs = p.feed(bytes(bad) + frame(0, 4, 200))
        self.assertEqual(evs[0]["kind"], BAD_CHECKSUM)
        self.assertEqual(evs[0]["offset"], 0)
        self.assertEqual(evs[0]["raw"], bytes(bad))  # 原始字节留痕
        packets = [e for e in evs if e["kind"] == PACKET]
        self.assertEqual(len(packets), 1)
        self.assertEqual(packets[0]["seq"], 4)  # 后续好帧不受影响

    def test_bad_eof_isolated(self):
        p = StreamParser()
        bad = bytearray(frame(0, 5, 1))
        bad[10] = 0x00
        evs = p.feed(bytes(bad))
        self.assertEqual(evs[0]["kind"], BAD_EOF)
        self.assertIn("end marker", evs[0]["detail"])

    def test_overlapping_sof_resync(self):
        """坏帧内部含 AA 55，重扫后应识别为真帧起点。"""
        p = StreamParser()
        good = frame(0, 8, 8)
        junk = protocol.SOF + b"\x01\x02" + good  # 假 SOF 后紧跟真帧
        evs = p.feed(junk)
        packets = [e for e in evs if e["kind"] == PACKET]
        self.assertEqual(len(packets), 1)
        self.assertEqual(packets[0]["seq"], 8)

    def test_truncated_on_close(self):
        p = StreamParser()
        p.feed(frame(0, 1, 1))
        p.feed(frame(0, 2, 2)[:5])  # 半包
        evs = p.flush_eof()
        self.assertEqual(evs[0]["kind"], TRUNCATED)
        self.assertEqual(evs[0]["offset"], 12)
        self.assertEqual(len(evs[0]["raw"]), 5)

    def test_absolute_offset_across_feeds(self):
        p = StreamParser(base_offset=1000)
        evs = p.feed(b"\x99" + frame(0, 0, 0))
        self.assertEqual(evs[0]["offset"], 1000)
        self.assertEqual(evs[1]["offset"], 1001)
        self.assertEqual(p.offset, 1001 + 12)


if __name__ == "__main__":
    unittest.main()

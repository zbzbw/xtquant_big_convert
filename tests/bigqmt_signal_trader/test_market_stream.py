from __future__ import annotations

import unittest

from bigqmt_signal_trader.market_stream import MarketStreamBuffer
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers


class _UnusedProvider:
    pass


class MarketStreamBufferTests(unittest.TestCase):
    def test_drain_preserves_callback_batch_order(self):
        stream = MarketStreamBuffer(max_batches=4, max_records=10)
        stream.append({"600000.SH": {"time": 1}}, received_at_ns=10)
        stream.append(
            {
                "000001.SZ": {"time": 2},
                "600000.SH": {"time": 2},
            },
            received_at_ns=20,
        )

        result = stream.drain(after_sequence=0, max_batches=10, max_records=10)

        self.assertFalse(result["gap"])
        self.assertFalse(result["has_more"])
        self.assertEqual(result["next_sequence"], 2)
        self.assertEqual(result["returned_records"], 3)
        self.assertEqual(
            [batch["sequence"] for batch in result["batches"]],
            [1, 2],
        )

    def test_overflow_is_visible_as_sequence_gap(self):
        stream = MarketStreamBuffer(max_batches=2, max_records=2)
        stream.append({"600000.SH": {"time": 1}})
        stream.append({"600001.SH": {"time": 2}})
        stream.append({"600002.SH": {"time": 3}})

        result = stream.drain(after_sequence=0)

        self.assertTrue(result["gap"])
        self.assertEqual(result["earliest_sequence"], 2)
        self.assertEqual(result["dropped_batches"], 1)
        self.assertEqual(result["dropped_records"], 1)

    def test_record_limit_returns_at_least_one_complete_batch(self):
        stream = MarketStreamBuffer(max_batches=4, max_records=10)
        stream.append(
            {
                "600000.SH": {"time": 1},
                "600001.SH": {"time": 1},
            }
        )
        stream.append({"600002.SH": {"time": 2}})

        first = stream.drain(after_sequence=0, max_records=1)
        second = stream.drain(
            after_sequence=first["next_sequence"],
            max_records=1,
        )

        self.assertEqual(first["returned_batches"], 1)
        self.assertEqual(first["returned_records"], 2)
        self.assertTrue(first["has_more"])
        self.assertEqual(second["next_sequence"], 2)

    def test_large_callback_is_chunked_without_losing_bootstrap_identity(self):
        stream = MarketStreamBuffer(
            max_batches=10,
            max_records=10,
            batch_max_records=2,
        )

        latest = stream.append(
            {
                "600000.SH": {"time": 1},
                "600001.SH": {"time": 1},
                "600002.SH": {"time": 1},
                "600003.SH": {"time": 1},
                "600004.SH": {"time": 1},
            },
            received_at_ns=10,
            is_bootstrap=True,
        )
        first = stream.drain(after_sequence=0, max_records=2)
        second = stream.drain(after_sequence=first["next_sequence"], max_records=2)
        third = stream.drain(after_sequence=second["next_sequence"], max_records=2)

        self.assertEqual(latest, 3)
        self.assertEqual(first["schema_version"], 2)
        self.assertEqual(first["batch_max_records"], 2)
        self.assertEqual(first["latest_callback_sequence"], 1)
        self.assertEqual(first["returned_records"], 2)
        self.assertTrue(first["has_more"])
        self.assertEqual(second["returned_records"], 2)
        self.assertEqual(third["returned_records"], 1)
        batches = first["batches"] + second["batches"] + third["batches"]
        self.assertEqual([batch["sequence"] for batch in batches], [1, 2, 3])
        self.assertEqual(
            [batch["callback_sequence"] for batch in batches],
            [1, 1, 1],
        )
        self.assertEqual([batch["callback_part"] for batch in batches], [1, 2, 3])
        self.assertTrue(all(batch["is_bootstrap"] for batch in batches))


class MarketStreamRpcTests(unittest.TestCase):
    def test_rpc_exposes_status_and_cursor_drain(self):
        stream = MarketStreamBuffer()
        stream.append({"600000.SH": {"time": 1}})
        handlers = BigQmtRpcHandlers(
            account_id="acct",
            market_data=_UnusedProvider(),
            position_provider=_UnusedProvider(),
            market_stream=stream,
        )

        status = handlers.handle("get_market_stream_status")
        drained = handlers.handle(
            "drain_market_stream",
            {
                "stream_id": stream.stream_id,
                "after_sequence": 0,
            },
        )

        self.assertEqual(status["stream_id"], stream.stream_id)
        self.assertEqual(drained["next_sequence"], 1)

    def test_rpc_rejects_stale_stream_identity(self):
        stream = MarketStreamBuffer()
        handlers = BigQmtRpcHandlers(
            account_id="acct",
            market_data=_UnusedProvider(),
            position_provider=_UnusedProvider(),
            market_stream=stream,
        )

        with self.assertRaisesRegex(RuntimeError, "identity changed"):
            handlers.handle(
                "drain_market_stream",
                {"stream_id": "stale", "after_sequence": 0},
            )


if __name__ == "__main__":
    unittest.main()

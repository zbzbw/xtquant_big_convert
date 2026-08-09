import datetime
import os
import sys
import unittest
from types import SimpleNamespace
from unittest import mock


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader_strategy as strategy_module


class FakeApp:
    def __init__(self):
        self.inited = 0
        self.ticks = []
        self.orders = []
        self.trades = []
        self.sync_reasons = []

    def on_init(self, runtime):
        self.inited += 1

    def tick(self, now=None):
        self.ticks.append(now)

    def on_order_event(self, event):
        self.orders.append(event)

    def on_trade_event(self, event):
        self.trades.append(event)

    def sync_positions(self, reason):
        self.sync_reasons.append(reason)


class FakeContext:
    def __init__(self):
        self.accounts = []

    def set_account(self, account_id):
        self.accounts.append(account_id)


class FakeHistoryContext(FakeContext):
    def is_last_bar(self):
        return False


class FakeMarketStreamContext(FakeContext):
    def __init__(self):
        super().__init__()
        self.subscription_markets = []
        self.market_callback = None
        self.unsubscribed = []
        self.sector_requests = []

    def subscribe_whole_quote(self, markets, callback):
        self.subscription_markets.append(list(markets))
        self.market_callback = callback
        return 7

    def unsubscribe_quote(self, subscription_id):
        self.unsubscribed.append(subscription_id)

    def get_stock_list_in_sector(self, sector_name, real_timetag=-1):
        self.sector_requests.append((sector_name, real_timetag))
        values = {
            "\u6caa\u6df1A\u80a1": ["600000.SH", "000001.SZ"],
            "\u6caa\u6df1ETF": ["510050.SH"],
            "\u6caa\u6df1\u6307\u6570": ["000001.SH"],
        }
        return values.get(sector_name, [])


class FakeRpcService:
    def __init__(self):
        self.drained = []

    def drain_pending(self, max_items=20):
        self.drained.append(max_items)
        return 0

    def stop(self):
        pass


class BigQmtStrategyRunnerTest(unittest.TestCase):
    def setUp(self):
        self.app = FakeApp()
        strategy_module.reset_app()
        strategy_module.set_app_factory(lambda context: self.app)

    def tearDown(self):
        strategy_module.reset_app()
        strategy_module.set_app_factory(None)
        strategy_module.set_account_id("")

    def test_init_builds_app_and_calls_on_init(self):
        strategy_module.init(FakeContext())

        self.assertEqual(self.app.inited, 1)

    def test_init_sets_bigqmt_account_when_configured(self):
        context = FakeContext()
        strategy_module.set_account_id("test-account")

        strategy_module.init(context)

        self.assertEqual(context.accounts, ["test-account"])

    def test_init_detects_bigqmt_account_from_runtime_global(self):
        context = FakeContext()
        strategy_module.account = "runtime-account"
        try:
            strategy_module.init(context)
        finally:
            delattr(strategy_module, "account")

        self.assertEqual(context.accounts, ["runtime-account"])

    def test_adjust_forwards_to_app_tick(self):
        strategy_module.init(FakeContext())
        strategy_module.adjust(FakeContext())

        self.assertEqual(len(self.app.ticks), 1)
        self.assertIsInstance(self.app.ticks[0], datetime.datetime)

    def test_handlebar_forwards_to_app_tick(self):
        strategy_module.init(FakeContext())
        strategy_module.handlebar(FakeContext())

        self.assertEqual(len(self.app.ticks), 1)
        self.assertIsInstance(self.app.ticks[0], datetime.datetime)

    def test_adjust_skips_history_bars_when_bigqmt_exposes_is_last_bar(self):
        strategy_module.init(FakeContext())

        strategy_module.adjust(FakeHistoryContext())

        self.assertEqual(self.app.ticks, [])

    def test_adjust_drains_rpc_even_when_not_last_bar(self):
        rpc_service = FakeRpcService()
        strategy_module._rpc_service = rpc_service

        strategy_module.adjust(FakeHistoryContext())

        self.assertEqual(rpc_service.drained, [20])
        self.assertEqual(self.app.ticks, [])

    def test_adjust_cadence_can_suppress_normal_summary(self):
        config = {
            "adjust_cadence": {
                "log_normal": False,
                "window_seconds": 1.0,
                "warn_threshold_seconds": 2.0,
            }
        }
        with mock.patch.object(
            strategy_module.time,
            "time",
            side_effect=(100.0, 100.1, 101.1),
        ):
            with mock.patch("builtins.print") as print_mock:
                for _ in range(3):
                    strategy_module._record_adjust_tick(config)

        print_mock.assert_not_called()

    def test_adjust_cadence_preserves_stall_warning(self):
        config = {
            "adjust_cadence": {
                "log_normal": False,
                "window_seconds": 1.0,
                "warn_threshold_seconds": 0.5,
            }
        }
        with mock.patch.object(
            strategy_module.time,
            "time",
            side_effect=(100.0, 100.1, 101.1),
        ):
            with mock.patch("builtins.print") as print_mock:
                for _ in range(3):
                    strategy_module._record_adjust_tick(config)

        self.assertEqual(print_mock.call_count, 1)
        self.assertIn("WARNING adjust cadence stalled", print_mock.call_args[0][0])

    def test_market_stream_bridges_whole_quote_callback(self):
        context = FakeMarketStreamContext()

        stream = strategy_module._start_market_stream(
            context,
            {
                "market_stream": {
                    "enabled": True,
                    "markets": ("SH", "SZ"),
                    "max_batches": 10,
                    "max_records": 100,
                    "batch_max_records": 2,
                }
            },
        )
        context.market_callback(
            {
                "600000.SH": {"time": 1},
                "600001.SH": {"time": 1},
                "600002.SH": {"time": 1},
            }
        )
        context.market_callback({"000001.SZ": {"time": 1}})
        context.market_callback({"600000.SH": {"time": 2}})
        result = stream.drain(after_sequence=0)

        self.assertEqual(context.subscription_markets, [["SH", "SZ"]])
        self.assertEqual(result["next_sequence"], 4)
        self.assertEqual(result["batches"][0]["records"]["600000.SH"]["time"], 1)
        self.assertEqual(
            [batch["is_bootstrap"] for batch in result["batches"]],
            [True, True, True, False],
        )
        self.assertEqual(
            [batch["callback_sequence"] for batch in result["batches"]],
            [1, 1, 2, 3],
        )

        strategy_module.reset_app()

        self.assertEqual(context.unsubscribed, [7])

    def test_market_stream_requires_native_whole_quote_api(self):
        with self.assertRaisesRegex(RuntimeError, "subscribe_whole_quote"):
            strategy_module._start_market_stream(
                FakeContext(),
                {"market_stream": {"enabled": True}},
            )

    def test_market_stream_filters_callbacks_to_configured_sectors(self):
        context = FakeMarketStreamContext()

        stream = strategy_module._start_market_stream(
            context,
            {
                "market_stream": {
                    "enabled": True,
                    "markets": ("SH", "SZ"),
                    "sectors": ("cn_a_share", "cn_etf", "cn_index"),
                    "max_batches": 10,
                    "max_records": 100,
                    "batch_max_records": 10,
                }
            },
        )
        context.market_callback(
            {
                "600000.SH": {"time": 1},
                "000001.SZ": {"time": 1},
                "510050.SH": {"time": 1},
                "000001.SH": {"time": 1},
                "204001.SH": {"time": 1},
            }
        )

        result = stream.drain(after_sequence=0)
        records = {
            code
            for batch in result["batches"]
            for code in batch["records"]
        }

        self.assertEqual(
            records,
            {"600000.SH", "000001.SZ", "510050.SH", "000001.SH"},
        )
        self.assertEqual(result["universe_size"], 4)
        self.assertEqual(result["filtered_records"], 1)

    def test_zmq_rpc_build_does_not_create_redis_clients(self):
        config = {
            "account_id": "acct",
            "enable_rpc": True,
            "rpc": {
                "enabled": True,
                "account_id": "acct",
                "transport": "zmq",
                "zmq": {"connect_address": "tcp://127.0.0.1:20146"},
                "background_threads": True,
            },
            "qmt_api": {},
        }
        app = SimpleNamespace(order_gateway=None, position_sync_sink=None)

        with mock.patch(
            "bigqmt_signal_trader.adapters.redis_common.build_redis_client",
            side_effect=AssertionError("ZMQ mode must not build Redis clients"),
        ):
            service = strategy_module._build_rpc_service(FakeContext(), app, config)

        self.assertIsNone(service.listen_redis)
        self.assertIsNone(service.redis)
        self.assertEqual(service._transport.name, "zmq")

    def test_order_and_trade_callbacks_forward_to_app(self):
        strategy_module.init(FakeContext())
        order = object()
        trade = object()

        strategy_module.on_order(FakeContext(), order)
        strategy_module.on_trade(FakeContext(), trade)

        self.assertEqual(self.app.orders, [order])
        self.assertEqual(self.app.trades, [trade])

    def test_bigqmt_named_callbacks_forward_to_app(self):
        strategy_module.init(FakeContext())
        order = object()
        trade = object()

        strategy_module.order_callback(FakeContext(), order)
        strategy_module.deal_callback(FakeContext(), trade)

        self.assertEqual(self.app.orders, [order])
        self.assertEqual(self.app.trades, [trade])

    def test_manual_sync_forwards_to_app(self):
        strategy_module.init(FakeContext())

        strategy_module.sync_positions(FakeContext())

        self.assertEqual(self.app.sync_reasons, ["manual"])


if __name__ == "__main__":
    unittest.main()

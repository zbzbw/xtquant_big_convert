import json
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.exec_events import (
    format_raw_snapshot,
    normalize_cancel_error_event,
    normalize_order_error_event,
    normalize_order_event,
    normalize_trade_event,
    order_channel,
    order_error_channel,
    cancel_error_channel,
    publish_order_event,
    publish_trade_event,
    raw_field_snapshot,
    trade_channel,
)
from bigqmt_signal_trader.xtquant_compat import BigQmtXtTrader, XtQuantTraderCallback


class FakeDeal:
    m_strAccountID = "acct"
    m_strInstrumentID = "600000.SH"
    m_dPrice = 10.5
    m_nVolume = 100
    m_strTradeID = "T1"
    m_strOrderSysID = "O1"
    m_strTradeTime = "2026-07-02 10:00:00"
    m_nDirection = 48
    m_dTradeAmount = 1050.0
    m_dComssion = 0.5


class FakeOrder:
    m_strAccountID = "acct"
    m_strInstrumentID = "000001.SZ"
    m_nOrderStatus = 50
    m_nVolumeTotal = 200
    m_nVolumeTraded = 50
    m_dLimitPrice = 9.9
    m_strOrderSysID = "O2"
    m_nDirection = 49
    m_strOptName = "s1"


class FakeRedis:
    def __init__(self):
        self.xadds = []
        self.pubs = []

    def xadd(self, key, fields, maxlen=None, approximate=None):
        self.xadds.append((key, fields))
        return b"1-0"

    def publish(self, key, value):
        self.pubs.append((key, value))
        return 1


class RecordingCallback(XtQuantTraderCallback):
    def __init__(self):
        self.orders = []
        self.trades = []
        self.order_errors = []
        self.cancel_errors = []
        self.async_responses = []
        self.cancel_async_responses = []
        self.account_statuses = []

    def on_stock_order(self, order):
        self.orders.append(order)

    def on_stock_trade(self, trade):
        self.trades.append(trade)

    def on_order_error(self, order_error):
        self.order_errors.append(order_error)

    def on_cancel_error(self, cancel_error):
        self.cancel_errors.append(cancel_error)

    def on_order_stock_async_response(self, response):
        self.async_responses.append(response)

    def on_cancel_order_stock_async_response(self, response):
        self.cancel_async_responses.append(response)

    def on_account_status(self, status):
        self.account_statuses.append(status)


class ExecEventsServerTest(unittest.TestCase):
    def test_normalize_trade_event_maps_thinktrader_fields(self):
        ev = normalize_trade_event(FakeDeal(), "acct")

        self.assertEqual(ev["event_type"], "trade")
        self.assertEqual(ev["stock_code"], "600000.SH")
        self.assertEqual(ev["trade_id"], "T1")
        self.assertEqual(ev["order_sys_id"], "O1")
        self.assertEqual(ev["volume"], 100)
        self.assertEqual(ev["price"], 10.5)
        self.assertEqual(ev["action"], "BUY")  # m_nDirection 48 -> buy
        self.assertEqual(ev["traded_at"], "2026-07-02 10:00:00")
        self.assertEqual(ev["commission"], 0.5)

    def test_normalize_order_event_maps_thinktrader_fields(self):
        ev = normalize_order_event(FakeOrder(), "acct")

        self.assertEqual(ev["event_type"], "order")
        self.assertEqual(ev["stock_code"], "000001.SZ")
        self.assertEqual(ev["order_sys_id"], "O2")
        self.assertEqual(ev["order_volume"], 200)
        self.assertEqual(ev["traded_volume"], 50)
        self.assertEqual(ev["status"], 50)
        self.assertEqual(ev["action"], "SELL")  # m_nDirection 49 -> sell
        self.assertEqual(ev["strategy_name"], "s1")

    def test_publish_writes_stream_and_channel(self):
        r = FakeRedis()
        publish_trade_event(r, "acct", {"event_type": "trade", "trade_id": "T1"})

        self.assertEqual(r.pubs[0][0], trade_channel("acct"))
        self.assertEqual(r.xadds[0][0], trade_channel("acct"))
        self.assertIn("T1", r.pubs[0][1])

        publish_order_event(r, "acct", {"event_type": "order"})
        self.assertEqual(r.pubs[1][0], order_channel("acct"))

    def test_arbitration_resolves_direction_offset_conflict_via_op_type(self):
        """When m_nDirection and m_nOffsetFlag disagree (futures: sell+open),
        arbitration via m_nOpType picks the semantically correct field."""
        class Deal:
            m_strInstrumentID = "600000.SH"
            m_nDirection = 49    # EEntrustBS sell
            m_nOffsetFlag = 48   # offset 48 = 开仓 (open)
            m_nOpType = 24       # STOCK_SELL — arbiter confirms sell
            m_nVolume = 10
            m_dPrice = 1.0
            m_strTradeID = "X"

        ev = normalize_trade_event(Deal(), "acct")

        self.assertEqual(ev["action"], "SELL")       # from direction via arbitration
        self.assertEqual(ev["direction"], 49)        # direction field = m_nDirection
        self.assertEqual(ev["offset_flag"], 48)      # raw offset preserved, not conflated

    def test_arbitration_stock_sell_wrong_direction_fixed_by_op_type(self):
        """Stock sell: m_nDirection=48 (bug: always 48), m_nOffsetFlag=49,
        m_nOpType=24 → arbitration picks offset (49→SELL)."""
        class SellOrder:
            m_strInstrumentID = "601398.SH"
            m_nDirection = 48       # QMT bug — always 48 in live callbacks
            m_nOffsetFlag = 49      # 平仓 = sell (correct)
            m_nOpType = 24          # STOCK_SELL (correct)
            m_nVolumeTotal = 100
            m_nVolumeTraded = 0
            m_dLimitPrice = 6.34
            m_strOrderSysID = "S123"

        ev = normalize_order_event(SellOrder(), "acct")
        self.assertEqual(ev["action"], "SELL")
        self.assertEqual(ev["direction"], 49)       # offset_flag wins via arbitration

    def test_arbitration_stock_buy_agree(self):
        """Stock buy: m_nDirection=48, m_nOffsetFlag=48 → agree → BUY."""
        class BuyOrder:
            m_strInstrumentID = "601398.SH"
            m_nDirection = 48
            m_nOffsetFlag = 48
            m_nOpType = 23
            m_nVolumeTotal = 100
            m_nVolumeTraded = 0
            m_dLimitPrice = 5.0
            m_strOrderSysID = "B456"

        ev = normalize_order_event(BuyOrder(), "acct")
        self.assertEqual(ev["action"], "BUY")
        self.assertEqual(ev["direction"], 48)

    def test_direction_zero_falls_back_to_offset(self):
        """m_nDirection=0 is treated as absent; offset determines direction."""
        class SellOrder:
            m_strInstrumentID = "601398.SH"
            m_nDirection = 0
            m_nOffsetFlag = 49
            m_nVolumeTotal = 100
            m_nVolumeTraded = 0
            m_dLimitPrice = 6.34
            m_strOrderSysID = "S123"

        ev = normalize_order_event(SellOrder(), "acct")
        self.assertEqual(ev["action"], "SELL")
        self.assertEqual(ev["direction"], 49)

    def test_direction_none_falls_back_to_offset(self):
        """m_nDirection=None → offset determines direction."""
        class BuyOrder:
            m_strInstrumentID = "601398.SH"
            m_nDirection = None
            m_nOffsetFlag = 48
            m_nVolumeTotal = 100
            m_nVolumeTraded = 0
            m_dLimitPrice = 5.0
            m_strOrderSysID = "B456"

        ev = normalize_order_event(BuyOrder(), "acct")
        self.assertEqual(ev["action"], "BUY")
        self.assertEqual(ev["direction"], 48)

    def test_pledge_direction_has_no_buy_sell_action(self):
        class Deal:
            m_strInstrumentID = "600000.SH"
            m_nDirection = 81   # 质押入库
            m_nVolume = 10
            m_dPrice = 1.0

        ev = normalize_trade_event(Deal(), "acct")

        self.assertEqual(ev["action"], "")   # pledge is neither buy nor sell
        self.assertEqual(ev["direction"], 81)  # raw direction preserved

    def test_normalize_order_error_event_maps_fields(self):
        class OrderError:
            m_strAccountID = "acct"
            m_strInstrumentID = "600654.SH"
            m_strOrderSysID = "sys-err-1"
            m_nErrorID = 2147483647
            m_strErrorMsg = "废单"

        ev = normalize_order_error_event(OrderError(), "acct")

        self.assertEqual(ev["event_type"], "order_error")
        self.assertEqual(ev["account_id"], "acct")
        self.assertEqual(ev["stock_code"], "600654.SH")
        self.assertEqual(ev["order_sys_id"], "sys-err-1")
        self.assertEqual(ev["error_id"], 2147483647)
        self.assertEqual(ev["error_msg"], "废单")

    def test_normalize_cancel_error_event_maps_fields(self):
        class CancelError:
            m_strAccountID = "acct"
            m_strInstrumentID = "600654.SH"
            m_strOrderSysID = "sys-cancel-1"
            m_nErrorID = 99
            m_strErrorMsg = "撤单失败"

        ev = normalize_cancel_error_event(CancelError(), "acct")

        self.assertEqual(ev["event_type"], "cancel_error")
        self.assertEqual(ev["account_id"], "acct")
        self.assertEqual(ev["order_sys_id"], "sys-cancel-1")
        self.assertEqual(ev["error_id"], 99)
        self.assertEqual(ev["error_msg"], "撤单失败")

    def test_error_channels_are_account_scoped(self):
        self.assertTrue(order_error_channel("acct").endswith(":acct"))
        self.assertTrue(cancel_error_channel("acct").endswith(":acct"))


class RawFieldSnapshotTest(unittest.TestCase):
    """The snapshot exists to settle what live callbacks actually carry, so it
    must capture m_* and MiniQMT fields alike and never raise."""

    def test_captures_thinktrader_and_miniqmt_fields(self):
        snap = raw_field_snapshot(FakeOrder())

        self.assertIn("m_nDirection", snap)
        self.assertIn("49", snap["m_nDirection"])
        self.assertIn("int", snap["m_nDirection"])
        self.assertIn("m_strInstrumentID", snap)

    def test_captures_miniqmt_style_object(self):
        class XtOrderLike:
            stock_code = "601398.SH"
            order_type = 24
            order_volume = 100

        snap = raw_field_snapshot(XtOrderLike())

        self.assertIn("24", snap["order_type"])
        self.assertIn("601398.SH", snap["stock_code"])

    def test_captures_dict_payload(self):
        snap = raw_field_snapshot({"m_nOffsetFlag": 48, "order_type": 24})

        self.assertIn("48", snap["m_nOffsetFlag"])
        self.assertIn("24", snap["order_type"])

    def test_skips_callables_and_dunders(self):
        class WithMethod:
            m_nDirection = 49

            def m_method(self):
                return 1

        snap = raw_field_snapshot(WithMethod())

        self.assertIn("m_nDirection", snap)
        self.assertNotIn("m_method", snap)

    def test_unreadable_attribute_does_not_raise(self):
        class Exploding:
            m_nDirection = 49

            @property
            def m_nOffsetFlag(self):
                raise RuntimeError("boom")

        snap = raw_field_snapshot(Exploding())

        self.assertIn("m_nDirection", snap)
        self.assertIn("unreadable", snap["m_nOffsetFlag"])

    def test_format_is_a_single_ascii_safe_line(self):
        line = format_raw_snapshot("order", FakeOrder())

        self.assertNotIn("\n", line)
        self.assertTrue(line.startswith("[bigqmt_exec_raw] order"))
        self.assertIn("m_nDirection", line)


class ExecEventsClientDispatchTest(unittest.TestCase):
    def _trader(self):
        trader = BigQmtXtTrader(account_id="acct")
        cb = RecordingCallback()
        trader.register_callback(cb)
        return trader, cb

    def test_dispatch_trade_invokes_on_stock_trade(self):
        trader, cb = self._trader()
        event = {
            "event_type": "trade",
            "account_id": "acct",
            "stock_code": "600000.SH",
            "order_sys_id": "sys-1",
            "trade_id": "t-1",
            "volume": 100,
            "price": 10.5,
            "action": "BUY",
            "traded_at": "2026-07-02 10:00:00",
        }
        trader._dispatch_event(json.dumps(event).encode("utf-8"))

        self.assertEqual(len(cb.trades), 1)
        trade = cb.trades[0]
        self.assertEqual(trade.stock_code, "600000.SH")
        self.assertEqual(trade.trade_id, "t-1")
        self.assertEqual(trade.traded_volume, 100)
        self.assertEqual(trade.traded_price, 10.5)
        self.assertEqual(trade.order_type, 23)  # BUY -> STOCK_BUY

    def test_dispatch_order_invokes_on_stock_order(self):
        trader, cb = self._trader()
        event = {
            "event_type": "order",
            "account_id": "acct",
            "stock_code": "000001.SZ",
            "order_sys_id": "sys-2",
            "order_volume": 200,
            "traded_volume": 50,
            "price": 9.9,
            "status": 50,
            "action": "SELL",
        }
        trader._dispatch_event(json.dumps(event).encode("utf-8"))

        self.assertEqual(len(cb.orders), 1)
        order = cb.orders[0]
        self.assertEqual(order.stock_code, "000001.SZ")
        self.assertEqual(order.order_volume, 200)
        self.assertEqual(order.traded_volume, 50)
        self.assertEqual(order.order_status, 50)
        self.assertEqual(order.order_type, 24)  # SELL -> STOCK_SELL

    def test_dispatch_without_callback_is_noop(self):
        trader = BigQmtXtTrader(account_id="acct")
        # No callback registered; must not raise.
        trader._dispatch_event(json.dumps({"event_type": "trade"}).encode("utf-8"))

    def test_dispatch_order_error_invokes_on_order_error(self):
        trader, cb = self._trader()
        event = {
            "event_type": "order_error",
            "account_id": "acct",
            "stock_code": "600654.SH",
            "order_sys_id": "sys-err-1",
            "error_id": 2147483647,
            "error_msg": "废单",
        }
        trader._dispatch_event(json.dumps(event).encode("utf-8"))

        self.assertEqual(len(cb.order_errors), 1)
        err = cb.order_errors[0]
        self.assertEqual(err.order_id, "sys-err-1")
        self.assertEqual(err.error_id, 2147483647)
        self.assertEqual(err.error_msg, "废单")
        self.assertEqual(err.stock_code, "600654.SH")

    def test_dispatch_cancel_error_invokes_on_cancel_error(self):
        trader, cb = self._trader()
        event = {
            "event_type": "cancel_error",
            "account_id": "acct",
            "stock_code": "600654.SH",
            "order_sys_id": "sys-cancel-1",
            "error_id": 99,
            "error_msg": "撤单失败",
        }
        trader._dispatch_event(json.dumps(event).encode("utf-8"))

        self.assertEqual(len(cb.cancel_errors), 1)
        err = cb.cancel_errors[0]
        self.assertEqual(err.order_id, "sys-cancel-1")
        self.assertEqual(err.error_id, 99)
        self.assertEqual(err.error_msg, "撤单失败")

    def test_order_stock_async_returns_seq_and_fires_response(self):
        trader, cb = self._trader()
        # order_stock is stubbed? No — it would do an RPC. Instead call the
        # helper directly with a dict-like result via monkeypatching is heavy;
        # here we only verify the seq increments and the async-response path
        # fires when order_stock returns a dict (mocked below).
        original_order_stock = trader.order_stock

        def fake_order_stock(*args, **kwargs):
            return {"order_sys_id": "sys-ok-1", "user_order_id": "u-1"}

        trader.order_stock = fake_order_stock
        try:
            seq = trader.order_stock_async("acct", "600654.SH", 23, 100, 11, 10.0, "s", "r")
        finally:
            trader.order_stock = original_order_stock

        self.assertGreater(seq, 0)
        self.assertEqual(len(cb.async_responses), 1)
        resp = cb.async_responses[0]
        self.assertEqual(resp.order_id, "sys-ok-1")
        self.assertEqual(resp.account_id, "acct")
        self.assertEqual(resp.seq, seq)
    def test_order_stock_async_minus_one_fires_order_error(self):
        trader, cb = self._trader()
        original_order_stock = trader.order_stock

        def fake_order_stock(*args, **kwargs):
            return -1  # MiniQMT: submit failed

        trader.order_stock = fake_order_stock
        try:
            seq = trader.order_stock_async("acct", "600654.SH", 23, 100, 11, 10.0, "s", "r")
        finally:
            trader.order_stock = original_order_stock

        self.assertGreater(seq, 0)
        self.assertEqual(len(cb.order_errors), 1)
        err = cb.order_errors[0]
        self.assertEqual(err.error_id, -1)
        self.assertEqual(err.stock_code, "600654.SH")
        # No success response for a failed submit.
        self.assertEqual(len(cb.async_responses), 0)

    def test_cancel_order_stock_async_fires_response(self):
        trader, cb = self._trader()
        original = trader.cancel_order_stock_sysid

        def fake_cancel(account, market, sysid):
            return True

        trader.cancel_order_stock_sysid = fake_cancel
        try:
            seq = trader.cancel_order_stock_sysid_async("acct", "SH", "sys-1")
        finally:
            trader.cancel_order_stock_sysid = original

        self.assertGreater(seq, 0)
        self.assertEqual(len(cb.cancel_async_responses), 1)
        resp = cb.cancel_async_responses[0]
        self.assertTrue(resp.success)
        self.assertEqual(resp.order_sys_id, "sys-1")
        self.assertEqual(resp.account_id, "acct")
        self.assertEqual(resp.seq, seq)

    def test_connect_and_subscribe_fire_account_status(self):
        trader, cb = self._trader()
        trader.client.account_id = "acct"
        # connect() calls ping via RPC — stub it.
        trader.client.call = lambda *a, **k: {"ok": True}
        trader.connect()
        trader.subscribe("acct")

        self.assertEqual(len(cb.account_statuses), 2)
        status = cb.account_statuses[0]
        self.assertEqual(status.account_id, "acct")
        self.assertEqual(status.account_type, "STOCK")
        self.assertEqual(status.status, 1)


if __name__ == "__main__":
    unittest.main()

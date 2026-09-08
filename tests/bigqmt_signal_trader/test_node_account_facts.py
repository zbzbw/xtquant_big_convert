"""Synthetic node facts exercise real handlers/adapters, never rewrite replies."""
import datetime as dt
import os
import sys
from types import SimpleNamespace as Row
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
from bigqmt_signal_trader.adapters.order_bigqmt import BigQmtOrderGateway
from bigqmt_signal_trader.adapters.position_bigqmt import BigQmtPositionProvider
from bigqmt_signal_trader.models import TradeSignal
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, RedisPubSubRpcService
from bigqmt_signal_trader.risk_guard import validate_signal


class SyntheticQmt:
    def __init__(self):
        self.facts = {"account_id": "sim-account", "session_id": "session-1"}
        self.writes = []
        self.orders = []
        self.trades = []

    def session_facts(self):
        return dict(self.facts)

    def get_full_tick(self, codes):
        return {code: {"lastPrice": 10.0} for code in codes}

    def query(self, account, account_type, detail, strategy=""):
        if detail == "ORDER":
            return self.orders
        if detail in ("DEAL", "TRADE"):
            return self.trades
        if detail == "POSITION":
            return [Row(stock_code="600000.SH", volume=100, available=0, cost=10.0)] if self.trades else []
        if detail == "ACCOUNT":
            return [Row(cash=99000.0 if self.trades else 100000.0,
                        total_asset=100000.0, frozen_cash=0.0)]
        return []

    def passorder(self, *args):
        self.writes.append(("submit", args))
        self.orders.append(Row(order_sys_id="order-1", user_order_id=args[9],
                               stock_code=args[3], offset_flag=48, volume=args[6],
                               traded_volume=0, status=50, price=args[5]))

    def partial_fill(self):
        self.orders[0].traded_volume = 100
        self.orders[0].status = 55
        self.trades.append(Row(trade_id="trade-1", order_sys_id="order-1",
                               user_order_id="tag-1", stock_code="600000.SH",
                               offset_flag=48, volume=100, price=10.0))

    def cancel(self, *args):
        self.writes.append(("cancel", args))
        self.orders[0].status = 53
        return True


class MemoryRedis:
    def setex(self, *args):
        return True

    def publish(self, *args):
        return 1


def make_handlers(qmt, **overrides):
    options = dict(
        account_id="sim-account", market_data=BigQmtMarketDataProvider(qmt),
        position_provider=BigQmtPositionProvider(qmt.query),
        order_gateway=BigQmtOrderGateway(qmt, "sim-account", qmt.passorder, qmt.cancel, qmt.query),
        allow_order_methods=True,
        node_account_binding={"account_id": "sim-account", "account_environment": "broker_sim"},
        session_facts_reader=qmt.session_facts,
    )
    options.update(overrides)
    return BigQmtRpcHandlers(**options)


def order(**overrides):
    result = dict(stock_code="600000.SH", action="BUY", volume=200,
                  price=10.0, price_type="LIMIT", order_remark="tag-1")
    result.update(overrides)
    return result


def test_protocol_limit_partial_cancel_reconcile_and_journal():
    qmt = SyntheticQmt()
    handlers = make_handlers(qmt)
    service = RedisPubSubRpcService(MemoryRedis(), handlers, account_id="sim-account")

    def rpc(method, params=None):
        response = service.process_request(dict(method=method, params=params or {}))
        assert response["ok"], response["error"]
        return response["data"]

    assert rpc("ping")["account_environment"] == "broker_sim"
    assert rpc("get_full_tick", {"codes": ["600000.SH"]})["600000.SH"]["lastPrice"] == 10.0
    now = dt.datetime(2026, 9, 8, 10)
    signal = TradeSignal.from_dict(dict(signal_id="tag-1", account_id="sim-account",
        action="BUY", created_at=now, expire_at=now + dt.timedelta(minutes=1),
        schema_version=1, stock_code="600000.SH", amount=200))
    decision = validate_signal(signal, now, handlers.handle("get_positions"))
    assert decision.allowed and decision.volume == 200
    batch = {"orders": [order(volume=decision.volume, require_idempotency_check=True)]}
    assert rpc("order_stock_batch", batch)[0]["accepted"]
    # QMT's existing stock fixed-price path (11/1101) is the DAY limit path;
    # no order rewrite or alternative converter gateway is used here.
    assert qmt.writes[0][1][:7] == (23, 1101, "sim-account", "600000.SH", 11, 10.0, 200)
    assert len(handlers._submit_journal) == 1
    assert rpc("order_stock_batch", batch)[0]["idempotent"]
    assert len(qmt.writes) == 1
    qmt.partial_fill()
    snapshot = rpc("query_execution_snapshot")
    assert snapshot["orders"][0]["traded_volume"] == 100
    assert snapshot["trades"][0]["volume"] == 100
    assert rpc("cancel_order", {"order_sys_id": "order-1"})["success"]
    assert qmt.writes[-1][1][1] == "sim-account"
    snapshot = rpc("query_execution_snapshot")
    assert snapshot["orders"][0]["status"] == "53"
    assert rpc("get_positions")["600000.SH"]["volume"] == 100
    assert rpc("get_asset")["cash"] == 99000.0


@pytest.mark.parametrize("environment,identity", [
    (env, account) for env in (None, "live", "paper", "broker_sim")
    for account in (None, "other", "sim-account")
    if (env, account) != ("broker_sim", "sim-account")
])
def test_environment_identity_matrix_zero_writes(environment, identity):
    qmt = SyntheticQmt()
    qmt.facts["account_id"] = identity
    handlers = make_handlers(qmt, node_account_binding={
        "account_id": "sim-account", "account_environment": environment})
    expected = "live" if identity == "sim-account" and environment == "live" else None
    assert handlers.handle("ping")["account_environment"] == expected
    for method, params in [("order_stock", order()), ("order_stock_batch", {"orders": [order()]}),
                           ("cancel_order_stock", {"order_id": "order-1"})]:
        with pytest.raises(PermissionError):
            handlers.handle(method, params)
    assert qmt.writes == [] and handlers._submit_journal == {}


@pytest.mark.parametrize("overrides", [
    {"node_account_binding": None}, {"session_facts_reader": None},
    {"session_facts_reader": lambda: {"account_id": "sim-account"}},
    {"session_facts_reader": lambda: 1 / 0},
])
def test_client_forgery_and_asset_echo_cannot_supply_evidence(overrides):
    qmt = SyntheticQmt()
    handlers = make_handlers(qmt, **overrides)
    forged = dict(account_id="sim-account", account_environment="broker_sim", orders_enabled=True,
                  node_account_binding={"account_id": "sim-account", "account_environment": "broker_sim"},
                  session_id="session-1", account_type="STOCK")
    assert handlers.handle("get_asset").account_id == "sim-account"
    assert handlers.handle("ping", forged)["account_environment"] is None
    with pytest.raises(PermissionError):
        handlers.handle("submit_order", order(**forged))
    assert qmt.writes == []


@pytest.mark.parametrize("params", [{"account_id": "other"}, {"account": "other"},
    {"account_id": "sim-account", "account": {"account_id": "sim-account", "id": "other"}}])
def test_conflicting_rpc_account_and_batch_override_zero_writes(params):
    qmt = SyntheticQmt()
    handlers = make_handlers(qmt)
    for method, payload in [("submit_order", order(**params)),
                            ("cancel_order", dict(order_id="order-1", **params)),
                            ("submit_orders_batch", {"orders": [order(), order(**params)]})]:
        with pytest.raises(PermissionError):
            handlers.handle(method, payload)
    assert qmt.writes == []


@pytest.mark.parametrize("changed", [{"account_id": "other", "session_id": "session-1"},
                                     {"account_id": "sim-account", "session_id": "session-2"}, {}])
def test_session_change_invalidates_old_binding_even_if_restored(changed):
    qmt = SyntheticQmt()
    handlers = make_handlers(qmt)
    original = qmt.facts
    qmt.facts = changed
    # No intervening ping: the write entry itself must refresh evidence.
    with pytest.raises(PermissionError):
        handlers.handle("submit_order", order())
    qmt.facts = original
    assert handlers.handle("ping")["account_environment"] is None
    with pytest.raises(PermissionError):
        handlers.handle("cancel_order", {"order_id": "order-1"})
    assert qmt.writes == []


def test_default_switch_and_gateway_account_remain_closed():
    qmt = SyntheticQmt()
    for enabled in (False, True):
        handlers = make_handlers(qmt, allow_order_methods=enabled)
        handlers.order_gateway.account_id = "other"
        with pytest.raises((ValueError, PermissionError)):
            handlers.handle("submit_order", order())
    assert qmt.writes == []


def test_batch_rechecks_session_after_identity_query_before_write():
    qmt = SyntheticQmt()
    handlers = make_handlers(qmt)
    query = qmt.query

    def changed_session(*args):
        qmt.facts["session_id"] = "reconnected"
        return query(*args)

    handlers.order_gateway.get_trade_detail_data = changed_session
    result = handlers.handle("submit_orders_batch", {
        "orders": [order(require_idempotency_check=True)]})
    assert not result[0]["accepted"]
    assert qmt.writes == [] and handlers._submit_journal == {}


def test_binding_is_copied_and_ping_does_not_authorize_future_cancel():
    qmt = SyntheticQmt()
    binding = {"account_id": "sim-account", "account_environment": "live"}
    handlers = make_handlers(qmt, node_account_binding=binding)
    binding["account_environment"] = "broker_sim"
    assert handlers.handle("ping")["account_environment"] == "live"
    handlers = make_handlers(qmt)
    assert handlers.handle("ping")["account_environment"] == "broker_sim"
    qmt.facts = {}
    with pytest.raises(PermissionError):
        handlers.handle("cancel_order", {"order_id": "order-1"})
    assert qmt.writes == []


def test_strategy_runtime_assembly_uses_only_node_inputs(monkeypatch):
    import bigqmt_signal_trader_strategy as strategy
    # The runtime applies local defaults at import; isolate its module globals
    # so this assembly test cannot enable RPC in unrelated runner tests.
    monkeypatch.setattr(strategy, "_config", dict(strategy._config))
    monkeypatch.setattr(strategy, "_account_id", strategy._account_id)
    import bigqmt_signal_trader_redis_rpc_runtime as runtime

    qmt = SyntheticQmt()
    config = {"node_account_binding": {"account_id": "sim-account", "account_environment": "broker_sim"},
              "session_facts_reader": qmt.session_facts}
    with patch.object(runtime, "BIGQMT_REDIS_CONFIG", config), patch.object(runtime, "configure") as configure:
        runtime._apply_config("sim-account")
    assembled = configure.call_args.kwargs
    assembled.update(redis_client=MemoryRedis(), response_redis_client=MemoryRedis(),
                     qmt_api={"get_trade_detail_data": qmt.query})
    app = Row(order_gateway=make_handlers(qmt).order_gateway)
    service = strategy._build_rpc_service(qmt, app, assembled)
    assert service.handlers.handle("ping")["account_environment"] == "broker_sim"
    assembled.pop("node_account_binding")
    assembled["rpc"]["node_account_binding"] = config["node_account_binding"]
    service = strategy._build_rpc_service(qmt, app, assembled)
    assert service.handlers.handle("ping")["account_environment"] is None

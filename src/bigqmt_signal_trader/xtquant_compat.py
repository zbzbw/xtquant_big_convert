"""MiniQMT-style client objects backed by Big QMT Redis RPC.

This module is the replacement edge for existing code that already calls
``xt_trader.query_stock_positions(...)`` or ``xtdata.get_full_tick(...)``.
The Big QMT process remains the only place that touches QMT runtime APIs.
"""

import os
import json
import time
import uuid
import threading
import importlib
from typing import Any, Dict, Iterable, List, Optional

from .full_tick_cache import request_full_tick_cache, wait_full_tick_cache
from .local_cache import LocalMarketCache
from .redis_rpc import call_redis_rpc


# Default OHLCV fields pulled + cached by download_history_data*.
DEFAULT_DOWNLOAD_FIELDS = ["open", "high", "low", "close", "volume", "amount"]
_TIME_COL_NAMES = ("stime", "time", "index", "date", "datetime", "timetag")


STOCK_BUY = 23
STOCK_SELL = 24
FIX_PRICE = 11
LATEST_PRICE = 5
MARKET_PEER_PRICE_FIRST = 44
MARKET_SH_CONVERT_5_LIMIT = 43
MARKET_SZ_CONVERT_5_CANCEL = 47
SZ_MARKET = 1
SH_MARKET = 0

CLIENT_CONFIG_MODULE_ENV = "BIGQMT_CLIENT_CONFIG_MODULE"
DEFAULT_CLIENT_CONFIG_MODULES = (
    "bigqmt_signal_trader_client_config",
    "bigqmt_signal_trader_local_config",
)

ORDER_UNREPORTED = 48
ORDER_WAIT_REPORTING = 49
ORDER_REPORTED = 50
ORDER_REPORTED_CANCEL = 51
ORDER_PARTSUCC_CANCEL = 52
ORDER_PART_CANCEL = 53
ORDER_CANCELED = 54
ORDER_PART_SUCC = 55
ORDER_SUCCEEDED = 56
ORDER_JUNK = 57
ORDER_UNKNOWN = 255

# ---------------------------------------------------------------------------
# xtconstant 枚举常量（对齐原生 MiniQMT xtquant/xtconstant.py，91 个全量）
# ---------------------------------------------------------------------------

# 账号类型
FUTURE_ACCOUNT = 1            # 期货
SECURITY_ACCOUNT = 2          # 股票
CREDIT_ACCOUNT = 3            # 信用
FUTURE_OPTION_ACCOUNT = 5     # 期货期权
STOCK_OPTION_ACCOUNT = 6      # 股票期权
HUGANGTONG_ACCOUNT = 7        # 沪港通
SHENGANGTONG_ACCOUNT = 11     # 深港通

# 委托类型 - 期货六键风格
FUTURE_OPEN_LONG = 0                  # 开多
FUTURE_CLOSE_LONG_HISTORY = 1         # 平昨多
FUTURE_CLOSE_LONG_TODAY = 2           # 平今多
FUTURE_OPEN_SHORT = 3                 # 开空
FUTURE_CLOSE_SHORT_HISTORY = 4        # 平昨空
FUTURE_CLOSE_SHORT_TODAY = 5          # 平今空
# 委托类型 - 期货四键风格
FUTURE_CLOSE_LONG_TODAY_FIRST = 6     # 平多，优先平今
FUTURE_CLOSE_LONG_HISTORY_FIRST = 7   # 平多，优先平昨
FUTURE_CLOSE_SHORT_TODAY_FIRST = 8    # 平空，优先平今
FUTURE_CLOSE_SHORT_HISTORY_FIRST = 9  # 平空，优先平昨
# 委托类型 - 期货两键风格
FUTURE_CLOSE_LONG_TODAY_HISTORY_THEN_OPEN_SHORT = 10  # 卖出，优先平仓平今，余量开空
FUTURE_CLOSE_LONG_HISTORY_TODAY_THEN_OPEN_SHORT = 11  # 卖出，优先平仓平昨，余量开空
FUTURE_CLOSE_SHORT_TODAY_HISTORY_THEN_OPEN_LONG = 12  # 买入，优先平仓平今，余量开多
FUTURE_CLOSE_SHORT_HISTORY_TODAY_THEN_OPEN_LONG = 13  # 买入，优先平仓平昨，余量开多
FUTURE_OPEN = 14               # 买入，不优先平仓
FUTURE_CLOSE = 15              # 卖出，不优先平仓
# 委托类型 - 期货跨商品套利
FUTURE_ARBITRAGE_OPEN = 16               # 开仓
FUTURE_ARBITRAGE_CLOSE_HISTORY_FIRST = 17  # 平，优先平昨
FUTURE_ARBITRAGE_CLOSE_TODAY_FIRST = 18    # 平，优先平今
# 委托类型 - 期货展期
FUTURE_RENEW_LONG_CLOSE_HISTORY_FIRST = 19   # 看多，优先平昨
FUTURE_RENEW_LONG_CLOSE_TODAY_FIRST = 20     # 看多，优先平今
FUTURE_RENEW_SHORT_CLOSE_HISTORY_FIRST = 21  # 看空，优先平昨
FUTURE_RENEW_SHORT_CLOSE_TODAY_FIRST = 22    # 看空，优先平今

# 委托类型 - 股票
STOCK_BUY = 23
STOCK_SELL = 24
# 委托类型 - 信用交易
CREDIT_BUY = 23                       # 担保品买入
CREDIT_SELL = 24                      # 担保品卖出
CREDIT_FIN_BUY = 27                   # 融资买入
CREDIT_SLO_SELL = 28                  # 融券卖出
CREDIT_BUY_SECU_REPAY = 29            # 买券还券
CREDIT_DIRECT_SECU_REPAY = 30         # 直接还券
CREDIT_SELL_SECU_REPAY = 31           # 卖券还款
CREDIT_DIRECT_CASH_REPAY = 32         # 直接还款
CREDIT_FIN_BUY_SPECIAL = 40           # 专项融资买入
CREDIT_SLO_SELL_SPECIAL = 41          # 专项融券卖出
CREDIT_BUY_SECU_REPAY_SPECIAL = 42    # 专项买券还券
CREDIT_DIRECT_SECU_REPAY_SPECIAL = 43  # 专项直接还券
CREDIT_SELL_SECU_REPAY_SPECIAL = 44   # 专项卖券还款
CREDIT_DIRECT_CASH_REPAY_SPECIAL = 45  # 专项直接还款

# 委托类型 - 股票期权
STOCK_OPTION_BUY_OPEN = 48       # 买入开仓
STOCK_OPTION_SELL_CLOSE = 49     # 卖出平仓
STOCK_OPTION_SELL_OPEN = 50      # 卖出开仓
STOCK_OPTION_BUY_CLOSE = 51      # 买入平仓
STOCK_OPTION_COVERED_OPEN = 52   # 备兑开仓
STOCK_OPTION_COVERED_CLOSE = 53  # 备兑平仓
STOCK_OPTION_CALL_EXERCISE = 54  # 认购行权
STOCK_OPTION_PUT_EXERCISE = 55   # 认沽行权
STOCK_OPTION_SECU_LOCK = 56      # 证券锁定
STOCK_OPTION_SECU_UNLOCK = 57    # 证券解锁

# 委托类型 - 期货期权
OPTION_FUTURE_OPTION_EXERCISE = 100  # 期货期权行权

# 报价类型（市价）
LATEST_PRICE = 5                        # 最新价
FIX_PRICE = 11                          # 指定价/限价
MARKET_SH_CONVERT_5_CANCEL = 42         # 最优五档即时成交剩余撤销[上交所][股票]
MARKET_SH_CONVERT_5_LIMIT = 43          # 最优五档即时成交剩转限价[上交所][股票]
MARKET_PEER_PRICE_FIRST = 44            # 对手方最优价格委托
MARKET_MINE_PRICE_FIRST = 45            # 本方最优价格委托
MARKET_SZ_INSTBUSI_RESTCANCEL = 46      # 即时成交剩余撤销委托[深交所][股票][期权]
MARKET_SZ_CONVERT_5_CANCEL = 47         # 最优五档即时成交剩余撤销[深交所][股票][期权]
MARKET_SZ_FULL_OR_CANCEL = 48           # 全额成交或撤销委托[深交所][股票][期权]

# 市场代码
SH_MARKET = 0
SZ_MARKET = 1

# 委托状态
ORDER_UNREPORTED = 48
ORDER_WAIT_REPORTING = 49
ORDER_REPORTED = 50
ORDER_REPORTED_CANCEL = 51
ORDER_PARTSUCC_CANCEL = 52
ORDER_PART_CANCEL = 53
ORDER_CANCELED = 54
ORDER_PART_SUCC = 55
ORDER_SUCCEEDED = 56
ORDER_JUNK = 57
ORDER_UNKNOWN = 255

# 账号状态
ACCOUNT_STATUS_INVALID = -1       # 无效
ACCOUNT_STATUS_OK = 0             # 正常
ACCOUNT_STATUS_WAITING_LOGIN = 1  # 连接中
ACCOUNT_STATUSING = 2             # 登陆中
ACCOUNT_STATUS_FAIL = 3           # 失败
ACCOUNT_STATUS_INITING = 4        # 初始化中
ACCOUNT_STATUS_CORRECTING = 5     # 数据刷新校正中
ACCOUNT_STATUS_CLOSED = 6         # 收盘后
ACCOUNT_STATUS_ASSIS_FAIL = 7     # 穿透副链接断开
ACCOUNT_STATUS_DISABLEBYSYS = 8   # 系统停用
ACCOUNT_STATUS_DISABLEBYUSER = 9  # 用户停用


class CompatObject:
    """Small attribute object matching xtquant's object-style returns."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        items = ", ".join("%s=%r" % (key, value) for key, value in sorted(self.__dict__.items()))
        return "%s(%s)" % (self.__class__.__name__, items)


class StockAccount:
    def __init__(self, account_id, account_type="STOCK"):
        self.account_id = str(account_id or "")
        self.account_type = str(account_type or "STOCK")


class XtQuantTraderCallback:
    def on_disconnected(self):
        pass

    def on_stock_order(self, order):
        pass

    def on_stock_trade(self, trade):
        pass

    def on_order_error(self, order_error):
        pass

    def on_cancel_error(self, cancel_error):
        pass

    def on_order_stock_async_response(self, response):
        pass

    def on_cancel_order_stock_async_response(self, response):
        pass

    def on_account_status(self, status):
        pass


def _env_int(name, default):
    value = os.environ.get(name)
    if value in (None, ""):
        return default
    return int(value)


def _env_float(name, default):
    value = os.environ.get(name)
    if value in (None, ""):
        return default
    return float(value)


def _env_bool(name, default=False):
    value = os.environ.get(name)
    if value in (None, ""):
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _bool_value(value, default=False):
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _import_optional_module(module_name):
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name == module_name:
            return None
        raise


def _quote_client_id():
    """Process-stable client id for whole-quote subscriptions. Config or env wins;
    otherwise read/create a persisted id so a restarted client is recognised as
    the same subscriber by the server."""
    client_config = load_client_config()
    configured = client_config.get("quote_client_id") or os.environ.get("BIGQMT_QUOTE_CLIENT_ID")
    if configured:
        return str(configured)
    cache_path = os.path.join(os.path.expanduser("~"), ".cache", "bigqmt", "quote_client_id")
    try:
        with open(cache_path, "r") as handle:
            existing = handle.read().strip()
            if existing:
                return existing
    except OSError:
        pass
    new_id = uuid.uuid4().hex
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w") as handle:
            handle.write(new_id)
    except OSError:
        pass
    return new_id


def _quote_push_zmq_address(client):
    """Derive the server whole-quote PUB address: same host as the RPC zmq
    endpoint, RPC port + 1 (the PUB socket binds a distinct port)."""
    from .transports.zmq_transport import DEFAULT_ZMQ_HOST, _default_zmq_port

    zmq_config = dict(getattr(client, "zmq_config", {}) or {})
    explicit = zmq_config.get("quote_push_connect_address")
    if explicit:
        return str(explicit)
    host = zmq_config.get("host") or DEFAULT_ZMQ_HOST
    port = zmq_config.get("port")
    base_port = int(port) if port is not None else _default_zmq_port(client.account_id)
    return "tcp://%s:%d" % (host, base_port + 1)


def load_client_config(module_name=None):
    """Load local private client config without requiring environment variables."""
    candidates = []
    selected = module_name or os.environ.get(CLIENT_CONFIG_MODULE_ENV)
    if selected:
        candidates.append(str(selected))
    candidates.extend(name for name in DEFAULT_CLIENT_CONFIG_MODULES if name not in candidates)

    for candidate in candidates:
        module = _import_optional_module(candidate)
        if module is None:
            continue
        redis_config = dict(getattr(module, "BIGQMT_REDIS_CONFIG", {}) or {})
        account_id = getattr(module, "BIGQMT_ACCOUNT_ID", None) or redis_config.get("account_id")
        timeout_seconds = getattr(module, "BIGQMT_RPC_TIMEOUT_SECONDS", None)
        if timeout_seconds is None:
            timeout_seconds = redis_config.get("rpc_timeout_seconds")
        full_tick_cache_config = dict(getattr(module, "BIGQMT_FULL_TICK_CACHE_CONFIG", {}) or {})
        for key in (
            "full_tick_cache_enabled",
            "full_tick_demand_ttl_seconds",
            "full_tick_cache_ttl_seconds",
            "full_tick_wait_seconds",
            "full_tick_poll_interval_seconds",
        ):
            if key in redis_config:
                full_tick_cache_config[key] = redis_config[key]
        local_cache_config = dict(getattr(module, "BIGQMT_LOCAL_CACHE_CONFIG", {}) or {})
        for key in ("local_cache_enabled", "local_cache_dir", "local_cache_fallback_rpc", "local_cache_format"):
            if key in redis_config:
                local_cache_config[key.replace("local_cache_", "")] = redis_config[key]
        formula_server_config = dict(getattr(module, "BIGQMT_FORMULA_SERVER_CONFIG", {}) or {})
        formula_server_config.update(dict(redis_config.get("formula_server") or {}))
        return {
            "module": candidate,
            "account_id": account_id,
            "redis_config": redis_config,
            "timeout_seconds": timeout_seconds,
            "full_tick_cache_config": full_tick_cache_config,
            "local_cache_config": local_cache_config,
            "formula_server_config": formula_server_config,
            "quote_client_id": getattr(module, "BIGQMT_QUOTE_CLIENT_ID", None),
        }
    return {}


def _account_id(account, fallback=""):
    if account is None:
        return str(fallback or "")
    if isinstance(account, str):
        return account
    for name in ("account_id", "m_strAccountID", "id"):
        value = getattr(account, name, None)
        if value:
            return str(value)
    if isinstance(account, dict):
        return str(account.get("account_id") or account.get("id") or fallback or "")
    return str(fallback or "")


def _action_to_order_type(action):
    text = str(action or "").upper()
    if text in ("BUY", str(STOCK_BUY)):
        return STOCK_BUY
    if text in ("SELL", str(STOCK_SELL)):
        return STOCK_SELL
    return 0


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, dict):
        return list(value.values())
    if isinstance(value, list):
        return value
    return [value]


def _restore_jsonable(value):
    if isinstance(value, dict):
        marker = value.get("__bigqmt_type__")
        if marker == "DataFrame":
            try:
                import pandas as pd

                return pd.DataFrame(value.get("records") or [], columns=value.get("columns") or None)
            except Exception:
                return value.get("records") or []
        if marker == "Series":
            try:
                import pandas as pd

                return pd.Series(value.get("data") or {})
            except Exception:
                return value.get("data") or {}
        return {key: _restore_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_restore_jsonable(item) for item in value]
    return value


def _normalize_code_for_filter(code):
    text = str(code or "").strip().upper()
    if "." not in text:
        return text
    return text.split(".", 1)[0]


def _is_hs_a_share(code):
    text = str(code or "").strip().upper()
    pure = _normalize_code_for_filter(text)
    if not (len(pure) == 6 and pure.isdigit()):
        return False
    if text.endswith(".SH"):
        return pure.startswith(("600", "601", "603", "605", "688", "689"))
    if text.endswith(".SZ"):
        return pure.startswith(("000", "001", "002", "003", "300", "301"))
    return pure.startswith(
        ("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688", "689")
    )


class BigQmtRpcClient:
    def __init__(
        self,
        account_id=None,
        redis_client=None,
        redis_config=None,
        timeout_seconds=None,
        transport=None,
    ):
        client_config = load_client_config()
        config_redis = dict(client_config.get("redis_config") or {})
        redis_config = dict(redis_config or {})
        merged_redis_config = dict(config_redis)
        merged_redis_config.update(redis_config)
        self.account_id = str(
            account_id
            or merged_redis_config.get("account_id")
            or client_config.get("account_id")
            or os.environ.get("BIGQMT_ACCOUNT_ID")
            or ""
        )
        self.redis_client = redis_client
        self.redis_config = {
            "host": merged_redis_config.get("host") or os.environ.get("BIGQMT_REDIS_HOST", "127.0.0.1"),
            "port": int(merged_redis_config.get("port") or _env_int("BIGQMT_REDIS_PORT", 6379)),
            "db": int(merged_redis_config.get("db") or _env_int("BIGQMT_REDIS_DB", 5)),
            "username": merged_redis_config.get("username", os.environ.get("BIGQMT_REDIS_USERNAME") or ""),
            "password": merged_redis_config.get("password", os.environ.get("BIGQMT_REDIS_PASSWORD") or ""),
        }
        config_timeout = client_config.get("timeout_seconds")
        self.timeout_seconds = float(
            timeout_seconds
            if timeout_seconds is not None
            else config_timeout
            if config_timeout is not None
            else _env_float("BIGQMT_RPC_TIMEOUT_SECONDS", 6.0)
        )
        full_tick_cache_config = dict(client_config.get("full_tick_cache_config") or {})
        self.full_tick_cache_config = {
            "enabled": _bool_value(
                full_tick_cache_config.get("enabled", full_tick_cache_config.get("full_tick_cache_enabled")),
                _env_bool("BIGQMT_FULL_TICK_CACHE_ENABLED", False),
            ),
            "demand_ttl_seconds": float(
                full_tick_cache_config.get("demand_ttl_seconds")
                or full_tick_cache_config.get("full_tick_demand_ttl_seconds")
                or _env_float("BIGQMT_FULL_TICK_DEMAND_TTL_SECONDS", 10.0)
            ),
            "cache_ttl_seconds": float(
                full_tick_cache_config.get("cache_ttl_seconds")
                or full_tick_cache_config.get("full_tick_cache_ttl_seconds")
                or _env_float("BIGQMT_FULL_TICK_CACHE_TTL_SECONDS", 10.0)
            ),
            "wait_seconds": float(
                full_tick_cache_config.get("wait_seconds")
                or full_tick_cache_config.get("full_tick_wait_seconds")
                or _env_float("BIGQMT_FULL_TICK_WAIT_SECONDS", 3.5)
            ),
            "poll_interval_seconds": float(
                full_tick_cache_config.get("poll_interval_seconds")
                or full_tick_cache_config.get("full_tick_poll_interval_seconds")
                or _env_float("BIGQMT_FULL_TICK_POLL_INTERVAL_SECONDS", 0.2)
            ),
        }
        # Client-side local market-data cache. download_history_data* pulls bars
        # over RPC once and persists them here; get_local_data then reads them with
        # no RPC. fallback_rpc=True lets get_local_data fetch+cache a cache miss.
        local_cache_config = dict(client_config.get("local_cache_config") or {})
        self.local_cache_config = {
            "enabled": _bool_value(
                local_cache_config.get("enabled", merged_redis_config.get("local_cache_enabled")),
                _env_bool("BIGQMT_LOCAL_CACHE_ENABLED", True),
            ),
            "dir": (
                local_cache_config.get("dir")
                or merged_redis_config.get("local_cache_dir")
                or os.environ.get("BIGQMT_LOCAL_CACHE_DIR")
                or None
            ),
            "fallback_rpc": _bool_value(
                local_cache_config.get("fallback_rpc", merged_redis_config.get("local_cache_fallback_rpc")),
                _env_bool("BIGQMT_LOCAL_CACHE_FALLBACK_RPC", False),
            ),
            "format": str(
                local_cache_config.get("format")
                or merged_redis_config.get("local_cache_format")
                or os.environ.get("BIGQMT_LOCAL_CACHE_FORMAT")
                or "auto"  # parquet if pyarrow is available, else pickle
            ),
        }
        # Transport selection. Default "redis" keeps the legacy call_redis_rpc
        # path (so existing client configs are unchanged). Setting transport to
        # "zmq"/"mysql"/"shm" (via config or constructor) routes calls through
        # the swappable transport layer instead.
        self.transport_name = str(
            transport
            or merged_redis_config.get("transport")
            or os.environ.get("BIGQMT_RPC_TRANSPORT")
            or "redis"
        ).lower()
        self.zmq_config = dict(merged_redis_config.get("zmq") or {})
        self.mysql_config = dict(merged_redis_config.get("mysql") or {})
        self._transport_instance = None  # lazily built by _transport()
        # FormulaServer read fast-path. QMT's C++ quote service (port 58600)
        # answers reference/history reads in ~0.07ms without touching the QMT
        # python thread. Enabled by default; every miss falls back to RPC, so a
        # client that cannot reach it just runs as before.
        formula_config = dict(
            client_config.get("formula_server_config")
            or merged_redis_config.get("formula_server")
            or {}
        )
        if "enabled" not in formula_config:
            formula_config["enabled"] = _env_bool("BIGQMT_FORMULA_ENABLED", True)
        self.formula_server_config = formula_config
        self._formula_router_instance = None  # lazily built by _formula_router()

    def _redis(self):
        if self.redis_client is None:
            import redis

            cfg = dict(self.redis_config)
            if not cfg.get("username"):
                cfg.pop("username", None)
            if not cfg.get("password"):
                cfg.pop("password", None)
            self.redis_client = redis.Redis(**cfg)
        return self.redis_client

    def _transport(self):
        if self._transport_instance is None:
            if self.transport_name in ("redis", "", "default"):
                # Legacy path: call_redis_rpc builds its own request envelope.
                return None
            from .transports.factory import build_transport

            client_config = load_client_config()
            config_redis = dict(client_config.get("redis_config") or {})
            zmq_config = dict(config_redis.get("zmq") or {})
            zmq_config.update(self.zmq_config)
            # ZMQ must work without Redis. Discovery is opt-in and unnecessary
            # when connect_address is explicitly configured.
            if (
                not zmq_config.get("connect_address")
                and bool(zmq_config.get("redis_discovery_enabled", False))
            ):
                zmq_config.setdefault("discovery_redis_client", self._redis())
            factory_config = {
                "zmq": zmq_config,
                "mysql": dict(config_redis.get("mysql") or {}, **self.mysql_config),
            }
            self._transport_instance = build_transport(
                self.transport_name,
                factory_config,
                account_id=self.account_id,
                print_prefix="[bigqmt_client]",
            )
        return self._transport_instance

    def _formula_router(self):
        """Lazily build the FormulaServer router. Never raises — a router that
        cannot be built simply means every read goes over RPC."""
        if self._formula_router_instance is None:
            try:
                from .formula_server import build_router

                self._formula_router_instance = build_router(
                    self.formula_server_config, print_prefix="[bigqmt_formula]"
                )
            except Exception as exc:
                print("[bigqmt_formula] disabled (%s: %s)" % (exc.__class__.__name__, exc))

                class _Disabled(object):
                    def supports(self, method):
                        return False

                self._formula_router_instance = _Disabled()
        return self._formula_router_instance

    def call(self, method, params=None, account_id=None, timeout_seconds=None):
        target_account = str(account_id or self.account_id or "")
        if not target_account:
            raise ValueError("Big QMT account_id is required")
        wait_seconds = self.timeout_seconds if timeout_seconds is None else timeout_seconds
        # Fast path: reference/history reads answered straight by QMT's
        # FormulaServer, bypassing the strategy process and its GIL. Anything it
        # declines (unmapped method, untranslatable params, server down) raises
        # Unroutable and drops through to the RPC bridge below.
        router = self._formula_router()
        if router.supports(method):
            from .formula_server import Unroutable

            try:
                return _restore_jsonable(router.call(method, params or {}))
            except Unroutable:
                pass
        transport = self._transport()
        if transport is not None:
            # Swappable transport path (zmq/mysql/...). Build the request
            # envelope the same way call_redis_rpc does.
            request = {
                "schema_version": 1,
                "request_id": uuid.uuid4().hex,
                "account_id": target_account,
                "method": method,
                "params": params or {},
                "ttl_seconds": 60,
            }
            response = transport.send_request(request, wait_seconds)
        else:
            response = call_redis_rpc(
                self._redis(),
                account_id=target_account,
                method=method,
                params=params or {},
                timeout_seconds=wait_seconds,
            )
        if not response.get("ok"):
            raise RuntimeError(response.get("error") or "Big QMT RPC failed: %s" % method)
        return _restore_jsonable(response.get("data"))

    def publish_event(self, event_type, payload, stream_template="bigqmt:quote_events:{account_id}"):
        account_id = str(self.account_id or "")
        event = {
            "event_type": str(event_type),
            "account_id": account_id,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "payload": payload or {},
        }
        raw = json.dumps(event, ensure_ascii=False, default=str)
        stream_key = stream_template.format(account_id=account_id)
        redis_client = self._redis()
        try:
            redis_client.xadd(stream_key, {"payload": raw}, maxlen=1000, approximate=True)
        except Exception:
            pass
        try:
            redis_client.publish(stream_key, raw)
        except Exception:
            pass
        return event

    def save_quote_subscription(self, seq, payload, active=True):
        account_id = str(self.account_id or "")
        key = "bigqmt:quote_subscriptions:%s" % account_id
        redis_client = self._redis()
        if active:
            value = json.dumps(payload or {}, ensure_ascii=False, default=str)
            try:
                redis_client.hset(key, str(seq), value)
            except Exception:
                pass
        else:
            try:
                redis_client.hdel(key, str(seq))
            except Exception:
                pass


class BigQmtXtData:
    def __init__(self, client):
        self.client = client
        self._subscribe_seq = int(time.time() * 1000)
        self._cache_obj = None
        self._quote_session = None          # lazily built WholeQuoteClientSession
        self._quote_session_factory = None  # test hook: returns a session-like object

    def _next_seq(self):
        self._subscribe_seq += 1
        return self._subscribe_seq

    def _local_cache(self):
        cfg = dict(getattr(self.client, "local_cache_config", {}) or {})
        if not _bool_value(cfg.get("enabled"), True):
            return None
        if self._cache_obj is None:
            self._cache_obj = LocalMarketCache(cache_dir=cfg.get("dir"), fmt=cfg.get("format", "auto"))
        return self._cache_obj

    def _call(self, method, **params):
        return self.client.call(method, params)

    def get_full_tick(self, code_list):
        codes = list(code_list or [])
        if not codes:
            return {}
        cache_config = dict(getattr(self.client, "full_tick_cache_config", {}) or {})
        if _bool_value(cache_config.get("enabled"), False):
            redis_client = self.client._redis()
            request_full_tick_cache(
                redis_client,
                self.client.account_id,
                codes,
                demand_ttl_seconds=cache_config.get("demand_ttl_seconds", 10),
                cache_ttl_seconds=cache_config.get("cache_ttl_seconds", 10),
            )
            data = wait_full_tick_cache(
                redis_client,
                self.client.account_id,
                codes,
                max_age_seconds=cache_config.get("cache_ttl_seconds", 10),
                wait_seconds=cache_config.get("wait_seconds", 3.5),
                poll_interval_seconds=cache_config.get("poll_interval_seconds", 0.2),
            )
            if data is not None:
                return data
            upper_codes = {str(code).strip().upper() for code in codes}
            if upper_codes & {"SH", "SZ", "BJ", "HK"}:
                # Whole-market snapshots must stay on the demand cache. A live RPC
                # here would ship ~50k rows on every miss, so surface the timeout.
                raise TimeoutError("full tick redis cache timeout: %s" % ",".join(str(code) for code in codes))
            # Symbol-list miss (cold start / expired snapshot): fall back to a live
            # RPC so the first call is ~ms instead of a hard wait_seconds stall.
            return self.client.call("get_full_tick", {"codes": codes}) or {}
        upper_codes = {str(code).strip().upper() for code in codes}
        timeout_seconds = 30 if upper_codes & {"SH", "SZ", "BJ", "HK"} else None
        return self.client.call("get_full_tick", {"codes": codes}, timeout_seconds=timeout_seconds) or {}

    def get_instrument_detail(self, stock_code):
        return self.client.call("get_instrument_detail", {"code": stock_code}) or {}

    def get_instrumentdetail(self, stock_code):
        return self.get_instrument_detail(stock_code)

    def get_instrument_type(self, stock_code, variety_list=None):
        return self._call("get_instrument_type", code=stock_code, variety_list=variety_list)

    def get_stock_list_in_sector(self, sector_name, real_timetag=-1):
        name = str(sector_name or "")
        try:
            return self._call("get_stock_list_in_sector", sector_name=sector_name, real_timetag=real_timetag) or []
        except Exception:
            pass
        if name in ("沪深A股", "沪深A股".encode("utf-8", errors="ignore").decode("utf-8", errors="ignore")):
            ticks = self.get_full_tick(["SH", "SZ"])
            return sorted(code for code in ticks.keys() if _is_hs_a_share(code))
        raise NotImplementedError("sector is not supported by BigQMT compat: %s" % sector_name)

    def get_market_data(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
    ):
        return self._call(
            "get_market_data",
            field_list=list(field_list or []),
            stock_list=list(stock_list or []),
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            fill_data=fill_data,
        )

    def get_market_data_ex(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
    ):
        # Live pull over RPC. Cache-through: whatever we fetch is written to the
        # local cache (keyed by dividend_type), so it stays the latest — important
        # for 前复权 (front-adjusted) data, whose history re-scales on each dividend.
        data = self._call(
            "get_market_data_ex",
            field_list=list(field_list or []),
            stock_list=list(stock_list or []),
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            fill_data=fill_data,
        )
        cache = self._local_cache()
        if cache is not None and isinstance(data, dict):
            for code, df in data.items():
                try:
                    cache.write(code, period, df, dividend_type=dividend_type)
                except Exception:
                    pass
        return data

    def get_local_data(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
        data_dir=None,
    ):
        """Read bars from the CLIENT-side local cache — no RPC to Big QMT.

        Populate the cache first with download_history_data2(...). Returns a dict
        {code: DataFrame}. A cache-missed code is omitted, unless
        local_cache_fallback_rpc is enabled (then it is fetched + cached).
        """
        codes = [str(c) for c in (stock_list or []) if str(c or "").strip()]
        cache = self._local_cache()
        if cache is None:
            # Cache disabled -> behave like a plain RPC local-data read.
            return self._call(
                "get_local_data",
                field_list=list(field_list or []),
                stock_list=codes,
                period=period,
                start_time=start_time,
                end_time=end_time,
                count=count,
                dividend_type=dividend_type,
                fill_data=fill_data,
                data_dir=data_dir,
            )
        fields = list(field_list or [])
        result = {}
        missing = []
        for code in codes:
            df = cache.read(code, period, start_time, end_time, count, dividend_type=dividend_type)
            if df is not None and getattr(df, "shape", (0,))[0] > 0:
                result[code] = self._select_fields(df, fields)
            else:
                missing.append(code)
        if missing and _bool_value(self.client.local_cache_config.get("fallback_rpc"), False):
            fetched = self._pull_and_cache(missing, period, start_time, end_time, count, dividend_type)
            for code in missing:
                df = fetched.get(code)
                if df is not None and getattr(df, "shape", (0,))[0] > 0:
                    result[code] = self._select_fields(df, fields)
        return result

    @staticmethod
    def _select_fields(df, fields):
        if not fields:
            return df
        try:
            keep = [c for c in df.columns if c in fields or c in _TIME_COL_NAMES]
            return df[keep] if keep else df
        except Exception:
            return df

    def _pull_and_cache(self, codes, period, start_time, end_time, count, dividend_type="none"):
        """Fetch codes over RPC (get_market_data_ex already caches them)."""
        data = self.get_market_data_ex(
            field_list=DEFAULT_DOWNLOAD_FIELDS,
            stock_list=list(codes),
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
        )
        out = {}
        for code in codes:
            df = data.get(code) if isinstance(data, dict) else None
            if df is not None and getattr(df, "shape", (0,))[0] > 0:
                out[code] = df
        return out

    def subscribe_quote(self, stock_code, period="1d", start_time="", end_time="", count=0, callback=None):
        seq = self._next_seq()
        payload = {
            "seq": seq,
            "stock_code": stock_code,
            "period": period,
            "start_time": start_time,
            "end_time": end_time,
            "count": count,
        }
        self.client.save_quote_subscription(seq, payload, active=True)
        self.client.publish_event("subscribe_quote", payload)
        if callback is not None:
            try:
                if str(period).lower() in ("tick", "full_tick"):
                    callback(self.get_full_tick([stock_code]))
                else:
                    callback(
                        self.get_market_data_ex(
                            stock_list=[stock_code],
                            period=period,
                            start_time=start_time,
                            end_time=end_time,
                            count=count,
                        )
                    )
            except Exception:
                pass
        return seq

    def subscribe_quote2(self, stock_code, period="1d", start_time="", end_time="", count=0, dividend_type=None, callback=None):
        return self.subscribe_quote(
            stock_code=stock_code,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            callback=callback,
        )

    def _whole_quote_session(self):
        if self._quote_session is None:
            if self._quote_session_factory is not None:
                self._quote_session = self._quote_session_factory()
            else:
                self._quote_session = self._build_quote_session()
        return self._quote_session

    def _build_quote_session(self):
        from .whole_quote_session import WholeQuoteClientSession

        client = self.client

        def rpc_call(method, params):
            return client.call(method, params)

        return WholeQuoteClientSession(
            rpc_call=rpc_call,
            push_channel=self._build_quote_push_channel(),
            client_id=_quote_client_id(),
            heartbeat_interval_seconds=_env_float("BIGQMT_QUOTE_HEARTBEAT_SECONDS", 3.0),
            sub_id_func=self._next_seq,
        )

    def _build_quote_push_channel(self):
        """Build the push-channel subscriber matching the RPC transport: redis
        deployments derive the channel locally; zmq deployments connect to the
        server PUB socket (host from zmq config, RPC port + 1)."""
        client = self.client
        from .quote_push_channel import RedisQuotePushChannel, ZmqQuotePushChannel

        transport_name = str(getattr(client, "transport_name", "redis") or "redis").lower()
        if transport_name in ("zmq",):
            address = _quote_push_zmq_address(client)
            return ZmqQuotePushChannel(connect_address=address)
        return RedisQuotePushChannel(client._redis(), account_id=client.account_id)

    def subscribe_whole_quote(self, code_list, callback=None):
        session = self._whole_quote_session()
        session.start()
        sub_id = session.subscribe_whole_quote(code_list, callback=callback)
        # The big-QMT whole-quote callback is incremental (changed symbols only),
        # so prime the callback once with a full get_full_tick snapshot.
        if callback is not None:
            try:
                callback(self.get_full_tick(code_list))
            except Exception:
                pass
        return sub_id

    def unsubscribe_quote(self, seq):
        # subscribe_whole_quote handles are owned by the push session; single-stock
        # subscribe_quote seqs still retire through the legacy redis-event path.
        session = self._quote_session
        if session is not None and session.has_subscription(seq):
            session.unsubscribe_quote(seq)
        else:
            payload = {"seq": seq}
            self.client.save_quote_subscription(seq, payload, active=False)
            self.client.publish_event("unsubscribe_quote", payload)
        return 0

    def run(self):
        while True:
            time.sleep(3600)

    def get_divid_factors(self, stock_code, start_time="", end_time=""):
        return self._call("get_divid_factors", stock_code=stock_code, start_time=start_time, end_time=end_time)

    def download_history_data2(self, stock_list, period, start_time="", end_time="", callback=None, incrementally=None, dividend_type="none", chunk_size=None):
        """Pull bars from Big QMT over RPC and cache them locally, in batches.

        Mirrors xtdata.download_history_data2: after this, get_local_data(..., the
        same dividend_type) reads the data locally with no further RPC. Each batch
        re-pulls live, so re-running keeps the cache latest — needed for 前复权
        (front-adjusted) data. ``callback`` (optional) is invoked once per stock with
        {finished, total, stockcode} — xtdata-style. Returns {finished, total}.
        """
        codes = [str(c) for c in (stock_list or []) if str(c or "").strip()]
        if not codes:
            return {"finished": 0, "total": 0}
        if self._local_cache() is None:
            raise RuntimeError("local cache is disabled (set local_cache_enabled=True to download)")
        total = len(codes)
        step = int(chunk_size or 300)
        if step <= 0:
            step = 300
        finished = 0
        for i in range(0, total, step):
            batch = codes[i:i + step]
            # get_market_data_ex is cache-through: it writes each code to the cache.
            self.get_market_data_ex(
                field_list=DEFAULT_DOWNLOAD_FIELDS,
                stock_list=batch,
                period=period,
                start_time=start_time,
                end_time=end_time,
                count=-1,
                dividend_type=dividend_type,
            )
            for code in batch:
                finished += 1
                if callback is not None:
                    try:
                        callback({"finished": finished, "total": total, "stockcode": code})
                    except Exception:
                        pass
        return {"finished": finished, "total": total}

    def download_history_data(self, stock_code, period, start_time="", end_time="", incrementally=None, dividend_type="none"):
        return self.download_history_data2([stock_code], period, start_time, end_time, dividend_type=dividend_type)

    def local_cache_stats(self):
        """Return (cached files, periods) for the client-side local cache."""
        cache = self._local_cache()
        return cache.stats() if cache is not None else (0, [])

    def get_trading_dates(self, market, start_time="", end_time="", count=-1):
        return self._call("get_trading_dates", market=market, start_time=start_time, end_time=end_time, count=count)

    def get_holidays(self):
        return self._call("get_holidays")

    def download_holiday_data(self, incrementally=True):
        return self._call("download_holiday_data", incrementally=incrementally)

    def get_ipo_info(self, start_time="", end_time=""):
        return self._call("get_ipo_info", start_time=start_time, end_time=end_time)

    def get_etf_info(self):
        return self._call("get_etf_info")

    def download_etf_info(self):
        return self._call("download_etf_info")

    def get_option_list(self, undl_code, dedate, opttype="", isavailavle=False):
        return self._call("get_option_list", undl_code=undl_code, dedate=dedate, opttype=opttype, isavailavle=isavailavle)

    def get_his_option_list(self, undl_code, dedate):
        return self._call("get_his_option_list", undl_code=undl_code, dedate=dedate)

    def get_his_option_list_batch(self, undl_code, start_time="", end_time=""):
        return self._call("get_his_option_list_batch", undl_code=undl_code, start_time=start_time, end_time=end_time)

    def get_financial_data(self, stock_list, table_list=None, start_time="", end_time="", report_type="report_time"):
        return self._call(
            "get_financial_data",
            stock_list=list(stock_list or []),
            table_list=list(table_list or []),
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
        )

    def download_financial_data(self, stock_list, table_list=None, start_time="", end_time="", incrementally=None):
        return self._call(
            "download_financial_data",
            stock_list=list(stock_list or []),
            table_list=list(table_list or []),
            start_time=start_time,
            end_time=end_time,
            incrementally=incrementally,
        )

    def download_financial_data2(self, stock_list, table_list=None, start_time="", end_time="", callback=None):
        result = self._call(
            "download_financial_data2",
            stock_list=list(stock_list or []),
            table_list=list(table_list or []),
            start_time=start_time,
            end_time=end_time,
        )
        if callback is not None:
            callback(result)
        return result

    def get_sector_list(self):
        return self._call("get_sector_list")

    def get_sector_info(self, sector_name=""):
        return self._call("get_sector_info", sector_name=sector_name)

    def get_markets(self):
        return self._call("get_markets")

    def get_market_last_trade_date(self, market):
        return self._call("get_market_last_trade_date", market=market)

    def call_formula(self, formula_name, stock_code, period, start_time="", end_time="", count=-1, dividend_type=None, extend_param=None):
        return self._call(
            "call_formula",
            formula_name=formula_name,
            stock_code=stock_code,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            extend_param=extend_param or {},
        )

    def subscribe_formula(self, formula_name, stock_code, period, start_time="", end_time="", count=-1, dividend_type=None, extend_param=None, callback=None):
        result = self._call(
            "subscribe_formula",
            formula_name=formula_name,
            stock_code=stock_code,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            extend_param=extend_param or {},
        )
        if callback is not None:
            callback(result)
        return result

    def unsubscribe_formula(self, request_id):
        return self._call("unsubscribe_formula", request_id=request_id)

    def get_formula_result(self, request_id, start_time="", end_time="", count=-1, timeout_second=-1):
        return self._call(
            "get_formula_result",
            request_id=request_id,
            start_time=start_time,
            end_time=end_time,
            count=count,
            timeout_second=timeout_second,
        )

    def gen_factor_index(self, data_name, formula_name, vars, sector_list, start_time="", end_time="", period="1d", dividend_type="none"):
        return self._call(
            "gen_factor_index",
            data_name=data_name,
            formula_name=formula_name,
            vars=vars,
            sector_list=list(sector_list or []),
            start_time=start_time,
            end_time=end_time,
            period=period,
            dividend_type=dividend_type,
        )

    # ------------------------------------------------------------------
    # 扩展行情/基本面方法（对应 ContextInfo 方法，走 RPC 白名单）。
    # 仅对最常用的显式声明签名；其余通过 __getattr__ 自动转发。
    # ------------------------------------------------------------------

    def get_longhubang(self, stock_list=None, start_time="", end_time="", count=-1):
        return self._call(
            "get_longhubang",
            stock_list=list(stock_list or []),
            start_time=start_time,
            end_time=end_time,
            count=count,
        )

    def get_top10_share_holder(self, stock_list, data_name, start_time, end_time, report_type="report_time"):
        return self._call(
            "get_top10_share_holder",
            stock_list=list(stock_list or []),
            data_name=data_name,
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
        )

    def get_holder_num(self, stock_list=None, start_time="", end_time="", report_type="report_time"):
        return self._call(
            "get_holder_num",
            stock_list=list(stock_list or []),
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
        )

    def get_turnover_rate(self, stock_code=None, start_time="19720101", end_time="22010101"):
        return self._call(
            "get_turnover_rate",
            stock_code=list(stock_code or []),
            start_time=start_time,
            end_time=end_time,
        )

    def get_industry(self, industry_name):
        return self._call("get_industry", industry_name=industry_name)

    def bsm_price(self, opt_type, target_price, strike_price, risk_free, sigma, days, dividend=0):
        return self._call(
            "bsm_price",
            opt_type=opt_type,
            target_price=target_price,
            strike_price=strike_price,
            risk_free=risk_free,
            sigma=sigma,
            days=days,
            dividend=dividend,
        )

    def bsm_iv(self, opt_type, target_price, strike_price, option_price, risk_free, days, dividend=0):
        return self._call(
            "bsm_iv",
            opt_type=opt_type,
            target_price=target_price,
            strike_price=strike_price,
            option_price=option_price,
            risk_free=risk_free,
            days=days,
            dividend=dividend,
        )

    def get_option_iv(self, opt_code):
        return self._call("get_option_iv", opt_code=opt_code)

    def get_option_detail_data(self, stockcode):
        return self._call("get_option_detail_data", stockcode=stockcode)

    def get_option_undl_data(self, undl_code_ref=""):
        return self._call("get_option_undl_data", undl_code_ref=undl_code_ref)

    def get_option_undl(self, opt_code):
        return self._call("get_option_undl", opt_code=opt_code)

    def get_raw_financial_data(self, field_list, stock_list, start_time, end_time, report_type="report_time", data_type="dict"):
        return self._call(
            "get_raw_financial_data",
            field_list=list(field_list or []),
            stock_list=list(stock_list or []),
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
            data_type=data_type,
        )

    def get_factor_data(self, field_list, stock_list, start_date, end_date):
        return self._call(
            "get_factor_data",
            field_list=list(field_list or []),
            stock_list=list(stock_list or []),
            start_date=start_date,
            end_date=end_date,
        )

    def get_north_finance_change(self, period):
        return self._call("get_north_finance_change", period=period)

    def get_hkt_statistics(self, stock_code):
        return self._call("get_hkt_statistics", stock_code=stock_code)

    def get_hkt_details(self, stock_code):
        return self._call("get_hkt_details", stock_code=stock_code)

    def create_sector(self, sector_name, stock_list):
        return self._call("create_sector", sector_name=sector_name, stock_list=list(stock_list or []))

    def get_stock_name(self, stock):
        return self._call("get_stock_name", stock=stock)

    def get_close_price(self, market, stock_code, real_timetag, period=86400000, divid_type=0):
        return self._call(
            "get_close_price",
            market=market,
            stock_code=stock_code,
            real_timetag=real_timetag,
            period=period,
            divid_type=divid_type,
        )

    def get_main_contract(self, code_market):
        return self._call("get_main_contract", code_market=code_market)

    def get_his_contract_list(self, market):
        return self._call("get_his_contract_list", market=market)

    def get_date_location(self, date):
        return self._call("get_date_location", date=date)

    def get_his_st_data(self, stock_code):
        return self._call("get_his_st_data", stock_code=stock_code)

    def get_his_index_data(self, stock_code):
        return self._call("get_his_index_data", stock_code=stock_code)

    def call_method(self, method, **params):
        """Generic escape hatch: call any RPC market-data method by name.

        Use this for ContextInfo methods that don't have an explicit wrapper
        above (e.g. ``xtdata.call_method("get_last_close", stock="000001.SZ")``,
        ``xtdata.call_method("get_float_caps", stockcode="000001.SZ")``). The
        full list of callable methods is in ``MARKET_DATA_METHODS``.
        """
        return self._call(method, **params)

    # ------------------------------------------------------------------
    # L2 行情（需 L2 权限 + 原生 xtdata SDK 行情服务）
    # ------------------------------------------------------------------

    def get_l2_quote(self, field_list=None, stock_code="", start_time="", end_time="", count=-1):
        return self._call("get_l2_quote", field_list=list(field_list or []),
                          stock_code=stock_code, start_time=start_time, end_time=end_time, count=count)

    def get_l2_order(self, field_list=None, stock_code="", start_time="", end_time="", count=-1):
        return self._call("get_l2_order", field_list=list(field_list or []),
                          stock_code=stock_code, start_time=start_time, end_time=end_time, count=count)

    def get_l2_transaction(self, field_list=None, stock_code="", start_time="", end_time="", count=-1):
        return self._call("get_l2_transaction", field_list=list(field_list or []),
                          stock_code=stock_code, start_time=start_time, end_time=end_time, count=count)

    # ------------------------------------------------------------------
    # 指数权重 / 交易日历 / 交易时段 / 可转债 / 品种判断
    # ------------------------------------------------------------------

    def get_index_weight(self, index_code):
        return self._call("get_index_weight", index_code=index_code)

    def get_trading_calendar(self, market, start_time="", end_time="", tradetimes=False):
        return self._call("get_trading_calendar", market=market, start_time=start_time,
                          end_time=end_time, tradetimes=tradetimes)

    def get_trade_times(self, stockcode):
        return self._call("get_trade_times", stockcode=stockcode)

    def get_cb_info(self, stockcode):
        return self._call("get_cb_info", stockcode=stockcode)

    def is_stock_type(self, stock, tag):
        return self._call("is_stock_type", stock=stock, tag=tag)

    # ------------------------------------------------------------------
    # 板块增删
    # ------------------------------------------------------------------

    def add_sector(self, sector_name, stock_list):
        return self._call("add_sector", sector_name=sector_name, stock_list=list(stock_list or []))

    def remove_sector(self, sector_name):
        return self._call("remove_sector", sector_name=sector_name)

    # ------------------------------------------------------------------
    # 时间戳转换（纯计算）
    # ------------------------------------------------------------------

    @staticmethod
    def datetime_to_timetag(datetime_str, format="%Y%m%d%H%M%S"):
        import datetime as _dt
        try:
            return int(_dt.datetime.strptime(str(datetime_str), format).timestamp() * 1000)
        except Exception:
            return 0

    @staticmethod
    def timetag_to_datetime(timetag, format):
        import datetime as _dt
        try:
            return _dt.datetime.fromtimestamp(int(timetag) / 1000.0).strftime(format)
        except Exception:
            return ""

    @staticmethod
    def timetagToDateTime(timetag, format):
        return BigQmtXtData.timetag_to_datetime(timetag, format)


class BigQmtXtTrader:
    def __init__(
        self,
        path=None,
        session_id=None,
        account_id=None,
        redis_client=None,
        redis_config=None,
        timeout_seconds=None,
    ):
        self.path = path
        self.session_id = session_id
        self.client = BigQmtRpcClient(
            account_id=account_id,
            redis_client=redis_client,
            redis_config=redis_config,
            timeout_seconds=timeout_seconds,
        )
        self.callback = None
        self._event_thread = None
        self._event_running = False

    def _cached_position_snapshot(self, account_id):
        key = "bigqmt:positions:%s" % str(account_id or self.client.account_id or "")
        try:
            raw = self.client._redis().get(key)
        except Exception:
            return {}
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw))
        except Exception:
            return {}

    def _cached_positions(self, account_id):
        snapshot = self._cached_position_snapshot(account_id)
        positions = snapshot.get("positions") if isinstance(snapshot, dict) else None
        if isinstance(positions, dict):
            return positions
        if isinstance(positions, list):
            return {str(item.get("stock_code") or idx): item for idx, item in enumerate(positions)}
        return {}

    def _cached_asset(self, account_id):
        snapshot = self._cached_position_snapshot(account_id)
        asset = snapshot.get("asset") if isinstance(snapshot, dict) else None
        return asset if isinstance(asset, dict) else {}

    def _redis_cache_enabled(self):
        return str(getattr(self.client, "transport_name", "redis") or "redis").lower() in (
            "redis",
            "",
            "default",
        )

    def register_callback(self, callback):
        self.callback = callback
        return 0

    def start(self):
        # Launch the real-time execution-event listener so a registered callback's
        # on_stock_order / on_stock_trade fire as soon as Big QMT pushes them.
        self._start_event_listener()
        return 0

    def connect(self):
        if self.client.account_id:
            self.client.call("ping")
        self._fire_account_status()
        return 0

    def subscribe(self, account):
        if not self.client.account_id:
            self.client.account_id = _account_id(account)
        # (Re)start the listener now that the account is known; the loop resubscribes
        # to the account's channels within ~1s if the account changed.
        self._start_event_listener()
        self._fire_account_status()
        return 0

    def stop(self):
        self._event_running = False
        thread = self._event_thread
        if thread is not None and thread.is_alive():
            thread.join(1.0)
        self._event_thread = None
        return 0

    def _start_event_listener(self):
        if self._event_thread is not None and self._event_thread.is_alive():
            return
        self._event_running = True
        self._event_thread = threading.Thread(
            target=self._event_loop, name="bigqmt-exec-events", daemon=True
        )
        self._event_thread.start()

    def _fire_account_status(self):
        """Fire on_account_status after connect/subscribe (MiniQMT parity).

        Big QMT has no per-strategy account-status push; we synthesize a
        CONNECTED status once the RPC link is up so client code that waits
        for on_account_status before trading keeps working.
        """
        callback = self.callback
        if callback is None:
            return
        try:
            callback.on_account_status(
                CompatObject(
                    account_id=str(self.client.account_id or ""),
                    account_type="STOCK",
                    status=1,  # ACCOUNT_STATUS_ONLINE (MiniQMT XtAccountStatus)
                )
            )
        except Exception:
            pass

    def _event_loop(self):
        from .exec_events import (
            order_channel,
            trade_channel,
            order_error_channel,
            cancel_error_channel,
        )

        while self._event_running:
            account_id = str(self.client.account_id or "")
            pubsub = None
            try:
                pubsub = self.client._redis().pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(
                    order_channel(account_id),
                    trade_channel(account_id),
                    order_error_channel(account_id),
                    cancel_error_channel(account_id),
                )
                while self._event_running:
                    if str(self.client.account_id or "") != account_id:
                        break  # account changed -> reconnect and resubscribe
                    message = pubsub.get_message(timeout=1.0)
                    if not message or message.get("type") != "message":
                        continue
                    self._dispatch_event(message.get("data"))
            except Exception:
                time.sleep(1.0)
            finally:
                try:
                    if pubsub is not None:
                        pubsub.close()
                except Exception:
                    pass

    def _dispatch_event(self, raw):
        callback = self.callback
        if callback is None:
            return
        try:
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
            event = json.loads(text)
        except Exception:
            return
        if not isinstance(event, dict):
            return
        account_id = str(event.get("account_id") or self.client.account_id or "")
        try:
            event_type = event.get("event_type")
            if event_type == "trade":
                callback.on_stock_trade(self._trade_from_dict(account_id, event))
            elif event_type == "order":
                callback.on_stock_order(self._order_from_dict(account_id, event))
            elif event_type == "order_error":
                callback.on_order_error(
                    CompatObject(
                        error_id=event.get("error_id"),
                        error_msg=event.get("error_msg") or "",
                        order_sys_id=event.get("order_sys_id") or "",
                        order_id=event.get("order_sys_id") or "",
                        stock_code=event.get("stock_code") or "",
                    )
                )
            elif event_type == "cancel_error":
                callback.on_cancel_error(
                    CompatObject(
                        error_id=event.get("error_id"),
                        error_msg=event.get("error_msg") or "",
                        order_sys_id=event.get("order_sys_id") or "",
                        order_id=event.get("order_sys_id") or "",
                        stock_code=event.get("stock_code") or "",
                    )
                )
        except Exception:
            pass

    def run_forever(self):
        while True:
            time.sleep(3600)

    def query_stock_asset(self, account):
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call("query_stock_asset", {"account_id": account_id}, account_id=account_id) or {}
        except Exception:
            if not self._redis_cache_enabled():
                raise
            data = self._cached_asset(account_id)
            if not data:
                raise
        if (
            self._redis_cache_enabled()
            and data.get("cash") is None
            and data.get("total_asset") is None
        ):
            data = self._cached_asset(account_id) or data
        cash = data.get("cash")
        total_asset = data.get("total_asset")
        frozen_cash = data.get("frozen_cash")
        market_value = data.get("market_value")
        if market_value is None and cash is not None and total_asset is not None:
            # total_asset = cash(available) + frozen_cash + market_value. Older
            # servers send neither frozen_cash nor market_value; deriving without
            # frozen_cash overstates market value by the frozen amount, so
            # subtract it whenever the server did report it.
            market_value = _safe_float(total_asset) - _safe_float(cash)
            if frozen_cash is not None:
                market_value -= _safe_float(frozen_cash)
        return CompatObject(
            account_id=account_id,
            cash=_safe_float(cash, 0.0) if cash is not None else None,
            available_cash=_safe_float(cash, 0.0) if cash is not None else None,
            # MiniQMT's XtAsset always exposes frozen_cash, so default to 0.0
            # rather than None: callers do arithmetic on it.
            frozen_cash=_safe_float(frozen_cash, 0.0) if frozen_cash is not None else 0.0,
            total_asset=_safe_float(total_asset, 0.0) if total_asset is not None else None,
            market_value=_safe_float(market_value, 0.0) if market_value is not None else 0.0,
        )

    def query_stock_positions(self, account):
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call("query_stock_positions", {"account_id": account_id}, account_id=account_id) or {}
        except Exception:
            if not self._redis_cache_enabled():
                raise
            data = self._cached_positions(account_id)
            if not data:
                raise
        positions = []
        for item in _as_list(data):
            stock_code = str(item.get("stock_code") or "")
            volume = _safe_int(item.get("volume"))
            available = _safe_int(item.get("available", item.get("can_use_volume")))
            cost = _safe_float(item.get("cost", item.get("avg_price")))
            positions.append(
                CompatObject(
                    account_id=account_id,
                    stock_code=stock_code,
                    stock_name=str(item.get("stock_name") or ""),
                    volume=volume,
                    can_use_volume=available,
                    enable_amount=available,
                    available_amount=available,
                    avg_price=cost,
                    price=cost,
                    open_price=cost,
                    cost_price=cost,
                    yesterday_volume=_safe_int(item.get("yesterday_volume"), volume),
                )
            )
        return positions

    def query_stock_position(self, account, stock_code):
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call(
                "query_stock_position",
                {"account_id": account_id, "stock_code": stock_code},
                account_id=account_id,
            )
        except Exception:
            if not self._redis_cache_enabled():
                raise
            normalized = str(stock_code or "").strip().upper()
            data = None
            for code, item in self._cached_positions(account_id).items():
                if str(code).upper() == normalized or str(code).split(".", 1)[0].upper() == normalized:
                    data = item
                    break
            if data is None:
                raise
        if not data:
            return None
        return [
            CompatObject(
                account_id=account_id,
                stock_code=str(item.get("stock_code") or ""),
                stock_name=str(item.get("stock_name") or ""),
                volume=_safe_int(item.get("volume")),
                can_use_volume=_safe_int(item.get("available", item.get("can_use_volume"))),
                enable_amount=_safe_int(item.get("available", item.get("can_use_volume"))),
                available_amount=_safe_int(item.get("available", item.get("can_use_volume"))),
                avg_price=_safe_float(item.get("cost", item.get("avg_price"))),
                price=_safe_float(item.get("cost", item.get("avg_price"))),
                open_price=_safe_float(item.get("cost", item.get("avg_price"))),
                cost_price=_safe_float(item.get("cost", item.get("avg_price"))),
                yesterday_volume=_safe_int(item.get("yesterday_volume"), _safe_int(item.get("volume"))),
            )
            for item in [data]
        ][0]

    def query_stock_orders(self, account, cancelable_only=False, strategy_name="bigqmt_signal_trader"):
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "query_stock_orders",
            {
                "account_id": account_id,
                "cancelable_only": bool(cancelable_only),
                "strategy_name": strategy_name,
            },
            account_id=account_id,
        ) or []
        return [self._order_from_dict(account_id, item) for item in _as_list(data)]

    def query_stock_order(self, account, order_id):
        order_id = str(order_id or "")
        for order in self.query_stock_orders(account, cancelable_only=False):
            if str(order.order_id) == order_id or str(order.order_sysid) == order_id:
                return order
        return None

    def query_stock_trades(self, account, strategy_name="bigqmt_signal_trader"):
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "query_stock_trades",
            {"account_id": account_id, "strategy_name": strategy_name},
            account_id=account_id,
        ) or []
        return [self._trade_from_dict(account_id, item) for item in _as_list(data)]

    def query_execution_snapshot(
        self,
        account,
        order_strategy_name="bigqmt_signal_trader",
        trade_strategy_name="",
    ):
        """Query orders and account-wide trades in one RPC round trip."""
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "query_execution_snapshot",
            {
                "account_id": account_id,
                "order_strategy_name": order_strategy_name,
                "trade_strategy_name": trade_strategy_name,
            },
            account_id=account_id,
        ) or {}
        result = dict(data) if isinstance(data, dict) else {}
        result["orders"] = [
            self._order_from_dict(account_id, item)
            for item in _as_list(result.get("orders"))
        ]
        result["trades"] = [
            self._trade_from_dict(account_id, item)
            for item in _as_list(result.get("trades"))
        ]
        return result

    def order_stock(
        self,
        account,
        stock_code,
        order_type,
        order_volume,
        price_type,
        price,
        strategy_name,
        order_remark,
    ):
        data = self.order_stock_result(
            account, stock_code, order_type, order_volume, price_type,
            price, strategy_name, order_remark,
        )
        return data.get("order_sys_id") or -1

    def order_stock_result(
        self, account, stock_code, order_type, order_volume, price_type,
        price, strategy_name, order_remark,
    ):
        account_id = _account_id(account, self.client.account_id)
        return self.client.call(
            "order_stock",
            {
                "account_id": account_id,
                "stock_code": stock_code,
                "order_type": order_type,
                "order_volume": order_volume,
                "price_type": price_type,
                "price": price,
                "strategy_name": strategy_name,
                "order_remark": order_remark,
            },
            account_id=account_id,
        ) or {}

    def order_stock_async(self, *args, **kwargs):
        # MiniQMT semantics: returns a seq; the result comes back through
        # on_order_stock_async_response(seq, order_error|None). Our RPC is
        # synchronous under the hood, so we fire the response callback
        # immediately with the seq and the submitted order.
        seq = self._next_async_seq()
        stock_code = str(kwargs.get("stock_code") or (args[1] if len(args) > 1 else ""))
        try:
            result = self.order_stock(*args, **kwargs)
        except Exception as exc:
            callback = self.callback
            if callback is not None:
                try:
                    callback.on_order_error(
                        CompatObject(
                            error_id=getattr(exc, "errno", 0),
                            error_msg=str(exc),
                            order_sys_id="",
                            order_id="",
                            stock_code=stock_code,
                        )
                    )
                except Exception:
                    pass
            return seq
        # MiniQMT: order_stock returns -1 when the order failed to submit.
        # NOTE: the server also pushes an order_error event for 废单 (via
        # exec_events), so a client may see this -1 error AND the server's
        # order_error — they carry different info (RPC submit failure vs QMT
        # rejection detail). We fire it only when the callback was registered,
        # keeping both signals available to the client.
        if isinstance(result, int) and result == -1:
            callback = self.callback
            if callback is not None:
                try:
                    callback.on_order_error(
                        CompatObject(
                            error_id=-1,
                            error_msg="order submit failed (order_stock returned -1)",
                            order_sys_id="",
                            order_id="",
                            stock_code=stock_code,
                        )
                    )
                except Exception:
                    pass
            return seq
        callback = self.callback
        if callback is not None:
            try:
                # Align with native XtOrderResponse: callback takes ONE arg
                # (response) carrying account_id/order_id/seq/error_msg.
                order_sys_id = str(result.get("order_sys_id") or result.get("order_sysid") or "") if isinstance(result, dict) else str(result)
                callback.on_order_stock_async_response(
                    CompatObject(
                        account_id=self.client.account_id,
                        seq=seq,
                        order_id=order_sys_id or str(result.get("user_order_id") or "") if isinstance(result, dict) else str(result),
                        order_sys_id=order_sys_id,
                        stock_code=stock_code,
                        strategy_name=str(kwargs.get("strategy_name") or (args[6] if len(args) > 6 else "")),
                        order_remark=str(kwargs.get("order_remark") or (args[7] if len(args) > 7 else "")),
                        error_msg="",
                    ),
                )
            except Exception:
                pass
        return seq

    def order_stock_batch(self, account, orders, batch_id=""):
        account_id = _account_id(account, self.client.account_id)
        payload = []
        for item in orders or []:
            entry = dict(item or {})
            entry.setdefault("account_id", account_id)
            payload.append(entry)
        params = {"account_id": account_id, "orders": payload}
        if batch_id:
            params["batch_id"] = str(batch_id)
        return self.client.call(
            "order_stock_batch",
            params,
            account_id=account_id,
        ) or []

    def cancel_order_stock_sysid(self, account, market, order_sysid):
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "cancel_order_stock_sysid",
            {
                "account_id": account_id,
                "market": market,
                "order_sysid": order_sysid,
            },
            account_id=account_id,
        ) or {}
        return bool(data.get("success", data))

    def cancel_order_stock(self, account, order_id):
        return self.cancel_order_stock_sysid(account, "", order_id)

    def unsubscribe(self, account):
        # MiniQMT xttrader.unsubscribe(account) — 取消账户订阅。
        # Big QMT RPC 模式下账户是被动响应，unsubscribe 为 no-op。
        return 0

    # ------------------------------------------------------------------
    # 账户 / 融资融券扩展查询
    # 这些在 MiniQMT 走 XtQuantServer RPC；Big QMT 经
    # get_trade_detail_data 查询，需相应账户权限（两融账户等）。
    # 无权限/上下文未绑定时服务端降级为 []。
    # ------------------------------------------------------------------

    def _query_account_list(self, account, method):
        account_id = _account_id(account, self.client.account_id)
        try:
            return self.client.call(method, {"account_id": account_id}, account_id=account_id) or []
        except Exception:
            return []

    def query_account_infos(self, account=None):
        return self._query_account_list(account, "query_account_infos")

    def query_account_status(self, account=None):
        return self._query_account_list(account, "query_account_status")

    def query_credit_detail(self, account):
        return self._query_account_list(account, "query_credit_detail")

    def query_stk_compacts(self, account):
        return self._query_account_list(account, "query_stk_compacts")

    def query_credit_subjects(self, account):
        return self._query_account_list(account, "query_credit_subjects")

    def query_credit_slo_code(self, account):
        return self._query_account_list(account, "query_credit_slo_code")

    def query_credit_assure(self, account):
        return self._query_account_list(account, "query_credit_assure")

    def query_appointment_info(self, account):
        return self._query_account_list(account, "query_appointment_info")

    def query_smt_secu_info(self, account):
        return self._query_account_list(account, "query_smt_secu_info")

    def query_smt_secu_rate(self, account, stock_code, max_term, fare_way, credit_type, trade_type):
        account_id = _account_id(account, self.client.account_id)
        try:
            return self.client.call(
                "query_smt_secu_rate",
                {"account_id": account_id, "stock_code": stock_code, "max_term": max_term,
                 "fare_way": fare_way, "credit_type": credit_type, "trade_type": trade_type},
                account_id=account_id,
            ) or []
        except Exception:
            return []

    def query_ipo_data(self, account=None):
        return self._query_account_list(account, "query_appointment_info")

    def query_new_purchase_limit(self, account):
        return {}

    # ------------------------------------------------------------------
    # async 变体：MiniQMT 的 *_async 方法返回 seq 后异步回调。
    # 在 RPC 模型里请求-响应本就是同步的，这里直接转发到同步实现并
    # 返回一个递增 seq，让旧代码 ``xt_trader.query_stock_positions_async(acc)``
    # 不报错（回调仍由 register_callback 注册的回调在事件来时触发）。
    # ------------------------------------------------------------------

    _async_seq = 0

    def _next_async_seq(self):
        BigQmtXtTrader._async_seq += 1
        return BigQmtXtTrader._async_seq

    def _async_query(self, sync_call, account, callback, *args, **kwargs):
        """Shared async query helper.

        MiniQMT's *_async query methods take a callback and hand the result to
        it (they return None). We accept an OPTIONAL callback for compat: when
        given, we call callback(result) synchronously (our RPC is already
        synchronous) and return None like MiniQMT; when omitted, we keep our
        seq-returning extension so existing callers don't break.
        """
        result = sync_call(account, *args, **kwargs)
        if callback is not None:
            try:
                callback(result)
            except Exception:
                pass
            return None
        return self._next_async_seq()

    def query_stock_asset_async(self, account, callback=None):
        return self._async_query(self.query_stock_asset, account, callback)

    def query_stock_positions_async(self, account, callback=None):
        return self._async_query(self.query_stock_positions, account, callback)

    def query_stock_orders_async(self, account, cancelable_only=False, callback=None):
        if callback is not None:
            result = self.query_stock_orders(account, cancelable_only)
            try:
                callback(result)
            except Exception:
                pass
            return None
        return self._next_async_seq()

    def query_stock_trades_async(self, account, callback=None):
        return self._async_query(self.query_stock_trades, account, callback)

    def query_account_infos_async(self, account=None, callback=None):
        if callback is not None:
            result = self.query_account_infos(account)
            try:
                callback(result)
            except Exception:
                pass
            return None
        return self._next_async_seq()

    def query_account_status_async(self, account=None, callback=None):
        if callback is not None:
            result = self.query_account_status(account)
            try:
                callback(result)
            except Exception:
                pass
            return None
        return self._next_async_seq()

    def query_credit_detail_async(self, account, callback=None):
        return self._async_query(self.query_credit_detail, account, callback)

    def query_stk_compacts_async(self, account, callback=None):
        return self._async_query(self.query_stk_compacts, account, callback)

    def query_credit_subjects_async(self, account, callback=None):
        return self._async_query(self.query_credit_subjects, account, callback)

    def query_credit_slo_code_async(self, account, callback=None):
        return self._async_query(self.query_credit_slo_code, account, callback)

    def query_credit_assure_async(self, account, callback=None):
        return self._async_query(self.query_credit_assure, account, callback)

    def query_ipo_data_async(self, account=None, callback=None):
        if callback is not None:
            result = self.query_ipo_data(account)
            try:
                callback(result)
            except Exception:
                pass
            return None
        return self._next_async_seq()

    def query_new_purchase_limit_async(self, account, callback=None):
        return self._async_query(self.query_new_purchase_limit, account, callback)

    def query_appointment_info_async(self, account, callback=None):
        return self._async_query(self.query_appointment_info, account, callback)

    def cancel_order_stock_async(self, account, order_id):
        # MiniQMT: returns seq, result comes back via on_cancel_order_stock_async_response.
        seq = self._next_async_seq()
        try:
            ok = self.cancel_order_stock(account, order_id)
        except Exception as exc:
            callback = self.callback
            if callback is not None:
                try:
                    callback.on_cancel_error(
                        CompatObject(
                            error_id=getattr(exc, "errno", 0),
                            error_msg=str(exc),
                            order_sys_id=str(order_id or ""),
                            stock_code="",
                        )
                    )
                except Exception:
                    pass
            return seq
        callback = self.callback
        if callback is not None:
            try:
                callback.on_cancel_order_stock_async_response(
                    CompatObject(
                        account_id=self.client.account_id,
                        seq=seq,
                        success=bool(ok),
                        order_sys_id=str(order_id or ""),
                        order_id=str(order_id or ""),
                    ),
                )
            except Exception:
                pass
        return seq

    def cancel_order_stock_sysid_async(self, account, market, order_sysid):
        seq = self._next_async_seq()
        try:
            ok = self.cancel_order_stock_sysid(account, market, order_sysid)
        except Exception as exc:
            callback = self.callback
            if callback is not None:
                try:
                    callback.on_cancel_error(
                        CompatObject(
                            error_id=getattr(exc, "errno", 0),
                            error_msg=str(exc),
                            order_sys_id=str(order_sysid or ""),
                            stock_code="",
                        )
                    )
                except Exception:
                    pass
            return seq
        callback = self.callback
        if callback is not None:
            try:
                callback.on_cancel_order_stock_async_response(
                    CompatObject(
                        account_id=self.client.account_id,
                        seq=seq,
                        success=bool(ok),
                        order_sys_id=str(order_sysid or ""),
                        order_id=str(order_sysid or ""),
                    ),
                )
            except Exception:
                pass
        return seq

    def set_relaxed_response_order_enabled(self, enabled=True):
        # 内部行为开关，RPC 模式下无意义，no-op。
        return 0

    def smt_appointment_async(self, account, stock_code, apt_days, apt_volume,
                              fare_ratio, sub_rare_ratio, fine_ratio, begin_date):
        # SMB/预约打新走独立通道，RPC 桥不支持；返回 -1 表示失败（对齐 MiniQMT
        # 语义：seq 为 -1 表示委托失败）。
        return -1

    def _order_from_dict(self, account_id, item):
        action = item.get("action")
        order_type = _action_to_order_type(action)
        order_sysid = str(item.get("order_sys_id") or item.get("order_sysid") or item.get("order_id") or "")
        return CompatObject(
            account_id=account_id,
            stock_code=str(item.get("stock_code") or ""),
            order_type=order_type,
            order_status=_safe_int(item.get("status", item.get("order_status")), ORDER_UNKNOWN),
            order_volume=_safe_int(item.get("volume", item.get("order_volume"))),
            traded_volume=_safe_int(item.get("traded_volume")),
            price=_safe_float(item.get("price")),
            order_sysid=order_sysid,
            order_id=order_sysid or str(item.get("user_order_id") or ""),
            strategy_name=str(item.get("strategy_name") or ""),
            order_remark=str(item.get("remark") or item.get("user_order_id") or ""),
        )

    def _trade_from_dict(self, account_id, item):
        action = item.get("action")
        order_type = _action_to_order_type(action)
        order_sysid = str(item.get("order_sys_id") or item.get("order_sysid") or "")
        trade_id = str(item.get("trade_id") or "")
        return CompatObject(
            account_id=account_id,
            stock_code=str(item.get("stock_code") or ""),
            order_type=order_type,
            order_sysid=order_sysid,
            order_id=order_sysid,
            trade_id=trade_id,
            traded_volume=_safe_int(item.get("volume", item.get("traded_volume"))),
            traded_price=_safe_float(item.get("price", item.get("traded_price"))),
            traded_at=str(item.get("traded_at") or ""),
            order_remark=str(item.get("user_order_id") or item.get("remark") or ""),
        )


XtQuantTrader = BigQmtXtTrader


_default_client = None
xt_trader = None
xtdata = None


def configure(account_id=None, redis_client=None, redis_config=None, timeout_seconds=None):
    global _default_client, xt_trader, xtdata
    _default_client = BigQmtRpcClient(
        account_id=account_id,
        redis_client=redis_client,
        redis_config=redis_config,
        timeout_seconds=timeout_seconds,
    )
    if xt_trader is None:
        xt_trader = BigQmtXtTrader(account_id=_default_client.account_id, redis_client=_default_client.redis_client)
    xt_trader.client = _default_client
    if xtdata is None:
        xtdata = BigQmtXtData(_default_client)
    else:
        xtdata.client = _default_client
    return xt_trader, xtdata


def get_default_client():
    global _default_client
    if _default_client is None:
        configure()
    return _default_client


configure()


__all__ = [
    "BigQmtRpcClient",
    "BigQmtXtData",
    "BigQmtXtTrader",
    "CompatObject",
    "StockAccount",
    "XtQuantTrader",
    "XtQuantTraderCallback",
    "configure",
    "get_default_client",
    "load_client_config",
    "xt_trader",
    "xtdata",
]

# 大 QMT Redis Queue RPC 说明

更新时间：2026-07-02

## 目标

在大 QMT 策略进程内启动一个 Redis RPC 服务，用来远程调用少量白名单方法。实盘默认使用 Redis list queue + QMT `run_time("adjust", ...)` 调度 drain；请求 payload 会做安全编码，避免大 QMT 内置 Redis 客户端读取包含股票代码的 JSON 时触发 `Sensitive Data Detected`。

- `ping`
- `get_ticks`
- `get_market_stream_status`
- `drain_market_stream`
- `get_instrument`
- `get_market_data` / `get_market_data_ex` / `get_local_data`
- `get_stock_list_in_sector` / `get_sector_list` / `get_sector_info`
- `get_divid_factors` / `download_history_data` / `download_history_data2`
- `get_trading_dates` / `get_holidays` / `download_holiday_data`
- `get_ipo_info` / `get_etf_info` / `get_option_list`
- `get_financial_data` / `download_financial_data`
- `call_formula` / `subscribe_formula` / `unsubscribe_formula` / `get_formula_result` / `gen_factor_index`
- `get_positions`
- `get_asset`
- `query_orders`
- `query_trades`
- `sync_positions`

下单、批量下单、撤单默认关闭；启用 `rpc_allow_order_methods=True` 后仍必须通过下面的节点账户校验，live 一律拒绝。

## 节点账户事实（ZBW-72）

`ping.account_environment` 为 `broker_sim`、`live` 或 JSON `null`（unknown）。环境来自节点操作者核验目标 QMT/柜台后的绑定，并在运行时与独立会话事实核对；这不是 SDK 自动识别模拟环境。

在服务端私有 `BIGQMT_REDIS_CONFIG` 提供以下输入，runtime 经现有 `configure` / strategy 装配传入 handler：

| 输入 | 契约 |
|---|---|
| `node_account_binding` | `{account_id: <已核验账号>, account_environment: "broker_sim" 或 "live"}`；handler 复制绑定，默认 `None` |
| `session_facts_reader` | 无参数的节点侧 callable，每次返回当前 `{account_id: <会话真实账号>, session_id: <会话代次字符串>}`；默认 `None` |

reader 必须读取独立的当前 QMT 会话事实，断开或无法读取时返回空值或抛错；会话代次必须在重连或账户切换时改变（即使切回同一账号）。它是节点装配接口，**不是一个假定存在的 QMT SDK 方法/字段**。本仓尚无经过实际节点验证的读取实现，因此默认关闭；禁止用配置账号、`_detect_account_id`、`AssetSnapshot.account_id` 回显、固定 session 字符串实现生产 reader。

handler 建立时核验绑定与会话账号、服务账号一致；每次 ping 和交易入口重新读取。事实缺失、身份冲突或会话代次变化会使本 handler 的绑定失效，之后恢复旧值也不能重新授权。节点必须重新核验并重新提供绑定、重建 handler，不能依靠重启和照搬旧私有配置恢复权限。撤单还核对 gateway 的实际目标账号。

RPC 参数、客户端 profile、端口/名称、STOCK 类型和写入开关不能提供或覆盖上述事实。批量下单先检查全部子项的账号，逐笔提交前再次核验会话；读查询和现有 RPC 方法集合保持原样。此变更收紧写入条件，不授权部署、真实模拟报单或实盘。

离线验证：`uv run --frozen --with pytest python -m pytest tests/bigqmt_signal_trader/test_node_account_facts.py -q`。合成 QMT API/节点事实驱动真实 handler、provider、转换、风控及提交 journal，覆盖股票固定价 DAY 路径、部分成交、撤单、对账和拒绝矩阵。合成正例仅证明协议实现；BWT 固定提交与客户端 guard/journal 联合接线由 ZBW-34 在本 PR 合入后完成，原固定版本缺字段拒绝用例仍须保留。

## MiniQMT 兼容方法名

RPC 服务端会把以下 MiniQMT 常用方法名映射到大 QMT 适配器：

| MiniQMT 方法名 | RPC 内部方法 | 说明 |
|---|---|---|
| `query_stock_asset` | `get_asset` | 查询账户资产 |
| `query_stock_positions` | `get_positions` | 查询全部持仓 |
| `query_stock_position` | `query_stock_position` | 查询单只持仓，按 `stock_code` 过滤 |
| `query_stock_orders` | `query_orders` | 查询委托；支持 `cancelable_only` 过滤 |
| `query_stock_trades` | `query_trades` | 查询成交 |
| `get_full_tick` | `get_ticks` | 默认直接 RPC 调用；可选开启 Redis 快照缓存降载 |
| `get_instrument_detail` / `get_instrumentdetail` | `get_instrument` | 查询合约详情 |
| `order_stock` / `order_stock_async` | `submit_order` | 买卖下单；默认关闭 |
| `cancel_order_stock` / `cancel_order_stock_sysid` | `cancel_order` | 撤单；默认关闭 |

`order_stock` 参数兼容 `stock_code`、`order_type`、`order_volume`、`price_type`、`price`、`strategy_name`、`order_remark`。其中 `order_type=23/STOCK_BUY` 映射为买入，`order_type=24/STOCK_SELL` 映射为卖出。

`price_type` 会透传到大 QMT `passorder()`，常用值包括 `11/FIX_PRICE`、`5/LATEST_PRICE`、`44/MARKET_PEER_PRICE_FIRST`、`43/MARKET_SH_CONVERT_5_LIMIT`、`47/MARKET_SZ_CONVERT_5_CANCEL`。

`get_full_tick/get_ticks` 的 `codes` 参数支持两种写法：传合约代码如 `["600000.SH", "000001.SZ"]` 查询指定标的；传市场代码如 `["SH", "SZ"]` 查询全市场全推快照。

注意：兼容层的 `xtdata.get_full_tick(codes)` 默认走 Redis RPC 现调大 QMT。若需要降低全市场行情的大 payload 压力，可在客户端和 QMT 本地配置里显式打开 `full_tick_cache_enabled=True` / `BIGQMT_FULL_TICK_CACHE_CONFIG["enabled"]=True`，改为 Redis 需求驱动快照。

## 全市场实时行情流

`get_full_tick(["SH", "SZ"])` 返回调用时刻的全市场最新快照，适合启动校验和缺口诊断，不能表达两次调用之间的全部变化。连续录制使用大 QMT 原生 `ContextInfo.subscribe_whole_quote(["SH", "SZ"], callback)`：

1. 策略 `init` 注册一次全推回调；策略主图周期可以保持 `1m`，无需改为 Tick。
2. SH、SZ 各自首次出现的全量回调标记为 bootstrap；后续回调标记为增量更新。
3. 每次策略启动产生新的 `stream_id`。原生 callback 获得 `callback_sequence`，并切成最多 `market_stream_batch_max_records` 条的有序 chunk；每个 chunk 获得递增 `sequence`，同时保留分片序号和 bootstrap 标记。
4. 外部录制器通过 `drain_market_stream` 按游标读取；`gap=true`、`dropped_batches>0` 或 `stream_id` 改变时必须记录缺口，不能把该区间标记为完整 Tick 数据。

开关仅配置在大 QMT 本地私有配置中：

```python
BIGQMT_REDIS_CONFIG = {
    "market_stream_enabled": True,
    "market_stream_markets": ("SH", "SZ"),
    "market_stream_max_batches": 20000,
    "market_stream_max_records": 1000000,
    "market_stream_batch_max_records": 1000,
}
```

状态请求：

```json
{"method": "get_market_stream_status", "params": {}}
```

游标读取：

```json
{
  "method": "drain_market_stream",
  "params": {
    "stream_id": "<current-stream-id>",
    "after_sequence": 0,
    "max_batches": 200,
    "max_records": 1000
  }
}
```

返回值包含 `earliest_sequence`、`latest_sequence`、`next_sequence`、`gap`、`has_more`、丢弃计数和回调 chunk `batches`。每个 chunk 包含 `callback_sequence`、`callback_part`、`callback_parts` 和 `is_bootstrap`。批次是游标读取，不会因为一次读取立即从缓冲区删除；缓冲区达到上限时淘汰最早批次并累计丢弃计数。

## 实现文件

- `src/bigqmt_signal_trader/redis_rpc.py`：RPC 协议、Redis queue 服务、外部客户端 helper。
- `src/bigqmt_signal_trader/market_stream.py`：全市场回调的有界顺序缓冲区和缺口元数据。
- `src/bigqmt_signal_trader/xtquant_compat.py`：MiniQMT 风格客户端兼容层。
- `src/xtquant/`：可选的 `xtquant` import shim，用于最终替换老 import。
- `src/bigqmt_signal_trader_strategy.py`：在 `init` 中启动 RPC；默认由 QMT `run_time("adjust", ...)` drain Redis queue，避免大 QMT 冻结自建后台线程。
- `src/bigqmt_signal_trader_redis_rpc_runtime.py`：大 QMT 策略入口，默认不消费交易信号，只启用 RPC 和持仓同步。
- `tests/bigqmt_signal_trader/test_redis_rpc.py`：RPC 单测。
- `tests/bigqmt_signal_trader/test_market_stream.py`：全市场回调缓冲区和游标 RPC 单测。

## 运行方式

把源码同步到 QMT 的 `python` 目录：

```powershell
$srcPkg = '<REPO_ROOT>\src\bigqmt_signal_trader'
$dstPkg = '<QMT_PYTHON_DIR>\bigqmt_signal_trader'
Get-ChildItem -LiteralPath $srcPkg -Force | ForEach-Object {
  Copy-Item -LiteralPath $_.FullName -Destination $dstPkg -Recurse -Force
}

Copy-Item -LiteralPath '<REPO_ROOT>\src\bigqmt_signal_trader_strategy.py' `
  -Destination '<QMT_PYTHON_DIR>\bigqmt_signal_trader_strategy.py' `
  -Force

Copy-Item -LiteralPath '<REPO_ROOT>\src\bigqmt_signal_trader_redis_rpc_runtime.py' `
  -Destination '<QMT_PYTHON_DIR>\bigqmt_signal_trader_redis_rpc_runtime.py' `
  -Force
```

QMT 本地私有配置文件：

```python
# <QMT_PYTHON_DIR>\bigqmt_signal_trader_local_config.py
# coding: utf-8

BIGQMT_ACCOUNT_ID = "你的资金账号"

BIGQMT_REDIS_CONFIG = {
    "host": "YOUR_REDIS_HOST",
    "port": 6379,
    "db": 5,
    "username": "",
    "password": "...",
    "rpc_allow_order_methods": False,
    "rpc_process_in_listener": True,
    "rpc_listener_methods": ("*",),
    "rpc_background_threads": False,
    "schedule_adjust": True,
    "schedule_adjust_interval": "500nMilliSecond",
    "full_tick_cache_enabled": False,
    "full_tick_demand_ttl_seconds": 10,
    "full_tick_cache_ttl_seconds": 10,
    "full_tick_refresh_interval_seconds": 3,
    "full_tick_max_requests": 8,
    "market_stream_enabled": True,
    "market_stream_markets": ("SH", "SZ"),
    "market_stream_max_batches": 20000,
    "market_stream_max_records": 1000000,
    "market_stream_batch_max_records": 1000,
}
```

这个文件含账号和 Redis 密码，只放 QMT 本地目录，不提交。

QMT 策略编辑器内容：

```python
#coding:gbk
import sys
import os
import importlib

_qmt_path = os.path.dirname(os.path.abspath(globals().get('__file__', '')))
if not _qmt_path:
    _qmt_path = 'D:/YOUR_QMT_PYTHON_DIR'
if _qmt_path not in sys.path:
    sys.path.insert(0, _qmt_path)

try:
    import bigqmt_signal_trader.redis_rpc as _redis_rpc
    _redis_rpc = importlib.reload(_redis_rpc)
except Exception:
    pass

try:
    import bigqmt_signal_trader_strategy as _strategy
    try:
        _strategy.reset_app()
    except Exception:
        pass
    _strategy = importlib.reload(_strategy)
except Exception:
    pass

import bigqmt_signal_trader_redis_rpc_runtime as _runtime
_runtime = importlib.reload(_runtime)

try:
    from bigqmt_signal_trader_local_config import BIGQMT_REDIS_CONFIG
    _runtime.configure_runtime_redis(BIGQMT_REDIS_CONFIG)
except Exception:
    pass

try:
    from bigqmt_signal_trader_local_config import BIGQMT_ACCOUNT_ID
    _runtime.configure_runtime_account(BIGQMT_ACCOUNT_ID)
except Exception:
    pass

try:
    _runtime.bind_runtime_api(
        passorder_func=passorder,
        cancel_func=cancel,
        get_trade_detail_data_func=get_trade_detail_data,
    )
except NameError:
    pass

init = _runtime.init
handlebar = _runtime.handlebar
adjust = _runtime.adjust
order_callback = _runtime.order_callback
deal_callback = _runtime.deal_callback
```

不要勾选“启动本地 python”。

## Redis 协议

### RPC 请求/响应

请求 channel：

```text
bigqmt:rpc:req:{account_id}
```

请求 payload：

```json
{
  "schema_version": 1,
  "request_id": "req-001",
  "account_id": "YOUR_ACCOUNT_ID",
  "method": "get_positions",
  "params": {},
  "reply_channel": "bigqmt:rpc:resp:YOUR_ACCOUNT_ID:req-001",
  "reply_key": "bigqmt:rpc:resp:YOUR_ACCOUNT_ID:req-001",
  "ttl_seconds": 60
}
```

响应会同时写入：

```text
bigqmt:rpc:resp:{account_id}:{request_id}
```

并 publish 到同名 channel。

响应格式：

```json
{
  "schema_version": 1,
  "request_id": "req-001",
  "account_id": "YOUR_ACCOUNT_ID",
  "method": "get_positions",
  "ok": true,
  "data": {},
  "error": "",
  "handled_at": "2026-07-01 10:30:00"
}
```

### 可选：get_full_tick 需求驱动缓存

默认情况下，`xtdata.get_full_tick(codes)` 直接走 RPC。只有显式打开 `full_tick_cache_enabled=True` / `BIGQMT_FULL_TICK_CACHE_CONFIG["enabled"]=True` 时，客户端才会写入需求：

```text
bigqmt:full_tick:demand:{account_id}
```

其中 hash field 是规范化代码集合的 request id，value 包含：

```json
{
  "request_id": "...",
  "codes": ["SH", "SZ"],
  "requested_at_ts": 1780000000.0,
  "expires_at_ts": 1780000010.0,
  "cache_ttl_seconds": 10
}
```

大 QMT 每轮刷新后写入快照：

```text
bigqmt:full_tick:cache:{account_id}:{request_id}
```

快照 Redis key 的 TTL 默认是 10 秒；客户端还会校验 `updated_at_ts`，超过 `cache_ttl_seconds` 的快照不会返回。第一次调用如果还没有快照，客户端默认最多等待 `3.5s` 等下一轮大 QMT 刷新；**个股列表**仍然没有新快照时回退一次 live RPC(`get_full_tick`)以避免冷启动硬停；**市场代码**(`SH/SZ/BJ/HK`)则抛出超时、不回退 live 拉全市场。

### 异步下载任务（download jobs）

`download_history_data` / `download_history_data2` 是耗时的长调用：如果走同步 RPC，服务端会在**策略线程**上一直下载，冻结整个 RPC pump（且客户端 6s 就超时崩）。因此这两个方法改为**异步分块任务**：客户端把任务写入 Redis 队列立即返回，大 QMT 的策略线程每个 tick 只下载 `download_job_chunk_size` 只（受 `download_job_max_wall_seconds` 墙钟预算约束），永不长时间阻塞。

Redis 布局（按账户）：

```text
bigqmt:download:queue:{account_id}          # 待处理 job_id 列表（RPUSH/LPOP）
bigqmt:download:job:{account_id}:{job_id}    # job JSON（含 state/done/total/error 进度）
bigqmt:download:current:{account_id}         # 当前正在处理的 job_id（串行，一次一个）
```

job 状态：`pending → running → done | failed`。历史 K 线下载到**大 QMT 机器**的本地库；客户端随后用 `get_local_data` / `get_market_data` 快读取回。

客户端用法：

```python
# 非阻塞：提交后轮询
job = xtdata.submit_download_history_data2(["600000.SH", "000001.SZ"], "1d")
status = xtdata.get_download_status(job["job_id"])   # {state, done, total, error}
status = xtdata.wait_download(job["job_id"])          # 阻塞轮询到 done/failed（仅客户端阻塞）

# 兼容：download_history_data2(...) 仍可直接调用 = 提交 + 等待（默认最多 1800s）；
# 超时会抛 TimeoutError（任务在服务端继续跑，可继续轮询）。大批量建议用 submit + 轮询。
```

服务端开关：`download_jobs_enabled`、`download_job_chunk_size`（默认 10，每 tick 最小下载块）、`download_job_max_wall_seconds`（默认 0.5s，每 tick 墙钟预算）、`download_job_ttl_seconds`（默认 3600）。

### 实时成交/委托回调推送（exec events）

大 QMT 的 `order_callback(ContextInfo, orderInfo)` / `deal_callback(ContextInfo, dealInfo)` 在策略进程内触发。服务端把 QMT 对象的 ThinkTrader `m_*` 字段规范化后 publish 到 Redis，客户端后台线程订阅并回调 —— 无需轮询即可**实时**拿到成交/委托。

Redis 频道（同名 stream，xadd + publish，供短时回放）：

```text
bigqmt:order_events:{account_id}
bigqmt:trade_events:{account_id}
```

成交事件字段（由 `deal_callback` 的 `m_*` 映射）：`stock_code`(`m_strInstrumentID`)、`trade_id`(`m_strTradeID`)、`order_sys_id`(`m_strOrderSysID`)、`volume`(`m_nVolume`)、`price`(`m_dPrice`)、`amount`(`m_dTradeAmount`)、`commission`(`m_dComssion`)、`direction`(`m_nDirection`) 及 `action`(尽力映射 BUY/SELL)、`traded_at`(`m_strTradeTime`)。委托事件类似（`m_nOrderStatus`→`status`、`m_nVolumeTotal`→`order_volume`、`m_nVolumeTraded`→`traded_volume`、`m_dLimitPrice`→`price`）。

客户端用法（MiniQMT 风格，回调实时触发）：

```python
class MyCallback(XtQuantTraderCallback):
    def on_stock_trade(self, trade):   # 成交实时回调
        print(trade.stock_code, trade.trade_id, trade.traded_volume, trade.traded_price)
    def on_stock_order(self, order):   # 委托状态实时回调
        print(order.stock_code, order.order_status, order.traded_volume)

xt_trader.register_callback(MyCallback())
xt_trader.start()          # 启动后台监听线程（订阅上面两个频道）
xt_trader.subscribe(acc)   # 账号确定后会自动重订阅到该账号频道
```

服务端开关：`exec_events_enabled`（默认 True）。`action` 由 `m_nDirection` 尽力映射（48/23→BUY，49/24→SELL），未知时为空但 `direction` 原值始终保留。

## 外部调用示例

```python
import sys
import redis

sys.path.insert(0, r"<REPO_ROOT>\src")

from bigqmt_signal_trader.redis_rpc import call_redis_rpc

r = redis.Redis(
    host="YOUR_REDIS_HOST",
    port=6379,
    db=5,
    username="",
    password="...",
)

response = call_redis_rpc(
    r,
    account_id="YOUR_ACCOUNT_ID",
    method="get_positions",
    params={},
    timeout_seconds=3,
)

print(response)
```

## 延迟模式

### 两档处理模型(重要)

同一进程只有一个 GIL,方法按处理线程分两档:

- **inline 档(后台接收线程直接处理)**:`ping`、行情类(`get_full_tick`/`get_market_data_ex`/
  `get_instrument_detail`)、`query_stock_asset`。中位数**亚毫秒**,但会撞上大 QMT 终端占 GIL
  的尾延迟(见下)。
- **deferred 档(推迟到主策略线程,经 adjust drain)**:所有走 `get_trade_detail_data` 的**交易
  查询**——持仓/委托/成交、信用/账户明细,以及下单/撤单。**原因**:`get_trade_detail_data` 在后台
  线程上返回空(账户实有持仓也查出 0),必须在 QMT 主线程上下文里跑。这些方法登记在
  `LISTENER_DEFERRED_METHODS`,由 `run_time("adjust", interval)` 每拍 `drain_pending()` 在主
  线程执行。(`get_asset` 例外,走另一个 QMT 调用,后台线程即可,保持 inline 低延迟。)

> `adjust` 不是 QMT 内置回调。QMT 只自动调 `init`/`handlebar`;`handlebar` 里 `return
> adjust(...)`,加上我们 `run_time("adjust", interval)` 注册的定时器,构成 RPC 队列的 drain 节奏。

### 尾延迟 = 大 QMT 终端占 GIL(不是本代码)

`gil_probe` 探针显示进程周期性被卡 ~490ms,但 `adjust_phase` 每段都 <50ms —— 即**尾延迟来自
QMT 终端自身的 C++ 主循环占着 GIL**,`setswitchinterval`/精简 adjust 都 preempt 不了。唯一根治
是把 serving 挪出该进程(sidecar 独立 GIL,见 `shm_transport.py` 预留)。

### schedule_adjust_interval 调这个数压尾延迟

`run_time` 间隔 = 后台线程拿到主线程 GIL 窗口的节奏源;间隔越小,inline 尾越低:

| interval | adjust 频率 | inline 尾(p90/max) | CPU | 说明 |
|---|---|---|---|---|
| `500nMilliSecond` | ~2.4/s | ~490 / 510ms | 极低 | 默认省电 |
| `200nMilliSecond` | 折中 | ~200ms 量级 | 中 | **推荐平衡点** |
| `100nMilliSecond` | ~2150/s(QMT 当"尽快跑"热循环) | ~92 / 108ms | 烧≈1 核 | 尾最低但费 CPU |

**deferred 交易查询恒定 ~1s**(实测 p50 1012~1013ms,与 interval 无关)—— 瓶颈是
`get_trade_detail_data` 自身的柜台查询开销,调 interval 无效。要低延迟拿持仓,走**客户端 redis
缓存**(position_sync 已在写)而非每次实时查。

### zmq 真机实测(同机 localhost)

- inline:`ping` p50 **0.4-0.5ms**;`get_market_data_ex` 因 handler 较重几乎必吃满一个尾窗口
  (500ms 档 p50≈495ms,100ms 档 p50≈96ms)。
- deferred:持仓/委托/成交 p50 **~1s**。
- 下单/撤单已在**实盘**验证:`order_stock` 挂单(status=50 已报)→ `query_stock_orders` 拿到
  sysid → `cancel_order_stock` 成功、无残留。

### 传输与后台线程

- **redis**(默认,跨机):`rpc_process_in_listener=True`。
- **zmq**(同机低延迟):只加 `transport="zmq"` 一行;非 redis 传输 `_build_rpc_service` 会自动开
  `background_threads`,端口按账号派生 `tcp://127.0.0.1:1556x`。
- 内置 Redis 客户端读取含股票代码的原始 JSON 会触发 `Sensitive Data Detected`;客户端 helper 默认
  对请求做安全编码。

## 安全约束

- 默认生产模式不在自建线程里调用 QMT API；QMT API 调用在 `adjust/handlebar` 中处理。
- 默认只读，远程下单关闭。
- 账号不匹配会拒绝请求。
- 响应写 Redis key 并设置 TTL，方便调用端超时后排查。

## 本地测试

```powershell
cd <REPO_ROOT>
python -B -m unittest discover -s tests\bigqmt_signal_trader
```

当前结果：

```text
Ran 68 tests
OK
```


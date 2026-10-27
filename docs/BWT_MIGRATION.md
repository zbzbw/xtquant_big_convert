# BWT converter migration (ZBW-74)

The candidate is `0.3.75+bwt.3`, based on upstream main as read on
2026-10-27. The latest upstream GitHub release is `v0.3.73`; main additionally
contains the 0.3.74 forming daily-bar fix and 0.3.75 MultiIndex serialization
fix. The previous BWT vendor package declared `0.2.0` and included changes
from both the fork's `bwt` and `main` branches. Package versions alone did not
describe those contents.

## Remaining local changes and their callers

Adapter paths below are under BWT `package/bwt/adapter/bigqmt`; script paths
are relative to the BWT repository.

| Previous patch | Decision | Current caller and behavior |
| --- | --- | --- |
| Trade costs and `TradeSnapshot` fields | Replace with upstream | `adapter/bigqmt/native/models.py` parses commission and amount; `nautilus/live_execution.py` consumes commission. Upstream preserves both fields, handles the two native commission spellings and extends the model. Existing calls use named arguments. |
| Native position/order/trade query failures | Reuse upstream queries; remove row skipping | BWT provider account snapshots and required duplicate checks need complete results. `list_positions`, `query_orders_strict` and `query_trades_strict` propagate row parsing failures, including a single invalid row among valid rows. `query_submission_identities_strict` inherits that behavior. Business RPC queries return the existing error response; required duplicate checks refuse native writes through the existing per-item error. Real empty native results remain successful. |
| Adjustable cadence logging | Retain | `scripts/deploy_bigqmt_embedded.py` explicitly suppresses normal summaries and configures the warning interval. Keep those settings alongside upstream scheduling, drain budgets and diagnostics. |
| Native whole-market callback bridge | Retain cursor recording; reuse upstream native subscription API | `native/provider.py`, `native/datafeed.py` and `recording/recorder.py` read callback batches. Upstream live quote push has no retained cursor, pagination or overflow report and cannot replace this recording input. |
| Local-only historical reads | Replace old argument-shape patch with a small upstream correction | `native/provider.py` requests `subscribe=False`. Preserve that explicit value for every supported argument shape, including the MiniQMT-style signature. Unsupported local-only reads fail instead of silently subscribing. Upstream supplies local data and forming daily-bar handling. |
| Callback splitting and pagination | Retain | The recorder drains complete bounded batches, retaining callback part numbers, bootstrap facts, sequence and overflow information. Push delivery alone does not provide these facts. |
| Sector filtering | Retain | The embedded preparation script currently supplies sector selection; explicit finite instruments take precedence and are passed to the native subscription API. The callback filter reports discarded records and rejects empty sectors. |
| Strategy-thread response conversion | Retain method deferral; remove `JSON_READY_METHODS` | Stream status/drains remain deferred from the listener. Upstream `to_jsonable` now handles conversion in the normal response path, including numpy values and typed payloads. The separate normalization bypass is removed. |
| BQZ1 response compression | Retain for existing callers | The current embedded script explicitly configures compression, and BWT's provider uses the matching converter ZMQ transport. Upstream typed-payload detection also runs after decompression. This does not select ZMQ for a new deployment. |
| Verified node account/session facts (ZBW-72) | Retain and apply to new order methods | `native/provider.py` and the Live consumer rely on truthful `ping.account_environment`. Missing, conflicting or changed node evidence leaves it unknown and refuses writes. Newly added raw `passorder` and batch cancellation obey the same broker_sim restriction; batch item accounts cannot escape the node account. |
| Exact recording subscription facts (ZBW-87) | Retain | `recording/recorder.py` checks schema v3 subscription/universe identities, activity and start time. Keep the existing identity representation for persisted consumers; this migration does not create a new identity format. |
| Named Pipe request cancellation (ZBW-175 counterexamples) | Correct upstream transport | A request larger than the pipe buffer can block in `WriteFile`. One monotonic deadline covers connection, write and read; the existing `CancelIoEx` entry cancels pending I/O. Stop waits for active client I/O to exit before closing its handles. Failed/cancelled writes propagate without reconnect/resend because delivery is uncertain. A later explicit call may reconnect after failure; a stopped instance rejects new calls and the existing client close path creates a fresh transport. Server workers handle cancelled/closed writes and release their handles. |

The converter buffer is a bounded in-memory source for recording. It neither
writes a Data Lake nor requires successful Data publication before it can run.
The persistent recording cache and its independent lifecycle remain owned by
the BWT recording work. Stopping the stream releases its own native subscription;
it does not stop the upstream quote-push service.

## Upstream changes to consume in ZBW-75

- Client and embedded code must use the same candidate. Update BWT's gitlink,
  version constraint (`>=0.2,<0.3` currently rejects the candidate) and lock file
  together after review. This PR changes none of those BWT files.
- Position queries now distinguish a real empty result from native exceptions.
  Missing volume, available quantity or yesterday quantity also raises instead
  of becoming zero. Unparsable security codes fail the complete query rather
  than producing a successful partial/empty result; this corrects upstream
  row skipping identified during review. Update incomplete synthetic fixtures; do not fill missing
  broker facts in production. Asset rows with unavailable values remain unknown.
- Upstream order and trade models add fields; BWT consumes their JSON by name.
  Account type reporting is separate from the verified simulation/live fact.
  The order switch remains false by default; enabling the switch alone does
  not establish a verified simulation session.
- RPC revision stays `20260920-market-stream-v3`, because the BWT stream output
  is retained. Package version and upstream fields are also available through
  the upstream deployment-reporting APIs.
- Converter native timestamps keep their external QMT meanings (including
  source Tick milliseconds and broker time text). Stream receive/start times
  remain Unix nanoseconds. BWT Adapter conversion to business UTC nanoseconds
  belongs to ZBW-167/75; no timestamp is relabeled as nanoseconds here.
- Upstream keeps Python `>=3.8`, requires pyzmq, bounds the optional Redis extra
  to `<8`, adds optional msgpack and includes the configuration CLI/single-file
  tooling. Embedded code uses upstream's QMT sandbox loading; a normal CPython
  import does not validate the target QMT interpreter.

## Package preparation

From the reviewed task branch, build once into an empty output directory:

```powershell
uv build --wheel --out-dir <new-output-directory>
uv venv --python 3.13 <new-client-environment>
uv pip install --python <new-client-environment>/Scripts/python.exe <wheel-path>
```

Run the client import from outside the source tree. Prepare embedded files from
the same wheel, including `bigqmt_signal_trader`, the strategy/runtime entry
modules and private local configuration. Reuse BWT's preparation entry after
ZBW-75 updates it. Never copy client `xtquant` shims over the QMT native SDK.
Keep `rpc_allow_order_methods=False` for recording. Use a new candidate suffix
for subsequent changed artifacts rather than replacing an existing version.

## Transport handoff to ZBW-174

Upstream `transports/pipe_transport.py` implements local Named Pipe requests.
`pipe_push_channel.py` carries execution and quote events; transport/channel
selection and runtime forwarding remain upstream implementations. The BWT
cursor handlers are transport-independent, so they can be exercised over Pipe
without redesigning recording. The `shm_transport.py` implementation remains an
explicit unsupported placeholder.

The `bwt.3` correction follows synthetic Windows counterexamples from ZBW-175.
It uses the existing synchronous transport and cancellation entry, with no BWT
request retry. Deadline cancellation continues until the request leaves native
I/O so cancellation between write/read calls cannot be lost. Tests collect
request outcomes and server worker failures, including blocked requests and
responses larger than 3 MiB. This is local transport validation; target QMT
loading and site adoption still belong to the downstream tasks.

ZBW-174 recommends Pipe requests with the existing bounded cursor reads after
reproducing slow-consumer blocking in direct push. ZBW-175 owns product
connection changes after ZBW-75 consumes the upgrade; ZBW-176 owns finite
target adoption. Existing offline ZMQ
results do not validate a new target transport. No live gateway is switched by
this migration; no orders are sent to an external account.

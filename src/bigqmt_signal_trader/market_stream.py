"""Thread-safe buffer for Big QMT whole-market quote callbacks."""

from collections import deque
import copy
import threading
import time
import uuid


MARKET_STREAM_SCHEMA_VERSION = 2


def _wall_time_ns():
    return int(time.time() * 1000000000)


class MarketStreamBuffer:
    """Retain ordered callback batches until an external recorder drains them."""

    def __init__(
        self,
        max_batches=20000,
        max_records=1000000,
        batch_max_records=None,
    ):
        if isinstance(max_batches, bool) or int(max_batches) <= 0:
            raise ValueError("max_batches must be positive")
        if isinstance(max_records, bool) or int(max_records) <= 0:
            raise ValueError("max_records must be positive")
        resolved_batch_max_records = (
            min(1000, int(max_records))
            if batch_max_records is None
            else batch_max_records
        )
        if (
            isinstance(resolved_batch_max_records, bool)
            or int(resolved_batch_max_records) <= 0
        ):
            raise ValueError("batch_max_records must be positive")
        if int(resolved_batch_max_records) > int(max_records):
            raise ValueError("batch_max_records must not exceed max_records")
        self.max_batches = int(max_batches)
        self.max_records = int(max_records)
        self.batch_max_records = int(resolved_batch_max_records)
        self.stream_id = uuid.uuid4().hex
        self.started_at_ns = _wall_time_ns()
        self._lock = threading.RLock()
        self._batches = deque()
        self._record_count = 0
        self._latest_sequence = 0
        self._latest_callback_sequence = 0
        self._dropped_batches = 0
        self._dropped_records = 0
        self._callback_errors = 0

    def append(self, records, received_at_ns=None, is_bootstrap=False):
        if not isinstance(records, dict):
            raise TypeError("market stream callback payload must be a mapping")
        if not isinstance(is_bootstrap, bool):
            raise TypeError("is_bootstrap must be a boolean")
        frozen = [
            (str(code), copy.deepcopy(value))
            for code, value in records.items()
            if str(code or "").strip()
        ]
        if not frozen:
            return None
        received = int(received_at_ns or _wall_time_ns())
        if received <= 0:
            raise ValueError("received_at_ns must be positive")
        with self._lock:
            self._latest_callback_sequence += 1
            callback_sequence = self._latest_callback_sequence
            callback_parts = (
                len(frozen) + self.batch_max_records - 1
            ) // self.batch_max_records
            for offset in range(0, len(frozen), self.batch_max_records):
                part_records = dict(frozen[offset:offset + self.batch_max_records])
                self._latest_sequence += 1
                batch = {
                    "sequence": self._latest_sequence,
                    "callback_sequence": callback_sequence,
                    "callback_part": offset // self.batch_max_records + 1,
                    "callback_parts": callback_parts,
                    "is_bootstrap": is_bootstrap,
                    "received_at_ns": received,
                    "record_count": len(part_records),
                    "records": part_records,
                }
                while self._batches and (
                    len(self._batches) >= self.max_batches
                    or self._record_count + len(part_records) > self.max_records
                ):
                    removed = self._batches.popleft()
                    removed_count = int(removed["record_count"])
                    self._record_count -= removed_count
                    self._dropped_batches += 1
                    self._dropped_records += removed_count
                self._batches.append(batch)
                self._record_count += len(part_records)
            return self._latest_sequence

    def record_callback_error(self):
        with self._lock:
            self._callback_errors += 1

    def status(self):
        with self._lock:
            earliest = (
                int(self._batches[0]["sequence"])
                if self._batches
                else self._latest_sequence + 1
            )
            return {
                "schema_version": MARKET_STREAM_SCHEMA_VERSION,
                "stream_id": self.stream_id,
                "started_at_ns": self.started_at_ns,
                "earliest_sequence": earliest,
                "latest_sequence": self._latest_sequence,
                "latest_callback_sequence": self._latest_callback_sequence,
                "buffered_batches": len(self._batches),
                "buffered_records": self._record_count,
                "dropped_batches": self._dropped_batches,
                "dropped_records": self._dropped_records,
                "callback_errors": self._callback_errors,
                "max_batches": self.max_batches,
                "max_records": self.max_records,
                "batch_max_records": self.batch_max_records,
            }

    def drain(self, after_sequence=0, max_batches=200, max_records=100000):
        after = int(after_sequence or 0)
        batch_limit = int(max_batches)
        record_limit = int(max_records)
        if after < 0:
            raise ValueError("after_sequence must not be negative")
        if batch_limit <= 0:
            raise ValueError("max_batches must be positive")
        if record_limit <= 0:
            raise ValueError("max_records must be positive")
        with self._lock:
            status = self.status()
            earliest = int(status["earliest_sequence"])
            gap = after < earliest - 1
            selected = []
            selected_records = 0
            for batch in self._batches:
                sequence = int(batch["sequence"])
                if sequence <= after:
                    continue
                batch_records = int(batch["record_count"])
                if selected and (
                    len(selected) >= batch_limit
                    or selected_records + batch_records > record_limit
                ):
                    break
                selected.append(copy.deepcopy(batch))
                selected_records += batch_records
                if len(selected) >= batch_limit:
                    break
            next_sequence = (
                int(selected[-1]["sequence"])
                if selected
                else after
            )
            latest = int(status["latest_sequence"])
            return {
                **status,
                "after_sequence": after,
                "next_sequence": next_sequence,
                "gap": gap,
                "has_more": next_sequence < latest,
                "returned_batches": len(selected),
                "returned_records": selected_records,
                "batches": selected,
            }


__all__ = ["MARKET_STREAM_SCHEMA_VERSION", "MarketStreamBuffer"]

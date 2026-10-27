# coding: utf-8
"""#236 follow-up: the four live defects and their fixes, end to end on a
real named pipe (Windows only).

  1. deferred methods never answered -- inline answers and adjust-thread
     answers now serialize per connection; frames no longer interleave.
  2. the request deadline covers connection, blocking writes and reads.
  3. stop() cancels pending I/O without reconnecting or resending.
  4. after a connection failure a later explicit call can connect again.
"""
import json
import os
import sys
import threading
import time
import unittest
from concurrent.futures import Future
from unittest.mock import patch


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.transports.base import TransportError, TransportTimeout
from bigqmt_signal_trader.transports.pipe_transport import NamedPipeTransport

WINDOWS = os.name == "nt"


def _transport_pair(pipe_name):
    server = NamedPipeTransport(pipe_name=pipe_name, print_prefix="[test-srv]")
    client = NamedPipeTransport(pipe_name=pipe_name, print_prefix="[test-cli]")
    return server, client


@unittest.skipUnless(WINDOWS, "named pipes are Windows-only")
class RoundTripTest(unittest.TestCase):
    def setUp(self):
        self.pipe = "bigqmt-test-%d-%d" % (os.getpid(), int(time.time() * 1000) % 100000)
        self.server, self.client = _transport_pair(self.pipe)
        self.servers = []
        self.server_threads = []
        self.server_errors = []
        self.requests = []
        self._collect_server(self.server)

    def _collect_server(self, server):
        self.servers.append(server)
        serve = server._serve_connection

        def collected(handle):
            self.server_threads.append(threading.current_thread())
            try:
                serve(handle)
            except BaseException as exc:
                self.server_errors.append(exc)

        server._serve_connection = collected

    def _request_thread(self, request, timeout):
        result = Future()

        def call():
            try:
                result.set_result(self.client.send_request(request, timeout))
            except BaseException as exc:
                result.set_exception(exc)

        thread = threading.Thread(target=call)
        self.requests.append((thread, result))
        thread.start()
        return thread, result

    def tearDown(self):
        self.client.stop()
        for server in self.servers:
            server.stop()
            if server._listener is not None:
                server._listener.join(2)
                self.assertFalse(server._listener.is_alive())
            self.assertEqual(server._server_handles, [])
        for thread in self.server_threads:
            thread.join(2)
            self.assertFalse(thread.is_alive())
        for thread, result in self.requests:
            thread.join(2)
            self.assertFalse(thread.is_alive())
            # Exceptions are asserted by each test; retrieve all results even
            # when an earlier assertion failed and cleanup had to unblock I/O.
            if result.exception() is None:
                result.result()
        self.assertEqual(self.server_errors, [])
        self.assertEqual(self.client._client_threads, {})

    def test_inline_round_trip(self):
        self.server.start_receiving(lambda req: {
            "request_id": req.get("request_id"), "ok": True, "echo": req.get("method")})

        answer = self.client.send_request(
            {"request_id": "r1", "method": "ping"}, timeout_seconds=5.0)

        self.assertEqual(answer, {"request_id": "r1", "ok": True, "echo": "ping"})

    def test_an_answer_written_on_another_thread_arrives(self):
        """The deferred shape: the request is received on the connection
        worker, parked, and answered from a DIFFERENT thread (the adjust
        thread in production). Before the per-connection write lock, this
        raced with inline answers and corrupted frames."""
        parked = threading.Event()
        released = threading.Event()

        def deferred_handler(req):
            parked.set()
            released.wait(5.0)
            return {"request_id": req.get("request_id"), "ok": True,
                    "echo": req.get("method"), "thread": threading.current_thread().name}

        self.server.start_receiving(deferred_handler)
        releaser = threading.Timer(0.2, released.set)
        releaser.start()

        answer = self.client.send_request(
            {"request_id": "r-deferred", "method": "get_asset"}, timeout_seconds=5.0)

        self.assertTrue(parked.is_set())
        self.assertEqual(answer["request_id"], "r-deferred")
        self.assertEqual(answer["echo"], "get_asset")
        releaser.cancel()

    def test_the_timeout_is_real(self):
        """Defect 3: a silent server must not hang the client forever."""
        self.server.start_receiving(lambda req: None)

        started = time.time()
        with self.assertRaises(TransportTimeout):
            self.client.send_request({"request_id": "r-hang", "method": "ping"},
                                     timeout_seconds=1.0)
        elapsed = time.time() - started

        self.assertLess(elapsed, 5.0, "the client waited %.1fs -- the timeout is decorative" % elapsed)

    def test_concurrent_writes_from_two_threads_keep_frames_intact(self):
        """Many responses racing on ONE connection from two threads (inline
        worker + adjust): every frame must arrive as one parseable message."""
        requests = []
        done_setup = threading.Event()

        def handler(req):
            requests.append(req.get("request_id"))
            done_setup.wait(1.0)
            return {"request_id": req.get("request_id"), "ok": True, "n": req.get("n")}

        self.server.start_receiving(handler)
        done_setup.set()

        errors = []
        def call(n):
            try:
                answer = self.client.send_request(
                    {"request_id": "req-%d" % n, "method": "work", "n": n},
                    timeout_seconds=10.0)
                if answer.get("n") != n:
                    errors.append("mismatched answer %r for %d" % (answer, n))
            except Exception as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=call, args=(n,)) for n in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15.0)
            self.assertFalse(thread.is_alive())

        self.assertEqual(errors, [])

    def test_stop_does_not_hang(self):
        """stop() with a request blocked in ReadFile must return promptly."""
        self.server.start_receiving(lambda req: None)
        # Collect the blocked caller's result in this test's thread.
        errors = []
        def call():
            try:
                self.client.send_request(
                    {"request_id": "x", "method": "ping"}, timeout_seconds=10.0)
            except Exception as exc:
                errors.append(exc)
        hanging = threading.Thread(target=call)
        hanging.start()
        time.sleep(0.5)

        started = time.time()
        self.client.stop()
        self.server.stop()
        elapsed = time.time() - started
        hanging.join(timeout=2)

        self.assertLess(elapsed, 6.0, "stop() took %.1fs" % elapsed)
        self.assertFalse(hanging.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], (TransportError, TransportTimeout))

    def test_a_restarted_server_takes_clients_again(self):
        """Surface the failed call; only a later explicit call reconnects."""
        self.server.start_receiving(lambda req: {"request_id": req.get("request_id"), "ok": True})
        self.client.send_request({"request_id": "a", "method": "ping"}, timeout_seconds=5.0)
        self.server.stop()

        replacement, _client2 = _transport_pair(self.pipe)
        self._collect_server(replacement)
        try:
            replacement.start_receiving(lambda req: {
                "request_id": req.get("request_id"), "ok": True, "echo": req.get("method")})
            with self.assertRaises(TransportError):
                self.client.send_request(
                    {"request_id": "b1", "method": "ping"}, timeout_seconds=5.0)
            answer = self.client.send_request(
                {"request_id": "b2", "method": "ping"}, timeout_seconds=5.0)
            self.assertEqual(answer["echo"], "ping")
        finally:
            replacement.stop()

    def test_large_bidirectional_message(self):
        payload = "x" * (3 * 1024 * 1024 + 19)
        self.server.start_receiving(lambda req: {"data": req["data"]})
        answer = self.client.send_request({"data": payload}, 2)
        self.assertEqual(answer["data"], payload)

    def test_timeout_cancels_blocked_write(self):
        self.server.start_receiving(lambda req: {}, background_threads=False)
        started = time.monotonic()
        thread, result = self._request_thread({"data": "x" * (3 * 1024 * 1024)}, 0.2)
        thread.join(1)
        self.assertFalse(thread.is_alive(), "WriteFile exceeded the request deadline")
        with self.assertRaises(TransportTimeout):
            result.result()
        self.assertLess(time.monotonic() - started, 1)

    def test_stop_cancels_blocked_write_without_resending(self):
        self.server.start_receiving(lambda req: {}, background_threads=False)
        original_write = self.client._write
        writing = threading.Event()

        def write(handle, payload):
            writing.set()
            return original_write(handle, payload)

        with patch.object(self.client, "_write", side_effect=write) as writes:
            thread, result = self._request_thread({"data": "x" * (3 * 1024 * 1024)}, 30)
            self.assertTrue(writing.wait(1))
            # The request has entered native WriteFile and cannot fit in the
            # unread pipe buffer. stop(), rather than the deadline, ends it.
            time.sleep(0.05)
            self.assertTrue(thread.is_alive())
            started = time.monotonic()
            self.client.stop()
            thread.join(1)
            self.assertFalse(thread.is_alive())
            self.assertLess(time.monotonic() - started, 1)
            with self.assertRaises(TransportError):
                result.result()
            self.assertEqual(writes.call_count, 1)
            with self.assertRaises(TransportError):
                self.client.send_request({"data": "after-stop"}, 1)
            self.assertEqual(writes.call_count, 1)

    def test_uncertain_write_is_not_resent(self):
        received = []
        arrived = threading.Event()

        def handle(req):
            received.append(req["request_id"])
            arrived.set()
            return {"ok": True}

        self.server.start_receiving(handle)
        write = self.client._write

        def delivered_then_failed(handle, payload):
            write(handle, payload)
            if not arrived.wait(1):
                raise AssertionError("server did not receive the first write")
            raise TransportError("synthetic uncertain write result")

        with patch.object(self.client, "_write", side_effect=delivered_then_failed) as writes:
            with self.assertRaisesRegex(TransportError, "uncertain"):
                self.client.send_request({"request_id": "once"}, 2)
            self.assertEqual(writes.call_count, 1)
        self.assertEqual(received, ["once"])
        self.assertTrue(self.client.send_request({"request_id": "explicit-next"}, 2)["ok"])
        self.assertEqual(received, ["once", "explicit-next"])

    def test_stop_cancels_blocked_response_without_thread_error(self):
        writing = threading.Event()
        write = self.server._write

        def response_write(handle, payload):
            writing.set()
            return write(handle, payload)

        self.server.start_receiving(lambda req: {"data": "x" * (3 * 1024 * 1024)})
        with patch.object(self.server, "_write", side_effect=response_write):
            self.client._write(self.client._client_handle(), b'{"request_id":"slow-response"}')
            self.assertTrue(writing.wait(1))
            time.sleep(0.05)
            started = time.monotonic()
            self.server.stop()
            for thread in self.server_threads:
                thread.join(1)
                self.assertFalse(thread.is_alive())
            self.assertLess(time.monotonic() - started, 1)
        self.assertEqual(self.server_errors, [])

    def test_connection_wait_uses_request_deadline(self):
        # No server: the default 5-second connection wait must fit this call.
        started = time.monotonic()
        with self.assertRaises(TransportTimeout):
            self.client.send_request({"request_id": "no-server"}, 0.2)
        self.assertLess(time.monotonic() - started, 1)

    def test_deadline_cancellation_between_native_calls_is_not_lost(self):
        self.server.start_receiving(lambda req: None)
        read = self.client._read

        def delayed_read(handle):
            # The first cancellation sees no pending I/O. A later ReadFile
            # must still be cancelled, rather than hanging after the deadline.
            time.sleep(0.3)
            return read(handle)

        with patch.object(self.client, "_read", side_effect=delayed_read):
            thread, result = self._request_thread({"request_id": "between-io"}, 0.1)
            thread.join(1)
            self.assertFalse(thread.is_alive())
            with self.assertRaises(TransportTimeout):
                result.result()

    def test_stop_cancels_connection_wait(self):
        thread, result = self._request_thread({"request_id": "no-server"}, 30)
        time.sleep(0.05)
        self.client.stop()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        with self.assertRaises(TransportError):
            result.result()


if __name__ == "__main__":
    unittest.main()

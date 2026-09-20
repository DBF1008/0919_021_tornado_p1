"""Tests for the error-handling/output-chain consistency fixes in web.py.

These tests cover three related problems:

1. An exception raised after ``flush()`` (i.e. after the status line and
   headers were sent) must not produce a response whose status code
   disagrees with the headers already sent, and must not corrupt the
   framing of a keepalive connection (which clients would observe as
   duplicate/garbage status lines on the reused connection).
2. If a ``GZipContentEncoding`` transform has incrementally compressed
   part of the response and the response then fails, the connection must
   be aborted so the client sees a network-level error instead of
   silently receiving an incomplete gzip stream.
3. The connection close callback must be cleared in exactly one place,
   and if the connection was closed while our callback was still
   registered, ``on_connection_close`` must still be delivered so
   resources are not leaked silently.

The in-process tests use a fake connection and need no sockets.
The ``Integration*`` tests use a real server and client socket.
"""

import gzip
import io
import socket
import unittest

from tornado import httputil, iostream, web
from tornado.concurrent import Future
from tornado.iostream import IOStream
from tornado.testing import AsyncHTTPTestCase, AsyncTestCase, ExpectLog, gen_test
from tornado.web import (
    Application,
    GZipContentEncoding,
    HTTPError,
    OutputTransform,
    RequestHandler,
)

app_log = web.app_log
gen_log = web.gen_log


class FakeStream:
    """Minimal stand-in for an IOStream."""

    def __init__(self) -> None:
        self._closed = False
        self.close_callback = None

    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        self._closed = True

    def set_close_callback(self, callback) -> None:
        self.close_callback = callback


class FakeConnection:
    """In-process stand-in for HTTP1Connection.

    Records the bytes that would be sent to the client in ``wire`` and
    emulates the Content-Length accounting of the real connection.
    """

    def __init__(self) -> None:
        self.stream = FakeStream()
        self.wire: list[bytes] = []
        self._close_callback = None
        self._headers_written = False
        self._expected_content_remaining = None
        self.finished = False

    def set_close_callback(self, callback) -> None:
        self._close_callback = callback

    def _closed_future(self):
        future = Future()
        future.set_exception(iostream.StreamClosedError())
        future.exception()
        return future

    def _format_chunk(self, chunk: bytes) -> bytes:
        if self._expected_content_remaining is not None:
            self._expected_content_remaining -= len(chunk)
            if self._expected_content_remaining < 0:
                self.stream.close()
                raise httputil.HTTPOutputError(
                    "Tried to write more data than Content-Length"
                )
        return chunk

    def write_headers(self, start_line, headers, chunk=None):
        if self.stream.closed():
            return self._closed_future()
        self._headers_written = True
        if "Content-Length" in headers:
            self._expected_content_remaining = int(headers["Content-Length"])
        data = b"HTTP/1.1 %d %s\r\n" % (
            start_line.code,
            start_line.reason.encode("latin1"),
        )
        for name, value in headers.get_all():
            data += name.encode("latin1") + b": " + value.encode("latin1") + b"\r\n"
        data += b"\r\n"
        if chunk:
            data += self._format_chunk(chunk)
        self.wire.append(data)
        future = Future()
        future.set_result(None)
        return future

    def write(self, chunk: bytes):
        if self.stream.closed():
            return self._closed_future()
        self.wire.append(self._format_chunk(chunk))
        future = Future()
        future.set_result(None)
        return future

    def finish(self) -> None:
        if (
            self._expected_content_remaining is not None
            and self._expected_content_remaining != 0
            and not self.stream.closed()
        ):
            self.stream.close()
            raise httputil.HTTPOutputError(
                "Tried to write %d bytes less than Content-Length"
                % self._expected_content_remaining
            )
        self.finished = True


def make_handler(handler_class, headers=None):
    """Build a RequestHandler wired to a FakeConnection (no sockets)."""
    connection = FakeConnection()
    request = httputil.HTTPServerRequest(
        method="GET",
        uri="/",
        version="HTTP/1.1",
        host="example.com",
        headers=httputil.HTTPHeaders(headers or {}),
        connection=connection,
    )
    application = Application([], log_function=lambda handler: None)
    handler = handler_class(application, request)
    return handler, connection


class ErrorAfterFlushTest(AsyncTestCase):
    """Problem 1: errors after the headers were sent must abort the
    connection instead of producing an inconsistent response."""

    @gen_test
    def test_error_after_flush_aborts_connection(self):
        events = []

        class Handler(RequestHandler):
            def get(self):
                self.write("partial")
                self.flush()
                raise HTTPError(500)

            def on_finish(self):
                events.append("on_finish")

        handler, connection = make_handler(Handler)
        with ExpectLog(gen_log, "Cannot send error response after headers written"):
            yield handler._execute([])
        self.assertTrue(handler._finished)
        # The connection is aborted without terminating the HTTP framing.
        self.assertTrue(connection.stream.closed())
        self.assertFalse(connection.finished)
        # The client sees exactly one status line (the original 200); no
        # conflicting error status line is emitted afterwards.
        wire = b"".join(connection.wire)
        self.assertEqual(wire.count(b"HTTP/1.1"), 1)
        self.assertTrue(wire.startswith(b"HTTP/1.1 200"))
        # The request lifecycle ran exactly once.
        self.assertEqual(events, ["on_finish"])
        # The close callback was cleared so the handler can be GC'd.
        self.assertIsNone(connection._close_callback)

    @gen_test
    def test_buffered_output_is_replaced_by_error_page(self):
        # Output that was written but not flushed is still discarded and
        # replaced by a consistent error response (unchanged behavior).
        class Handler(RequestHandler):
            def get(self):
                self.write("never sent")
                raise HTTPError(500)

        handler, connection = make_handler(Handler)
        yield handler._execute([])
        self.assertFalse(connection.stream.closed())
        self.assertTrue(connection.finished)
        wire = b"".join(connection.wire)
        self.assertEqual(wire.count(b"HTTP/1.1"), 1)
        self.assertTrue(wire.startswith(b"HTTP/1.1 500"))
        self.assertNotIn(b"never sent", wire)


class FailOnceTransform(OutputTransform):
    """Fails the first time transform_first_chunk runs, then passes."""

    def __init__(self, request) -> None:
        self.calls = 0

    def transform_first_chunk(self, status_code, headers, chunk, finishing):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("transform boom")
        return status_code, headers, chunk


class TransformFailureTest(AsyncTestCase):
    """Problem 1 (cont.): a transform failure during the first flush must
    not mark the headers as written; the error path can then still send a
    consistent error response."""

    @gen_test
    def test_transform_failure_allows_consistent_error_response(self):
        class Handler(RequestHandler):
            def get(self):
                self.write("data")
                self.flush()

        handler, connection = make_handler(Handler)
        transform = FailOnceTransform(handler.request)
        with ExpectLog(app_log, "Uncaught exception"):
            yield handler._execute([transform])
        self.assertTrue(handler._finished)
        # No bytes were sent before the failure, so a clean 500 response
        # (exactly one status line) could be sent and the keepalive
        # connection remains usable.
        wire = b"".join(connection.wire)
        self.assertEqual(wire.count(b"HTTP/1.1"), 1)
        self.assertTrue(wire.startswith(b"HTTP/1.1 500"))
        self.assertFalse(connection.stream.closed())
        self.assertTrue(connection.finished)


class GzipAbortTest(AsyncTestCase):
    """Problem 2: a response that fails after gzip compression started
    must abort the connection so the client detects the truncation."""

    @gen_test
    def test_gzip_stream_aborted_on_mid_stream_error(self):
        class Handler(RequestHandler):
            def get(self):
                self.set_header("Content-Type", "text/html")
                self.write("x" * 2048)
                self.flush()
                self.write("never flushed")
                raise RuntimeError("boom")

        handler, connection = make_handler(
            Handler, headers={"Accept-Encoding": "gzip"}
        )
        transforms = [GZipContentEncoding(handler.request)]
        with ExpectLog(app_log, "Uncaught exception"):
            with ExpectLog(
                gen_log, "Cannot send error response after headers written"
            ):
                yield handler._execute(transforms)
        self.assertTrue(connection.stream.closed())
        self.assertFalse(connection.finished)
        wire = b"".join(connection.wire)
        self.assertEqual(wire.count(b"HTTP/1.1"), 1)
        header_block, _, body = wire.partition(b"\r\n\r\n")
        self.assertIn(b"Content-Encoding: gzip", header_block)
        # The gzip stream is incomplete: decompression fails, so the
        # client gets a detectable error (together with the connection
        # close) instead of silently accepting corrupt data.
        with self.assertRaises(Exception):
            gzip.GzipFile(fileobj=io.BytesIO(body)).read()

    @gen_test
    def test_gzip_stream_complete_on_clean_finish(self):
        class Handler(RequestHandler):
            def get(self):
                self.set_header("Content-Type", "text/html")
                self.write("x" * 2048)
                self.flush()
                self.write("y" * 100)

        handler, connection = make_handler(
            Handler, headers={"Accept-Encoding": "gzip"}
        )
        yield handler._execute([GZipContentEncoding(handler.request)])
        self.assertFalse(connection.stream.closed())
        self.assertTrue(connection.finished)
        wire = b"".join(connection.wire)
        _, _, body = wire.partition(b"\r\n\r\n")
        self.assertEqual(
            gzip.GzipFile(fileobj=io.BytesIO(body)).read(), b"x" * 2048 + b"y" * 100
        )


class GzipTransformGuardTest(unittest.TestCase):
    """Problem 2 (cont.): the gzip transform fails loudly (instead of
    corrupting the output) if its stream was never initialized."""

    def test_chunk_before_first_chunk_raises(self):
        request = httputil.HTTPServerRequest(
            method="GET",
            uri="/",
            headers=httputil.HTTPHeaders({"Accept-Encoding": "gzip"}),
        )
        transform = GZipContentEncoding(request)
        with self.assertRaises(httputil.HTTPOutputError):
            transform.transform_chunk(b"data", finishing=False)


class FinishFailureTest(AsyncTestCase):
    """Problem 1 (cont.): if finish() itself fails (e.g. Content-Length
    mismatch), the connection is aborted and the lifecycle runs once."""

    @gen_test
    def test_finish_failure_aborts_connection(self):
        events = []

        class Handler(RequestHandler):
            def get(self):
                self.set_header("Content-Length", "42")
                self.finish("ok")

            def on_finish(self):
                events.append("on_finish")

        handler, connection = make_handler(Handler)
        with ExpectLog(app_log, "Uncaught exception"):
            yield handler._execute([])
        self.assertTrue(handler._finished)
        self.assertTrue(connection.stream.closed())
        self.assertEqual(events, ["on_finish"])
        self.assertIsNone(connection._close_callback)


class CloseCallbackTest(AsyncTestCase):
    """Problem 3: close callback lifecycle."""

    @gen_test
    def test_close_callback_cleared_on_finish(self):
        class Handler(RequestHandler):
            def get(self):
                self.write("ok")

        handler, connection = make_handler(Handler)
        self.assertIsNotNone(connection._close_callback)
        yield handler._execute([])
        # Cleared so the keepalive connection does not keep the handler
        # alive; the connection itself stays open for reuse.
        self.assertIsNone(connection._close_callback)
        self.assertFalse(connection.stream.closed())
        self.assertTrue(connection.finished)

    @gen_test
    def test_close_notification_delivered_when_connection_closed_early(self):
        events = []

        class Handler(RequestHandler):
            def get(self):
                # Simulate the client disappearing while we were working:
                # the stream is closed but the ioloop has not delivered
                # the close notification to the connection yet.
                self.request.connection.stream.close()  # type: ignore
                self.write("ok")

            def on_connection_close(self):
                events.append("close")

        handler, connection = make_handler(Handler)
        yield handler._execute([])
        # The pending notification was delivered exactly once before the
        # callback was cleared, instead of being dropped silently.
        self.assertEqual(events, ["close"])
        self.assertIsNone(connection._close_callback)
        # Clearing is idempotent: no duplicate notification.
        handler._clear_close_callback()
        self.assertEqual(events, ["close"])


def read_chunked_body(data: bytes) -> bytes:
    """Best-effort de-chunking of a (possibly truncated) chunked body."""
    body = b""
    while data:
        size_line, _, rest = data.partition(b"\r\n")
        try:
            size = int(size_line, 16)
        except ValueError:
            break
        if size == 0:
            break
        body += rest[:size]
        data = rest[size + 2:]
    return body


class IntegrationAbortTest(AsyncHTTPTestCase):
    """End-to-end tests over real sockets (require network permissions)."""

    def get_app(self):
        class OkHandler(RequestHandler):
            def get(self):
                self.write("ok")

        class FlushThenErrorHandler(RequestHandler):
            def get(self):
                self.write("partial")
                self.flush()
                raise HTTPError(500)

        class GzipThenErrorHandler(RequestHandler):
            def get(self):
                self.set_header("Content-Type", "text/html")
                self.write("x" * 2048)
                self.flush()
                raise RuntimeError("boom")

        return Application(
            [
                ("/ok", OkHandler),
                ("/flush_error", FlushThenErrorHandler),
                ("/gzip_error", GzipThenErrorHandler),
            ]
        )

    async def connect(self):
        stream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
        await stream.connect(("127.0.0.1", self.get_port()))
        return stream

    @gen_test
    def test_error_after_flush_closes_connection(self):
        stream = yield self.connect()
        with ExpectLog(gen_log, "Cannot send error response after headers written"):
            stream.write(b"GET /flush_error HTTP/1.1\r\nHost: localhost\r\n\r\n")
            # read_until_close completes only if the server closes the
            # connection (a keepalive connection would be left open and
            # this test would time out).
            data = yield stream.read_until_close()
        # Exactly one status line: no conflicting error status is sent
        # after the original headers.
        self.assertEqual(data.count(b"HTTP/1.1"), 1)
        self.assertTrue(data.startswith(b"HTTP/1.1 200 OK"))
        # The chunked framing was not terminated cleanly.
        self.assertFalse(data.rstrip().endswith(b"0\r\n\r\n"))
        stream.close()

    @gen_test
    def test_gzip_error_propagates_as_connection_close(self):
        stream = yield self.connect()
        with ExpectLog(app_log, "Uncaught exception"):
            with ExpectLog(
                gen_log, "Cannot send error response after headers written"
            ):
                stream.write(
                    b"GET /gzip_error HTTP/1.1\r\nHost: localhost\r\n"
                    b"Accept-Encoding: gzip\r\n\r\n"
                )
                data = yield stream.read_until_close()
        self.assertEqual(data.count(b"HTTP/1.1"), 1)
        header_block, _, raw_body = data.partition(b"\r\n\r\n")
        self.assertIn(b"Content-Encoding: gzip", header_block)
        # The client can detect the truncated gzip stream as an error.
        with self.assertRaises(Exception):
            gzip.GzipFile(fileobj=io.BytesIO(read_chunked_body(raw_body))).read()
        stream.close()

    @gen_test
    def test_keepalive_still_works_for_normal_requests(self):
        stream = yield self.connect()
        for _ in range(2):
            stream.write(b"GET /ok HTTP/1.1\r\nHost: localhost\r\n\r\n")
            header_block = yield stream.read_until(b"\r\n\r\n")
            self.assertTrue(header_block.startswith(b"HTTP/1.1 200 OK"))
            headers = httputil.HTTPHeaders.parse(header_block.decode("latin1"))
            body = yield stream.read_bytes(int(headers["Content-Length"]))
            self.assertEqual(body, b"ok")
        stream.close()


if __name__ == "__main__":
    unittest.main()

"""Bounded incremental SSE framing in the private HTTP worker."""
import io
import json
import time
import unittest

from mas_host.bridges._http_worker import read_response
from mas_host.bridges.service_client import http_exchange
from mas_host.service_contracts import ServiceError
from test_bridge_server import BoundedPeer


class Response(io.BytesIO):
    def __init__(self, content, media="text/event-stream", chunk_size=1):
        super().__init__(content)
        self.media = media
        self.chunk_size = chunk_size

    def getheader(self, _key, default=""):
        return self.media

    def read1(self, size):
        return self.read(min(size, self.chunk_size))


def event(value, newline=b"\n"):
    return b"data: " + json.dumps(value, ensure_ascii=False).encode() + newline + newline


class HttpStreamTests(unittest.TestCase):
    def test_cr_lf_crlf_bom_multiline_and_unicode_with_one_byte_chunks(self):
        for newline in (b"\r", b"\n", b"\r\n"):
            with self.subTest(newline=newline):
                prefix = b"\xef\xbb\xbf: comment" + newline + newline
                payload = (b'data: {"jsonrpc":"2.0",' + newline +
                           'data: "id":1,"result":{"value":"a\u2028b"}}'.encode() + newline + newline)
                answer = read_response(Response(prefix + payload + b"never read this"), 65536, 1)
                self.assertNotIn(b"never", answer)
                self.assertIn("a\u2028b".encode(), answer)
                self.assertTrue(answer.endswith(newline) or newline == b"\r\n" and answer.endswith(b"\r"))

    def test_notification_wrong_id_boolean_id_and_method_do_not_finish(self):
        prefix = b"".join(event(value) for value in (
            {"jsonrpc": "2.0", "method": "notifications/progress", "params": {}},
            {"jsonrpc": "2.0", "id": 2, "result": {}},
            {"jsonrpc": "2.0", "id": True, "result": {}},
            {"jsonrpc": "2.0", "id": 1, "method": "nested", "result": {}},
        ))
        final = event({"jsonrpc": "2.0", "id": 1, "error": {"code": -1, "message": "error"}})
        self.assertEqual(read_response(Response(prefix + final + b"tail"), 65536, 1), prefix + final)

    def test_aggregate_byte_bound_includes_comments_and_notifications(self):
        with self.assertRaises(ValueError):
            read_response(Response(b":comment\n\n" * 20), 100, 1)
        with self.assertRaises(ValueError):
            read_response(Response(b"x" * 101, media="application/json"), 100)
        self.assertEqual(read_response(Response(b"whole body", media="application/json"), 100, 1), b"whole body")

    def test_real_chunked_response_returns_before_eof_and_default_waits(self):
        final = event({"jsonrpc": "2.0", "id": "rpc", "result": {"accepted": True}})
        def respond(connection, stop):
            connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n" +
                format(len(final), "x").encode() + b"\r\n" + final + b"\r\n")
            stop.wait(3)
        with BoundedPeer(respond) as peer:
            started = time.monotonic()
            status, _headers, body = http_exchange(peer.endpoint, method="GET", timeout_seconds=1,
                allow_loopback_http=True, sse_response_id="rpc")
            self.assertEqual(status, 200)
            self.assertEqual(body, final)
            self.assertLess(time.monotonic() - started, 1)
        with BoundedPeer(respond) as peer:
            with self.assertRaises(ServiceError) as raised:
                http_exchange(peer.endpoint, method="GET", timeout_seconds=.25, allow_loopback_http=True)
            self.assertEqual(raised.exception.code, "transport_timeout")


if __name__ == "__main__":
    unittest.main()

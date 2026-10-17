"""Real pinned-Node proxy routing against offline, loopback-only fixtures.

Set MAS_TEST_CURSOR_NODE to a separately installed, hash-verified pinned Node
when the optional SDK is absent. These tests do not contact a provider, and the
SDK configuration fixture does not establish how a remote server's HTTP/2
override affects the real SDK's generation transport.
"""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import select
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest

from mas_host.cursor_local import _validate_node
from mas_host.cursor_transport import (ENVIRONMENT_PROXY_HTTP1, SDK_DEFAULT,
                                       bridge_transport)


# Generate disposable test credentials using the pinned Node itself. Construct
# a minimal self-signed X.509 certificate without an openssl/cryptography test
# dependency or a checked-in private key. Its SAN is deliberately not localhost.
_CERTIFICATE_SCRIPT = r"""
const {generateKeyPairSync, sign, X509Certificate} = require('node:crypto');
const {writeFileSync} = require('node:fs');
function der(tag, ...parts) {
  const body = Buffer.concat(parts);
  let n = body.length, length;
  if (n < 128) length = Buffer.from([n]);
  else { const a = []; while (n) { a.unshift(n & 255); n >>>= 8; }
         length = Buffer.from([128 | a.length, ...a]); }
  return Buffer.concat([Buffer.from([tag]), length, body]);
}
const seq = (...parts) => der(48, ...parts);
const oid = hex => der(6, Buffer.from(hex, 'hex'));
const alg = seq(oid('2a864886f70d01010b'), der(5));
const name = seq(der(49, seq(oid('550403'), der(12, Buffer.from('MAS offline proxy test')))));
const pair = generateKeyPairSync('rsa', {modulusLength: 2048});
const extensions = der(163, seq(
  seq(oid('551d13'), der(1, Buffer.from([255])), der(4, seq(der(1, Buffer.from([255]))))),
  seq(oid('551d11'), der(4, seq(der(130, Buffer.from('remote.invalid')))))
));
const tbs = seq(der(160, der(2, Buffer.from([2]))), der(2, Buffer.from([1])), alg, name,
  seq(der(24, Buffer.from('20000101000000Z')), der(24, Buffer.from('21000101000000Z'))),
  name, pair.publicKey.export({type: 'spki', format: 'der'}), extensions);
const cert = seq(tbs, alg, der(3, Buffer.from([0]), sign('sha256', tbs, pair.privateKey)));
const pem = new X509Certificate(cert).toString();
writeFileSync(process.argv[1], pem, {mode: 0o600});
writeFileSync(process.argv[2], pair.privateKey.export({type: 'pkcs8', format: 'pem'}), {mode: 0o600});
"""

_ENTRY_SCRIPT = r"""
import http from 'node:http';
import https from 'node:https';
import {readFixtureConfig} from '@cursor/sdk';
const url = new URL(process.argv[2]);
const headers = process.argv[3] ? {Authorization: process.argv[3]} : {};
const result = process.argv[4] === 'fetch' ? await (async () => {
  try {
    const response = await fetch(url, {headers, signal: AbortSignal.timeout(3000)});
    return {ok: true, body: await response.json()};
  } catch (error) { return {ok: false, code: error.cause?.code ?? error.code}; }
})() : await new Promise(resolve => {
  const request = (url.protocol === 'https:' ? https : http).get(url, {headers}, response => {
    let body = '';
    response.setEncoding('utf8');
    response.on('data', part => { body += part; });
    response.on('end', () => resolve({ok: true, body: JSON.parse(body)}));
    response.on('error', error => resolve({ok: false, code: error.code}));
  });
  request.setTimeout(3000, () => request.destroy(new Error('fixture deadline')));
  request.on('error', error => resolve({ok: false, code: error.code}));
});
process.stdout.write(JSON.stringify({...result, configured: readFixtureConfig(),
  entry: process.argv[1], args: process.argv.slice(2)}));
"""


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False


class _Origin(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_GET(self):
        self.server.requests.append((self.path, dict(self.headers)))
        body = json.dumps({"route": "origin"}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _TrapProxy(_Origin):
    def do_GET(self):
        self.server.requests.append((self.path, dict(self.headers)))
        body = json.dumps({"route": "proxy"}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_CONNECT(self):
        self.server.requests.append((self.path, dict(self.headers)))
        # Only explicitly registered fixture destinations can be tunnelled.
        # Never resolve the supplied host or connect to an external address.
        destination = self.server.tunnels.get(self.path)
        if destination is None:
            self.send_error(502)
            return
        with socket.create_connection(destination, timeout=3) as upstream:
            self.send_response(200, "Connection established")
            self.end_headers()
            self.wfile.flush()
            peers = (self.connection, upstream)
            try:
                while True:
                    readable, _, _ = select.select(peers, [], [], 3)
                    if not readable:
                        return
                    for source in readable:
                        data = source.recv(65536)
                        if not data:
                            return
                        (upstream if source is self.connection else self.connection).sendall(data)
            except (OSError, ValueError):
                return


class CursorProxyIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not sys.platform.startswith("linux"):
            raise unittest.SkipTest("optional pinned Linux Cursor Node runtime")
        supplied = os.environ.get("MAS_TEST_CURSOR_NODE")
        if supplied:
            cls.node = Path(supplied).resolve()
        else:
            spec = importlib.util.find_spec("cursor_sdk")
            if spec is None:
                raise unittest.SkipTest("optional pinned Cursor Node runtime")
            cls.node = Path(spec.origin).parent / "_vendor" / "bridge" / "bin" / "node"
        # An explicitly supplied or installed wrong runtime is a failure, never
        # an optional skip that could conceal untested proxy behavior.
        _validate_node(cls.node)
        cls.cert_directory = tempfile.TemporaryDirectory(prefix="mas-proxy-certs-")
        cls.addClassCleanup(cls.cert_directory.cleanup)
        cls.cert = Path(cls.cert_directory.name) / "cert.pem"
        cls.key = Path(cls.cert_directory.name) / "key.pem"
        subprocess.run([str(cls.node), "-e", _CERTIFICATE_SCRIPT, str(cls.cert), str(cls.key)],
                       env={"PATH": os.defpath}, check=True, capture_output=True, timeout=15)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="mas-proxy-fixture-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.entry = self.root / "bridge" / "dist" / "bin" / "fixture.mjs"
        self.entry.parent.mkdir(parents=True)
        self.entry.write_text(_ENTRY_SCRIPT)
        sdk = self.root / "bridge" / "node_modules" / "@cursor" / "sdk"
        (sdk / "dist" / "esm").mkdir(parents=True)
        (sdk / "package.json").write_text(
            '{"type":"module","exports":{"import":"./dist/esm/index.js"}}')
        (sdk / "dist" / "esm" / "index.js").write_text(
            "let configured = null;\n"
            "export function configureCursorSdk(config) { configured = config; }\n"
            "export function readFixtureConfig() { return configured; }\n")
        self.origin = self.serve("127.0.0.1", _Origin)
        self.other_origin = self.serve("127.0.0.2", _Origin)
        self.tls_origin = self.serve("127.0.0.1", _Origin, tls=True)
        self.proxy = self.serve("127.0.0.1", _TrapProxy)
        port = self.tls_origin.server_port
        self.proxy.tunnels = {f"remote.invalid:{port}": ("127.0.0.1", port),
                              f"wrong.invalid:{port}": ("127.0.0.1", port),
                              "remote.invalid:80": ("127.0.0.1", self.origin.server_port),
                              f"127.0.0.2:{self.other_origin.server_port}":
                                  ("127.0.0.2", self.other_origin.server_port)}
        self.proxy_url = f"http://127.0.0.1:{self.proxy.server_port}"

    def serve(self, host, handler, *, tls=False):
        server = _Server((host, 0), handler)
        server.requests = []
        if tls:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(self.cert, self.key)
            server.socket = context.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def request(self, url, *, policy=ENVIRONMENT_PROXY_HTTP1, environment=None, authorization="", client="http",
                reference_proxy=False):
        environment = {"PATH": os.defpath, "HOME": str(self.root),
                       "HTTP_PROXY": self.proxy_url, "HTTPS_PROXY": self.proxy_url,
                       **(environment or {})}
        original = dict(environment)
        parent_environment = dict(os.environ)
        arguments = [url, authorization, client]
        argv, child = bridge_transport(self.node, self.entry, arguments, environment, policy)
        if reference_proxy:
            self.assertEqual(policy, SDK_DEFAULT)
            argv.insert(1, "--use-env-proxy")
        self.assertEqual(environment, original, "transport builder mutated caller environment")
        self.assertEqual(dict(os.environ), parent_environment, "transport builder mutated parent environment")
        completed = subprocess.run(argv, env=child, capture_output=True, text=True, timeout=10)
        self.assertEqual(environment, original)
        self.assertEqual(dict(os.environ), parent_environment)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertEqual(result["entry"], str(self.entry))
        self.assertEqual(result["args"], arguments)
        expected = {"local": {"useHttp1ForAgent": True}} if policy == ENVIRONMENT_PROXY_HTTP1 else None
        self.assertEqual(result["configured"], expected)
        return result

    def test_default_policy_remains_direct_with_inherited_proxy(self):
        for client in ("http", "fetch"):
            result = self.request(f"http://127.0.0.2:{self.other_origin.server_port}/default",
                                  policy=SDK_DEFAULT, client=client)
            self.assertEqual(result["body"]["route"], "origin")
        self.assertEqual(self.proxy.requests, [])

    def test_proxy_routes_http_and_https_without_disabling_tls_verification(self):
        for client in ("http", "fetch"):
            with self.subTest(client=client):
                before = len(self.proxy.requests)
                result = self.request("http://remote.invalid/plain", client=client)
                self.assertTrue(result["ok"], result)
                self.assertEqual(len(self.proxy.requests), before + 1)
                before_tls = len(self.tls_origin.requests)
                url = f"https://remote.invalid:{self.tls_origin.server_port}/secure"
                rejected = self.request(url, client=client)
                self.assertFalse(rejected["ok"], "untrusted certificate was accepted")
                self.assertIn(rejected["code"], {"DEPTH_ZERO_SELF_SIGNED_CERT", "SELF_SIGNED_CERT_IN_CHAIN"})
                self.assertEqual(len(self.tls_origin.requests), before_tls)
                accepted = self.request(url, environment={"NODE_EXTRA_CA_CERTS": str(self.cert)}, client=client)
                self.assertEqual(accepted["body"]["route"], "origin")
                wrong_name = self.request(f"https://wrong.invalid:{self.tls_origin.server_port}/wrong",
                                          environment={"NODE_EXTRA_CA_CERTS": str(self.cert)}, client=client)
                self.assertFalse(wrong_name["ok"])
                self.assertEqual(wrong_name["code"], "ERR_TLS_CERT_ALTNAME_INVALID")
                self.assertEqual(len(self.proxy.requests), before + 4)
                self.assertEqual(len(self.tls_origin.requests), before_tls + 1)

    def test_authenticated_loopback_callbacks_never_reach_proxy(self):
        marker = "Bearer offline-callback-canary"
        for client in ("http", "fetch"):
            for host in ("127.0.0.1", "localhost"):
                result = self.request(f"http://{host}:{self.origin.server_port}/callback",
                                      environment={"NO_PROXY": "", "no_proxy": ""},
                                      authorization=marker, client=client)
                self.assertEqual(result["body"]["route"], "origin")
        self.assertEqual(self.proxy.requests, [])
        self.assertEqual(len(self.origin.requests), 4)
        self.assertTrue(all(headers["Authorization"] == marker for _, headers in self.origin.requests))
        result = self.request("http://localhost.evil.invalid/not-loopback")
        self.assertEqual(result["body"]["route"], "proxy")
        self.assertNotIn(marker, json.dumps(self.proxy.requests))

    def test_effective_no_proxy_rules_are_preserved_including_lowercase_precedence(self):
        url = f"http://127.0.0.2:{self.other_origin.server_port}/rules"
        cases = [
            ({"NO_PROXY": "127.0.0.2"}, "origin"),
            ({"NO_PROXY": "elsewhere.invalid", "no_proxy": "127.0.0.2"}, "origin"),
            ({"NO_PROXY": "127.0.0.2", "no_proxy": "elsewhere.invalid"}, "proxy"),
            ({"NO_PROXY": "127.0.0.2", "no_proxy": ""}, "origin"),
            ({"NO_PROXY": "*"}, "origin"),
            ({"NO_PROXY": "*", "no_proxy": "elsewhere.invalid"}, "proxy"),
            ({"NO_PROXY": "elsewhere.invalid", "no_proxy": "*"}, "origin"),
            ({"NO_PROXY": f"127.0.0.2:{self.other_origin.server_port}"}, "origin"),
            ({"NO_PROXY": "127.0.0.2:1"}, "proxy"),
        ]
        for client in ("http", "fetch"):
            for environment, route in cases:
                with self.subTest(environment=environment, client=client):
                    before = len(self.proxy.requests)
                    result = self.request(url, environment=environment, client=client)
                    self.assertTrue(result["ok"], result)
                    self.assertEqual(len(self.proxy.requests) - before, int(route == "proxy"))

    def test_wildcard_whitespace_preserves_each_native_clients_existing_semantics(self):
        url = f"http://127.0.0.2:{self.other_origin.server_port}/wildcard"
        for client in ("http", "fetch"):
            for wildcard in ("*", " * ", "*,elsewhere.invalid"):
                with self.subTest(client=client, wildcard=wildcard):
                    environment = {"NO_PROXY": wildcard}
                    before = len(self.proxy.requests)
                    reference = self.request(url, policy=SDK_DEFAULT, environment=environment,
                                             client=client, reference_proxy=True)
                    self.assertTrue(reference["ok"], reference)
                    reference_count = len(self.proxy.requests) - before
                    before = len(self.proxy.requests)
                    configured = self.request(url, environment=environment, client=client)
                    self.assertTrue(configured["ok"], configured)
                    self.assertEqual(len(self.proxy.requests) - before, reference_count)


if __name__ == "__main__":
    unittest.main()

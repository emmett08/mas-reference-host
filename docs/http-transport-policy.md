# Explicit HTTP transport policy

The default MAS HTTP transport uses system TLS trust, connects directly and
ignores ambient proxy/CA environment variables. It never follows redirects.
An optional `HTTPTransportPolicy` enables a configured corporate HTTP CONNECT
proxy, additional private CA roots and an exact destination-origin allowlist.
It does not disable certificate or hostname verification.

```python
from mas_host.bridges.http_policy import HTTPTransportPolicy
from mas_host.bridges.service_client import TaskServiceClient

policy = HTTPTransportPolicy.from_config({
    "proxy_url": "http://proxy.example:8080",
    "proxy_authorization_file": "/run/secrets/proxy-authorization",
    "additional_ca_file": "/run/secrets/corporate-ca.pem",
    "allowed_origins": ["https://mas.example", "https://identity.example"],
    "phase_timeouts_ms": {
        "dns": 2000, "connect": 5000, "tls": 5000,
        "headers": 10000, "body": 10000
    }
})
client = TaskServiceClient(
    "https://mas.example", token_file=private_token_file,
    principal="service-client", transport_policy=policy)
```

The proxy credential file contains one `Basic ...` or `Bearer ...` header value,
optionally followed by one newline, and follows the existing private-file rules.
It is read at request time, so rotation does not require changing the frozen
transport descriptor. It is sent only in CONNECT, never to the origin. Proxy
URLs cannot contain credentials, paths, queries or fragments. This profile
supports an HTTP proxy tunnelling an HTTPS origin; HTTPS-to-proxy transport and
proxying cleartext origins are rejected. Literal loopback addresses and
`localhost` bypass the proxy and do not read its credential file.

The optional origin allowlist is exact after normalising scheme, hostname and
port. An empty list permits configured endpoints under the usual endpoint
validation rules. CA PEM content is bounded and frozen when the policy is built;
its SHA-256, the proxy origin and allowed origins participate in remote adapter
identity. CA file paths and credentials do not. Additional roots extend system
trust; they do not replace it. Refresh the configured policy and implementation
version intentionally when trust roots change.

`BoundedJSONHTTP` (including A2A clients), `StreamableHTTPTransport` (MCP),
`TaskServiceClient` and `OAuthIntrospectionVerifier` accept `transport_policy`.
For OAuth deployment configuration, add the same dictionary as `auth.transport`.
No endpoint discovery, response header or request body can override this policy.
The existing isolated subprocess still bounds DNS, connect, TLS, header and
body handling with an absolute deadline, including when invoked from a worker
thread. Artefact chunk transfers inherit their configured HTTP/MCP policy.

`phase_timeouts_ms` is optional. When supplied, it specifies all five positive
integer budgets, each from 1 to 300,000 milliseconds. Each budget is a cumulative
wall-clock bound for that phase, clipped to the original operation deadline:
slowly arriving bytes cannot restart it. DNS covers the directly contacted host
(the proxy when configured). Connect covers all returned addresses and any
HTTP CONNECT negotiation; the proxy's target resolution and connection consume
that same budget because their individual phases are not visible to MAS. TLS
covers the origin handshake and certificate/hostname verification. Headers
covers writing the request and receiving response headers. Body covers the
complete bounded response, or a complete correlated MCP SSE response event.

The isolated worker uses one watchdog thread and synchronous name resolution.
An expired phase terminates that disposable worker, including a blocked DNS
lookup, and the parent reaps it. No resolver thread survives in the client.
The parent reports a fixed `transport_dns_timeout`, `transport_connect_timeout`,
`transport_tls_timeout`, `transport_headers_timeout` or `transport_body_timeout`
code; the outer deadline reports `transport_timeout`. These observations do not
prove non-execution or authorise replay of an uncertain effect. No upstream
exception, URL, request body or credential appears in a timeout diagnostic.

Policies without phase budgets retain their exact `mas-http-policy/1`
descriptor. Supplying budgets uses `mas-http-policy/2`; changing any budget
changes the frozen transport and peer identity. Register a new peer version
before selecting a changed policy. Secret rotation retains the existing binding.
The local and shared Rust-backed registries validate the same exact bounded
descriptor; configuration cannot supply a resolver, interpreter hook or socket
factory.

Offline tests use a real local TLS origin and CONNECT proxy: an untrusted CA
fails, configured trust succeeds, incorrect hostnames fail, origin/proxy
credentials remain separate, loopback bypass works, environment variables are
ignored and disallowed origins fail before a connection is attempted.
Additional tests interrupt blocked resolution, CONNECT and TLS; exercise
cumulative slow-header/body bounds over real local TLS; preserve the whole-call
deadline; and reject changed phase policies before reading peer credentials.

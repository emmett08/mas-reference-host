# Portable remote client

`mas-client` uses the authenticated service API on Linux, macOS and Windows. It
requires Python 3.10 or later and the `mas-host` package, without Rust binaries or
provider SDKs on the client. Execution remains on the configured Linux service.
The native client workflow installs the package and exercises actual HTTP and
command-line requests on all three platforms; check its result for the exact
revision being deployed.

```sh
mas-client https://mas.example/service discover --principal alice
mas-client https://mas.example/service submit --principal alice --data submit.json
mas-client https://mas.example/service inspect --principal alice --data task.json
```

The default credential prompt does not echo. It fails if a non-echoing terminal
is unavailable. `--token-stdin` explicitly accepts one token line from a private
pipe; do not put the token in a command argument or shell history. POSIX clients
can instead use `--token-file` with an owned, private regular file. Windows uses
the prompt, private pipe or an application-supplied credential provider.

`submit.json` contains `request_key`, `workflow` and `input`. Keep the original
request key after an interrupted response. `task.json` contains `task_id`.
The authenticated token supplies identity; `--principal` binds the local client
and does not grant server authority. Results are JSON on stdout; fixed errors
go to stderr. Requests are never automatically retried.

Supported commands are `discover`, `submit`, `inspect`, `result`, `cancel`,
`update`, `recover`, `events`, `wait`, `task`, `list`, `operator` and `artifact`.
`--data` supplies the named arguments of the corresponding `TaskServiceClient`
method; `operator` and `artifact` take an `operation` and a `payload` object.
`--data -` reads bounded JSON from stdin and cannot share it with `--token-stdin`.
Plain HTTP requires `--allow-loopback-http` and is restricted to loopback IPs.

Applications can resolve rotating credentials without a filesystem profile:

```python
from mas_host.bridges.service_client import TaskServiceClient

client = TaskServiceClient(
    'https://mas.example', principal='alice',
    token_provider=trusted_secret_store.current_token,
)
receipt = client.discover('alice')
```

The callable is trusted application configuration. It is called and validated on
every request. Its exception text and returned credentials never enter client
diagnostics. The Python client also accepts an explicit `HTTPTransportPolicy`
for approved proxy/CA routes; it does not inherit ambient proxy settings.

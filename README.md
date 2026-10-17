# MAS Python reference host

The Python reference implementation extracted from
[`emmett08/mas`](https://github.com/emmett08/mas/tree/395c407111ea6c37eaa203f60d3cc162b7c9c56b/host)
at commit `395c407111ea6c37eaa203f60d3cc162b7c9c56b`.
The `host/` package, tests, dependency locks, host build and qualification utilities, containers, documentation, and
`examples/{host,bridges,workflows}` fixtures retain their upstream layout.
`formal/authority-traces.json` is an imported test corpus, not a protocol specification.
GPL-3.0-only and the upstream third-party notices are retained.

## Repository boundaries

* [mas](https://github.com/emmett08/mas) owns the Rust protocol, wire format,
  formal models, and protocol verification. Do not implement a competing protocol here.
* **This repository** owns the Python reference host and its regression tests.
  Reference behavior is not a production-readiness or sandboxing guarantee.
* [mas-host](https://github.com/emmett08/mas-host) owns the separate native
  application host and its SDKs. New applications should target that host.
* [mas-host-examples](https://github.com/emmett08/mas-host-examples) owns the
  maintained native-host application examples. Fixtures here are reference-host
  regression inputs, not replacements for those applications.

The reference distribution remains named `mas-host` with the `mas-host` and
`mas-client` commands for compatibility. Its `mas_host` import **conflicts with
the native host's `mas-host-sdk` import**: install them in separate environments.

## Install and run

Use Linux and CPython 3.14 for the committed hash-locked optional dependencies.
The core package declares Python >=3.10; CI qualifies the locked 3.14 environment.
Create an isolated environment, then run from this checkout:

```sh
python3.14 -m venv .venv
. .venv/bin/activate
python -m pip install --require-hashes -r host/requirements-build.txt -r host/requirements-bridges.txt
python -m pip install --no-deps --no-build-isolation ./host
python -m pip check
mas-host --help
mas-client --help
```

The host delegates protocol operations to external `mas` and `mas-gateway`
executables; no Rust protocol source is vendored here. Download the protocol
source at the full SHA in `protocol-revision.txt` with `make protocol`. This
creates an **untracked** `.protocol/` checkout and `.protocol-revision` identity
file and compares the shared conformance corpora. With Rust 1.99.0, build its binaries:

```sh
make protocol
cargo +1.99.0 build --locked --release \
  --manifest-path .protocol/rust-protocol/Cargo.toml \
  --target-dir "$PWD/rust-protocol/target" \
  --features postgres-repository \
  --bin mas --bin mas-gateway --bin mas-protocol-codec --bin mas-shared
export MAS_BIN="$PWD/rust-protocol/target/release/mas"
export MAS_GATEWAY_BIN="$PWD/rust-protocol/target/release/mas-gateway"
export MAS_CODEC_BIN="$PWD/rust-protocol/target/release/mas-protocol-codec"
export MAS_SHARED_BIN="$PWD/rust-protocol/target/release/mas-shared"
mas-host --topology examples/host/mesh.json --mode mock --state-dir /tmp/mas-reference-demo
```

`rust-protocol/target` is only a compatibility build-output location for existing
tests, not a second protocol checkout. For installed deployments, binaries can
live elsewhere using the environment overrides above.

Optional SDKs use the corresponding `host/requirements-*.txt` hash locks.
Cursor additionally requires `python scripts/install-cursor-runtime.py` to
install its verified runtime repair. This does not require provider credentials.
Refresh locks only as a reviewed maintenance change using
`python scripts/release-locks.py`; verify them with `--check`.

## Verification

```sh
python -m unittest discover -s host/tests -p test_codec.py -v
python -m unittest discover -s host/tests -v
python -m unittest discover -s scripts/tests -v
python -m pip wheel --no-deps --no-build-isolation --wheel-dir /tmp/mas-reference-wheels ./host
```

CI builds the pinned external protocol and runs the existing suite with real
PostgreSQL, the locked bridge/provider dependencies, and the repaired Cursor
runtime. PostgreSQL tests require `MAS_TEST_POSTGRES_DSN` pointing to a disposable
database whose user can create databases; otherwise they explicitly skip.
Live-provider, real Temporal-server (`MAS_TEMPORAL_INTEROP=1`), and object-storage
integration qualifications require their own fixtures and are not implied by
offline CI. No paid-provider credentials belong in CI.

For bounded development exposure, run
`PYTHONPATH="$PWD/host" python scripts/check-reference-load.py --smoke --report /tmp/mas-reference-load.json`.
Non-smoke qualification requires a clean, committed checkout and imports from
that checkout. Reports bind this host revision and the external binary hashes;
one repository revision must not be presented as identifying both sources.

## Protection and security policy

All changes must use a reviewed PR, preserve the repository boundaries above,
retain licenses, and pass **reference-host** and **dependency-audit** checks.
Protocol or dependency pin changes require explicit compatibility review.
Do not weaken tests, silently skip missing mandatory CI binaries, or auto-publish
releases. Actions must be SHA-pinned, dependencies hash-locked, and workflow
tokens read-only without persisted checkout credentials. Never execute PR code
with `pull_request_target` or production secrets.

`.github/CODEOWNERS` assigns review ownership, including changes to CI and this
policy. A repository administrator must apply `.github/rulesets/main.json` in
Settings → Rules → Rulesets (or through the GitHub API): it requires code-owner
approval, current required checks, resolved discussions, and blocks default-branch
deletion and force pushes. Committing this template **does not enable protection**.
Administrators should delegate additional trusted code owners where needed for
author-independent review; do not bypass the policy to self-approve.

Use only disposable fixture data. Never commit or upload credentials, capability
tokens, private state databases, provider responses, or live acceptance evidence.
Report vulnerabilities privately through this repository's
[security advisory page](https://github.com/emmett08/mas-reference-host/security/advisories/new),
not a public issue containing exploit details or secrets. Dependency audits fail
closed; do not add blanket advisory exclusions.

## Containers, qualification and releases

After `make protocol`, run the credential-free reference mesh with:

```sh
docker compose up --build --abort-on-container-exit --exit-code-from mesh
```

The standard and Cursor Dockerfiles build Rust from `.protocol/rust-protocol`.
Python sources, locks, image package locks, Compose profiles and release tools
are maintained here. Runtime build records use `mas-runtime-build/2` to bind
this host repository and the separate protocol commit; the reader still accepts
historical version-1 records. Release manifests likewise record both source
identities. An override used by protocol compatibility CI is recorded separately
from the default reviewed pin. No release is published automatically.

The migrated workflows retain Linux amd64/arm64 container checks, installed
Temporal/LangGraph and real Temporal-server checks, Windows/macOS/Linux portable
clients, PostgreSQL/S3, trusted workspace containment, independent-VM faults and
image vulnerability scans. [Verification](docs/verification.md) separates the
historical evidence from checks on a submitted revision. The [host guide](docs/host-runner.md),
[operations](docs/operations.md) and [release procedure](docs/releases.md) provide
the corresponding commands.

## Coordinated migration

This repository owns the Python implementation, containers, release tooling,
reference examples and host documentation. `mas` retains protocol sources,
formal models, the shared trace corpus and immutable historical revisions.
Its compatibility workflows select this repository by full commit SHA and test
it against the protocol change under review. Required upstream check names are
preserved and fail if their migrated prerequisites fail or do not run.

The separate `mas-host`, `mas-host-examples` and `mas-demo` repositories keep their
own source, configuration and reviewed pins. The protocol removal PR exercises
their pinned revisions with the candidate Rust protocol without changing them.
Merge this repository's migration PR first, then the corresponding removal PR
only after its checks pass. Git history preserves all earlier pinned sources.

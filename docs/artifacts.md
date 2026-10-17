# Artefact exchange

`mas_host.artifacts` supplies immutable, bounded artefact exchange for any
workflow. Messages carry `ArtifactRef.to_dict()` values; bulk content uses local
files by default or an explicitly configured S3-compatible backend. Each reference contains its namespace,
content hash, byte count, media type, producer and input hashes. A second hash
binds those fields and the publication's read grants into an immutable identity.
References are limited to 8,192 JSON bytes, below the Rust protocol's complete
message limit. Applications must still budget for the surrounding message.

```python
from pathlib import Path
from mas_host.artifacts import ArtifactPolicy, ArtifactStore

# run_dir is the existing admitted Rust run directory. The host obtains actor
# identities from its authenticated configuration, not from message payloads.
store = ArtifactStore(
    Path(run_dir) / "artifacts",
    namespace="analysis-v1",
    principal="analyst",
    policy=ArtifactPolicy(
        max_artifact_bytes=8 * 1024 * 1024,
        max_total_bytes=64 * 1024 * 1024,
        max_artifacts=1024,
        max_links=4096,
    ),
)
reference = store.put_bytes(
    b"derived result\n",
    media_type="text/plain",
    input_sha256s=(input_digest,),
    readers=("checker",),
)
store.link("check-task-17", [reference], state="live")
message_payload = {"result": reference.to_dict()}
```

The recipient opens the **same local root and namespace**, under its own trusted
principal, then calls `read_bytes(reference)` or `open(reference)`. Both verify
the complete bounded content before returning any bytes. `open` returns a private, disk-backed verified snapshot; it does not buffer the
whole object in memory or expose unverified bytes to a consumer.
`verify(reference)` checks integrity without retaining the content in memory.
`put_stream(stream)` accepts a trusted seekable binary source with two-pass
hashing. `put_file(path)` streams a regular source file under the per-object limit;
symlinks, hard links and special files are rejected. A reference never selects a URL;
only trusted startup configuration selects the content backend.

`inspect(reference)` returns authorised metadata and associated link identities.
`status()` lists references readable by the principal, its own retention links
and the namespace's fixed policy. Publication of identical content and metadata
is idempotent, including after a restart. Different media types, input hashes or
read grants produce different reference identities. Identical bytes are stored
once within a namespace. There is no cross-namespace content lookup.

## Trust and scope

`principal` is an identity supplied by the trusted host. Qualified actor names
such as `coordinate/worker` and `review/checker` are preserved exactly in
producer identities, grants and retention ownership. They are opaque metadata;
namespace and retention-link identifiers remain path-safe components. The constructor is not
an authentication mechanism for remote callers, and the module does not isolate
malicious Python code running as the same operating-system user. Remote bridges
must resolve authenticated callers into configured principals before giving
access to the repository. Knowing a content hash or possessing a reference does
not grant access.

The producer always equals the publishing principal. Producers and explicitly
listed readers may read the object. Read grants are frozen; publishing another
reference with additional readers leaves earlier references unchanged. There is
no grant-revocation API in this version. Sensitive information therefore needs
an appropriate retention policy and protection of the private run directory.
Input hashes identify declared dependencies. The store verifies stored content;
it does not independently attest that the producer actually used those inputs.

A namespace is a locally configured scope with immutable quotas. Create its
configuration through trusted application code; allocating arbitrary new
namespaces is not a quota mechanism. Limits apply to object size, total physical
content bytes, manifest count and retention-link count. Crash-orphaned blobs and
pending files remain charged against the byte quota. One bounded pending object
may temporarily exist during publication. Metadata has a separate 16 MiB bound.
Publication is serialised across processes and atomically installs durable
content before its manifest. Unknown versions, inconsistent metadata and
corrupt content fail closed.

Within a deployment, new namespaces and publications also require its storage
admission thresholds to remain open. An identical retained publication is
verified and returned without allocating temporary content, even when admission
is closed. Reads and retention-state updates remain available so existing work
can be resolved. These are admission thresholds, not a physical byte quota;
existing writes still require free space. A file is fingerprinted before admission
and checked again while copying, so a changed source cannot publish mismatched
content.

## Retention and recovery

Place the root at `<run_dir>/artifacts`. For the local backend, deployment backup
includes objects and manifests. For S3, it includes metadata, object reservations
and exact backend identity; the separately protected bucket retains the bytes. Repository operations cooperate with the deployment
lock, so backup and run pruning cannot overlap publication or reads. Restored
stores require `readonly=True`; publication and retention changes are rejected
in audit-only restored deployments.

Explicit retention links protect results used beyond the producing run's
lifetime. Create a link before handing a reference to a downstream consumer.
Keep it `live` while the consumer is running or `unresolved` when its effects
cannot be determined. Only its creating principal can update it, and its set of
references is fixed. Release it when the owning application has established the
consumer's resolved terminal state:

```python
store.link("check-task-17", [reference], state="released")
```

Ordinary `prune_runs` retains a run containing any live or unresolved link and
blocks deletion when artefact retention metadata cannot be verified. Releasing
a link does not delete an object. Local objects disappear when the containing run becomes eligible for explicit
operational retention. S3 objects remain retained in their bucket; run pruning
does not issue remote deletion commands. Unlinked objects do
not extend a terminal run's lifetime. Applications remain responsible for
establishing and releasing links; the store does not infer lifecycle decisions
from model responses or elapsed time. Composition history also retains its
referenced run directories through the composition-store retention guard.

Standalone roots can be useful in embedding applications, but they do not
receive deployment backup or run-retention management automatically. The
`ArtifactRepository` is the common contract used by transfer, context selection
and workspace consumers. `ArtifactStore` owns grants and reference metadata for
both content strategies. Distributed consumers use authenticated bounded transfer;
S3 content storage retains the single-controller metadata ownership model.


## S3-compatible content storage

Install the optional, hash-locked SDK profile:

```sh
python -m pip install --require-hashes -r host/requirements-object-storage.txt
```

A trusted application selects the backend before constructing its repository:

```python
from mas_host.s3_artifacts import S3ArtifactContent
from mas_host.s3_transport import S3ContentConfig

backend = S3ArtifactContent(S3ContentConfig(
    endpoint="https://s3.eu-west-2.amazonaws.com",
    bucket="organisation-mas-artifacts",
    prefix="production",
    region="eu-west-2",
    credentials_file=Path("/run/secrets/mas-object-storage.json"),
))
store = ArtifactStore(Path(run_dir) / "artifacts", namespace="analysis-v1",
                      principal="analyst", content_backend=backend)
```

The credentials file is a private regular file owned by the service user, with
JSON fields `access_key_id`, `secret_access_key` and optional `session_token`.
The client reads it when accessing content, allowing credential rotation without
changing the object binding. Credentials, their path and SDK error bodies never
enter references, backend descriptors or general diagnostics. Management-only
inspection needs no SDK or credentials. The client uses the selected region,
path-style addressing, TLS verification, bounded connection/read timeouts, no
ambient proxies and no SDK retries. Cross-origin redirects are rejected before
sending credentials. An optional `ca_file` pins a private CA file by SHA-256.
Plain HTTP is available only with `allow_loopback_http=True` and a numeric
loopback endpoint for local tests. Other corporate transport profiles need
separate qualification.

The metadata owner pins the backend identity, a random repository identity and
per-content byte reservations in `object-content.json`. No configured backend
can silently replace an existing local namespace or another object-store
binding. Publication performs these ordered steps under the namespace lock:

1. Persist the content identity and byte reservation before any upload.
2. Copy the bounded source to a private snapshot and verify its complete SHA-256.
3. Conditionally create its derived S3 key with `If-None-Match: *` and SHA-256.
4. Read the full object back, check exact size/hash, then mark the reservation
   verified and publish the ordinary immutable reference.

A lost upload response leaves a charged reservation and no usable reference.
Repeating the same publication uses the same key; an existing key must pass full
content verification. An existing corrupt object is never overwritten. Different
principals/grants can reference the same immutable bytes without sharing access.
Reads authorise against the retained metadata before fetching content and expose
only a verified disk-backed snapshot. No object listing is needed for admission
or reads. The backend uses single-object PUT, capped at 5 GiB and the configured
per-object bound, so it creates no multipart-upload cleanup obligations. Existing
transfer-session expiry removes incomplete local transfer bytes.

Reserved and verified objects both consume namespace byte/count quota, including
uploads whose outcome or later metadata commit is uncertain. Reservations are
not erased automatically. Bucket credentials should permit only the configured
prefix, and bucket policy should prevent unrelated writers from mutating content.
The backend deliberately does not delete remote objects during run pruning:
retain the bucket, its access configuration and local manifests together; retire
an entire old repository prefix only after its consumers and recovery horizon
have ended. Do not configure an object expiry shorter than the host's retained
references. Deleting a retained object causes integrity/read failure, never
fallback to different bytes or an implicit provider rerun. Backup restoration
preserves exact object keys and stays audit-only; object-store replication and
controller failover remain separately qualified deployment responsibilities.

A bridge scope selects the same backend through `content_backend`:

```json
{
  "namespace": "analysis-v1",
  "principals": {"alice": "analyst", "bob": "checker"},
  "content_backend": {
    "type": "s3",
    "endpoint": "https://s3.eu-west-2.amazonaws.com",
    "bucket": "organisation-mas-artifacts",
    "prefix": "production",
    "region": "eu-west-2",
    "credentials_file": "/run/secrets/mas-object-storage.json"
  }
}
```

Changing the backend invalidates outstanding transfer sessions. Callers continue
to use the same HTTP/MCP chunk API; they receive no bucket credentials or signed
storage URLs. The workflow, workspace and scientific-array APIs are unchanged.

Source tests cover byte/grant equivalence with local storage, corrupt/oversized
reads, lost accepted uploads, metadata-commit crashes, quota preservation,
rebind rejection, context/workspace use and audit-only restore. A repeatable
integration harness creates and removes its own disposable bucket:

```sh
PYTHONPATH=host python scripts/check-object-storage.py --local-rustfs /path/to/rustfs
# An explicitly authorised isolated remote test account can instead run:
PYTHONPATH=host python scripts/check-object-storage.py \
  --endpoint https://objects.test.example --region eu-west-2 \
  --credentials-file /run/secrets/object-storage-test.json
```

The local integration passed with RustFS 1.0.1 and boto3/botocore 1.43.114:
conditional-write recovery after a lost response, complete hash checking,
authenticated multi-chunk HTTP upload/download, cross-principal denial,
revocation, restored metadata and server-side corruption rejection. This is
single-machine source-profile evidence. The deployment's AWS/vendor service,
TLS/private CA, replication, lifecycle and retained installed images still need
their declared W17 acceptance. The transport uses the official
[PutObject conditional-write contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
and [GetObject response contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).


## Authenticated cross-host transfer

`ArtifactTransferService` exposes opt-in chunk operations through authenticated
`POST /service/artifact/<operation>` and MCP `mas.artifact.<operation>` tools.
Operations are `upload_begin`, `upload_chunk`, `upload_commit`, `download_begin`,
`download_chunk`, `status`, `close` and `revoke`. Each binary chunk is at most
32 KiB, base64 encoded inside the existing bounded integer-JSON envelope. The
service never accepts a filesystem path or fetches a message-supplied URL.

Enable fixed repositories in the trusted bridge configuration:

```json
{
  "artifacts": {
    "ttl_seconds": 3600,
    "max_sessions": 16,
    "max_staging_bytes": 67108864,
    "scopes": {
      "analysis-results": {
        "namespace": "analysis-v1",
        "principals": {"alice": "analyst", "bob": "checker"},
        "policy": {
          "max_artifact_bytes": 16777216,
          "max_total_bytes": 67108864,
          "max_artifacts": 1024,
          "max_links": 4096
        }
      }
    }
  }
}
```

This is the `artifacts` field of the existing version-1 bridge configuration.
Each scope maps authenticated service principals to distinct local actor
identities. Aliasing two principals to one actor is rejected. The optional
scope `path` is relative to the deployment state directory; its default is
`shared-artifacts/<scope>`. The scope name, actor map, local quotas and paths are
operator configuration, never request authority. A scope rooted in a run's
`artifacts` directory participates in that run's retention; the default shared
root is included in deployment backup and storage admission but needs explicit
operator lifetime management. Upload publication does not infer a downstream
consumer's lifecycle or create a run-retention link on that consumer's behalf.

Upload and revocation operations require `mas:submit`; download, status and
close require `mas:read`. The authenticated principal must also appear in the
selected scope. Upload producers come from that mapping. The request's readers
are names in the same configured mapping, not self-asserted actor identities.
Download checks the exact stored reference and its existing grants. A content
hash, copied reference or guessed transfer identifier grants no access.

```python
from mas_host.bridges.artifact_client import ArtifactTransferClient
from mas_host.bridges.service_client import TaskServiceClient

peer = TaskServiceClient(
    "https://peer.example", principal="bob", token_file=private_token_file)
transfer = ArtifactTransferClient.from_service(peer)
# local_repository is the consumer's configured ArtifactStore.
receipt = transfer.materialize("analysis-results", remote_reference, local_repository)
local_reference = receipt["reference"]
```

`from_mcp(configured_mcp_client)` provides the same transfer operations. A2A
application data parts, MCP structured results, Temporal payloads and LangGraph
state can carry the unchanged `ArtifactRef.to_dict()` value. Their configured
consumer calls `materialize` explicitly through the selected HTTP/MCP peer.
There is no additional A2A RPC extension or automatic file-URL download.

Materialisation streams bounded chunks into a private temporary file and checks
the complete size and SHA-256 before publication. The local producer is the
local repository principal; the source content hash is an input dependency.
A second durable artefact records the exact source reference and configured
peer descriptor. A live retention link protects both local objects. Release
`receipt["link_id"]` with both `receipt["reference"]` and `receipt["receipt"]`
only after the consuming work is resolved terminal. Peer credentials and proxy
credentials never enter this provenance receipt.

`publish(scope, repository, reference, readers=(...))` uploads a verified local
object and returns the remote reference. Upload bytes remain unusable until an
exact-size/hash commit. Repeated identical chunks at the same offset and exact
commits are idempotent. Changed bytes conflict. The client does not retry failed
requests. If a chunk or commit reply is lost, `ArtifactTransferIncomplete`
contains `transfer_id`; retain it and call `inspect_transfer(transfer_id)`.
The owner can then repeat the exact chunk/commit rather than allocate another
upload. A lost initial begin reply leaves a bounded expiring reservation.

Sessions persist across service restarts and are bound to their original
principal and scope configuration. Changes to the scope invalidate old
sessions. Download sessions contain verified private snapshots. TTL expires
access; the next transfer operation cleans expired bytes. Staging reserves the
whole expected size before accepting content, with a separate session-count
bound. A private verification snapshot and local publication temporary can
add bounded transient disk use beyond the staging reservation; these limits are
not a filesystem quota. The client checks an overall transfer deadline between
requests, each of which also has its transport's hard deadline. Automatic
cross-session download resume is not implemented; explicit same-session offset
reads are available until expiry.

With the shared-authority profile, `SharedArtifactTransferService` retains the
session descriptor, authenticated owner, immutable scope binding, database-clock
expiry, acknowledged upload offsets and exact upload chunks in PostgreSQL.
Replacement controllers use the same transfer IDs, including after a lost chunk
or publication acknowledgement. Native transactions enforce the configured
global session and staging-byte limits, a maximum of 65,536 chunks and 2 GiB per upload,
and a 100,000-session retained-identity limit. Close and expiry release staging
bytes while retaining identity tombstones; no closed ID can be reopened.
Published references and verified content use the shared S3 repository.

Shared download descriptors survive replacement too. A controller may keep a
bounded, private, verified snapshot to avoid fetching the full object for each
chunk. This cache is disposable: a replacement verifies the original S3 object
again. Each read checks native ownership, scope, expiry, closure, grants and
controller epoch before returning bytes, including after S3 I/O. Shared transfer
configuration is immutable within its database namespace; changing it requires
an explicitly provisioned namespace, rather than silently remapping live sessions.
Across every configured scope exposing the same repository namespace, an actor
must map to one authenticated owner. Repeating that owner is allowed; assigning
the same actor to another owner through a second scope is rejected. Shared reads
also reject ambiguous mappings retained by an earlier writer before returning
cached or remote content.

Only the mapped producer can `revoke` future remote downloads for selected
readers. Revocation persists across restarts and applies to existing download
sessions as well as new ones. It does not rewrite immutable local manifests,
recall bytes already delivered or revoke a recipient's local copy. Revocation
records are bounded to 4,096 distinct grants and are included in backup.
Transfers, publication and revocation are disabled in audit-only restored
state; authorised local read-only inspection remains available.

## Scientific values without wire JSON floats

`mas_host.numeric_artifacts.put_float64` stores finite IEEE 754 binary64 arrays
as `application/vnd.mas.ndarray` artefacts. The `MASARRAY1` encoding contains a
bounded versioned header with dtype, little-endian byte order and integer shape,
followed by exactly the declared number of 8-byte values. The hash binds the
header and values together; mismatched dimensions and nonfinite values fail.
The protocol message still contains only the ordinary integer-JSON reference.

```python
from mas_host.numeric_artifacts import put_float64, open_float64

reference = put_float64(repository, [0.125, 3.141592653589793], shape=(2,))
with open_float64(repository, reference) as (shape, values):
    for value in values:
        consume(value)
```

This encoding preserves binary64 values; it does not claim arbitrary decimal
precision, units or coordinate semantics. Applications needing another encoding
can use a separately versioned media type under the same integrity and access
rules. Inside a MAS workflow, transfers belong in an admitted adapter invocation
with a frozen implementation and bounded transfer policy.

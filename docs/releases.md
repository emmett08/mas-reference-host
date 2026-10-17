# Releases and upgrades

The container build fixes Rust 1.99.0, Cargo.lock, both base-image manifests,
Alpine package archives, Python build tools and the complete OpenAI SDK dependency set. Python downloads
accept only the committed wheel hashes. The application wheel is built without
network access; the final image installs from the local wheel directory with
hash verification and no network. After installation and dependency checks, the
final image removes pip and the ensurepip bootstrap bundle. Runtime agents do
not need package installers; their vendored dependencies would otherwise add
unnecessary vulnerable code. Extend dependencies at image-build time through
the reviewed locks. Container acceptance checks that both installers are absent.
Build inputs are repeatable. Bit-for-bit image
reproducibility has not been established: build timestamps and the CI/build
infrastructure can still differ.

## Supported targets and lock maintenance

The container targets are CPython 3.14 on Alpine Linux amd64 and arm64. The runtime lock
contains musllinux hashes for both architectures, alongside glibc wheels for the
native Ubuntu test job. The audit-tool lock targets the amd64 CI
runner. The dependency-free embedded Python interface may work elsewhere, but
those platforms are outside these release checks.

To change dependencies, use a temporary CPython 3.14 virtual environment:

```sh
python3.14 -m venv .venv-release
. .venv-release/bin/activate
python -m pip install --require-hashes -r host/requirements-build.txt
# Update the explicit SDK/build versions in host/pyproject.toml first if needed.
python scripts/release-locks.py
python scripts/release-locks.py --check
```

The refresh command resolves wheel-only dependencies separately for both target
architectures and refuses conflicting versions. Review every version/hash
change, run all CI gates, and commit the lock updates together. Security-tool
versions are explicit in the refresh script and security workflow. Do not merge
a dependency-update PR until its regenerated locks pass the same checks.
For a Python ABI upgrade that should retain package versions, use
`python scripts/release-locks.py --keep-versions`; it downloads wheels for the
new ABI while requiring every currently locked package version.

Base-image tags identify the upstream release series; their digests identify the
actual downloaded manifests. Refresh them from Docker Hub using
`docker buildx imagetools inspect rust:1.99.0-alpine` and
`docker buildx imagetools inspect python:3.14-alpine`, then review both
architecture manifests and rerun image scanning. A fixed digest deliberately
does not receive security updates until changed. GitHub actions are pinned to
commits and must also be updated through review.

The production image uses Alpine 3.24, musl 1.2.6 and packaged SQLite 3.53.4.
Rust links both libc and SQLite dynamically, so the runtime package inventory
includes those libraries and receives their security advisories. The container
sets `RUSTFLAGS=-C target-feature=-crt-static` and
`LIBSQLITE3_SYS_USE_PKG_CONFIG=1`; the native Cargo build uses its bundled SQLite
dependency. The Rust builder and Python runtime both use pinned Alpine 3.24
images with matching development and runtime library archives.
Native amd64 and arm64 CI jobs check actual dynamic linkage, SQLite version and
the complete host test suite, including process-termination and storage-failure
tests, inside the installed container with networking disabled. Each job also
runs the native host suite and bounded mock workload. Architecture-specific
artefacts retain the test logs and workload report.

`docker/alpine-packages.json` fixes archive versions and SHA256 hashes separately
for builder/runtime and amd64/arm64. Downloads verify hashes; offline `apk`
installation also verifies package signatures. Refresh explicitly with
`python scripts/release-apk.py --lock docker/alpine-packages.json --refresh`.
Review package versions and dependencies, retain the corresponding base-image
series, and rerun all image/runtime gates. The package list is explicit: a new
dependency causes the offline build to fail until its archive is reviewed and
added. An archive disappearing upstream requires a reviewed refresh or an
independently retained copy with the same hash.

## Dependency batches and CI

Dependabot groups Cargo, pip, Docker and GitHub Actions version updates into one
weekly PR using [multi-ecosystem grouping](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/secure-your-dependencies/configuring-multi-ecosystem-updates).
Review the group as one compatible change: image upgrades can require matching
Alpine archives, Python wheel hashes, toolchains and application adjustments.
Grouping does not remove the security audit or its weekly schedule.

Each PR runs protocol, host/Compose and security workflows once. Standalone
push checks run only on `main`; branch pushes do not duplicate PR checks.
Version tags run those gates through the release workflow, without
also starting the standalone workflows. Superseded PR runs are cancelled;
release and main runs are retained. Distinct concurrency prefixes prevent the
three reusable workflows from cancelling one another.

The five JavaScript actions declare Node.js 24 and remain pinned to reviewed
commit hashes. The Trivy composite action's nested JavaScript actions also
declare Node.js 24. Hosted Ubuntu runners provide the runtime; self-hosted
substitutes require Actions runner 2.327.1 or later. Changing a job's application
Node version cannot change an action's declared JavaScript runtime.

## Candidate artefacts

The repository owner creates a `v0.MINOR.PATCH` tag at the reviewed commit to
trigger the release workflow. Manual dispatch must select an existing version
tag. Versions must use that exact numeric form, without a prerelease suffix or
leading zeroes. Branch dispatch is rejected before the expensive checks begin.
The workflow has read-only repository permissions and never creates, moves or
deletes a tag. It leaves repository branch and tag permissions unchanged.

The first job verifies that the selected lightweight or annotated tag resolves
to the event's exact source commit. Protocol/SPIN, host/Compose/acceptance and
security jobs then gate packaging. Each package job checks out that verified
source SHA and uses the verified version. A tag identifying a failed candidate
remains part of the release history; create a new version for a changed source.

For example, after reviewing and merging the intended source, replace the
version and full source SHA below with the chosen values, using the owner's
authenticated Git session:

```sh
git tag -a v0.1.4 FULL_REVIEWED_COMMIT_SHA -m 'MAS v0.1.4 candidate'
git push origin refs/tags/v0.1.4
```

Native jobs produce four packages: `standard` and `cursor`, each for `amd64`
and `arm64`. The standard variant builds `Dockerfile`; the Cursor variant builds
`Dockerfile.cursor`. Each produces its own OCI archive,
embedded BuildKit provenance, a CycloneDX SBOM, audit results, a source manifest
and `SHA256SUMS`. Before packaging is accepted, each final image must pass the
complete installed-container host suite and variant-specific runtime checks.
Standard images must use the expected shared musl/SQLite libraries. Cursor
images must run the installed native SDK bridge on glibc, verify its pinned Node
runtime and patched bridge source, and use the expected static Rust binaries.
These checks make no paid provider requests. The release
build exports the OCI archive once, with gzip layers and provenance. Skopeo
imports the requested architecture's runtime image from that archive into Docker;
there is no separate test-image build. A verifier checks descriptor hashes and
sizes, layer digests and uncompressed layer identities, platform selection and
provenance naming the runtime manifest. After import, Docker's image ID must
equal the archive's configuration digest and its ordered root filesystem layer
identities must match the archive configuration. The original archive checksum
must remain unchanged. This permits testing with Docker's classic image store
while retaining attestations in the distributed OCI archive.

For scanning, the verifier copies only validated regular OCI members into a
fresh directory, without applying tar paths or permissions. Trivy receives that
directory with a derived root index directly naming the verified runtime
manifest. Trivy 0.75 matches explicit references only among root descriptors;
it cannot resolve a nested leaf digest from the original root. Both
the vulnerability scan and SBOM use this input. Copied file hashes are rechecked
before packaging; the binding records original and derived index hashes, and
all blobs, the original archive and its provenance remain unchanged.
Layer hashing, decompression and copying use streams.
The release accepts its documented OCI layout, gzip or uncompressed
tar layers and supported SLSA provenance formats; unexpected formats fail the
gate. The artefacts include the verified archive/image binding, imported image
identity, Skopeo version and test log in the checksum manifest. Skopeo is a
recorded CI import tool, not an application dependency.
Workflow definitions establish these requirements; only successful executions
on the tagged commit establish that the candidate passed them.
The manifest records the variant, platform, selected Dockerfile and relevant
dependency/runtime locks, as well as the verified tested-image binding. It is an
unsigned identity record; it is not an independent attestation of correctness.
Action artefacts expire after 30 days. Publication is a separate owner action;
the workflow does not publish a release or push an image registry tag.

### Retain and publish the accepted packages

After every job succeeds on the intended tagged commit, the owner can distribute
the accepted bytes without giving a bot repository write access:

1. Download all four package artefacts from that workflow run:
   `mas-VERSION-standard-amd64`, `mas-VERSION-standard-arm64`,
   `mas-VERSION-cursor-amd64` and `mas-VERSION-cursor-arm64`, where `VERSION`
   includes the leading `v`. Keep each downloaded ZIP under its distinct
   artefact name; each contains a separate `SHA256SUMS` and source manifest.
2. Extract each ZIP into a separate directory and run `sha256sum -c SHA256SUMS`
   there. Check its manifest's source commit, tag, variant and platform against
   the intended release and the successful workflow. Retain the original ZIPs.
3. Using the owner's GitHub session, create a release **from the existing tag**
   and attach those four original ZIPs. Record the source SHA, workflow-run URL,
   supported deployment profiles and any outstanding live acceptance in the
   release notes. Mark an unaccepted deployment candidate as a prerelease.
4. Copy the same ZIPs and release notes into the organisation's retained release
   storage. Verify the extracted checksums there before the 30-day Actions
   retention expires. Periodically verify retrieval; a release page alone does
   not prove that a recovery copy remains available.

GitHub Release assets and retained operator storage provide distribution beyond
the Actions retention window. Use the image from its verified archive directly;
rebuilding the source would produce a new image requiring its own acceptance.

Before distribution, download the intended architecture's artefacts and run
`sha256sum -c SHA256SUMS`. Import or copy the OCI archive with a tool supporting
the OCI layout, retaining its digest and provenance. Record the tested image
digest in the deployment configuration. A release tag alone cannot establish
that an application's live-provider acceptance has passed.

### Candidate acceptance history

The [`v0.1.0` acceptance run](https://github.com/emmett08/mas/blob/main/evidence/readiness-ci-initial.json) passed its
protocol, host, both architecture and security gates, then failed packaging on
both architectures. The workflow requested both an OCI export with provenance
and Docker export from the same build; the Docker exporter rejected the
resulting manifest list. The tag remains attached to that failed candidate and
must not be rewritten. Candidate `v0.1.1` introduced the corrected single-archive
import path. Its separate arm64 container run exposed a
[crash-test synchronisation error](https://github.com/emmett08/mas/blob/main/evidence/readiness-ci-recovery-race.json):
provider death did not establish that the guardian had released run ownership.
The runtime correctly rejected premature recovery. Both final architecture
imports and 131-test container suites passed, but the
[scanner input gate failed](https://github.com/emmett08/mas/blob/main/evidence/readiness-ci-scanner-input.json) because
Trivy accepts an OCI directory rather than this OCI tar archive. Candidate
`v0.1.2` provides the verified directory input, waits for actual lock availability
and adds deterministic regressions for these boundaries.
Candidate `v0.1.2` passed all native, container and prerequisite gates, then
[failed scanner reference resolution](https://github.com/emmett08/mas/blob/main/evidence/readiness-ci-scanner-reference.json).
Candidate `v0.1.3` derives the scanner's direct root reference as described above
and includes its regression. Its [complete acceptance run](https://github.com/emmett08/mas/actions/runs/38478414137)
passed on both architectures at `67e95d6d476153fa174b21ce3d7564a61c6bda7b`,
including final package tests, scans, SBOM generation and uploads. Subsequent
dependency changes require their own acceptance evidence; the old tag's result
does not establish their behaviour.

The implementation follows Docker's [OCI exporter options](https://docs.docker.com/build/exporters/oci-docker/)
and [attestation storage format](https://docs.docker.com/build/metadata/attestations/attestation-storage/),
the [OCI image layout](https://github.com/opencontainers/image-spec/blob/main/image-layout.md),
Skopeo's [copy interface](https://github.com/containers/skopeo/blob/main/docs/skopeo-copy.1.md),
and Trivy's [OCI directory and digest selection](https://trivy.dev/docs/dev/guide/target/container_image/#oci-layout)
and [pinned 0.75.0 reference resolver](https://github.com/aquasecurity/trivy/blob/v0.75.0/pkg/fanal/image/oci.go).

## Upgrade and rollback

1. Stop admission of new jobs and inspect open tasks and unknown provider
   outcomes. Quiesce host runners before making a backup with the supported
   administration command. Keep the old image digest and configuration.
2. Restore the backup into a separate staging state directory. Restored state
   remains audit-only: use candidate inspection, reconciliation and cached
   completed-result replay to check it. Run the mocked acceptance harness in a
   separate, fresh state directory. Never invoke a provider for an unresolved
   call merely to test recovery. Review schema/version errors before upgrading
   production.
3. Run bounded live acceptance with a dedicated provider project, spending
   limits and an operator. Record actual provider/model identity and usage.
   The automated workflows never invoke the paid API.
4. Start the candidate with the production state volume and independent
   reconciliation watcher. Inspect budgets, disk headroom, expiries and errors.
   Keep the pre-upgrade backup until the acceptance window has closed.
5. If rollback is needed, stop the candidate first. Preserve its current state
   and restore the pre-upgrade backup into a separate directory for audit.
   Restore does not enable new admissions or undo remote effects. Reconcile
   calls made since the backup before making any retry decision, and retain
   both state copies. An older binary must never modify a database upgraded to
   a newer schema or reactivate a restored snapshot. A cutover that needs new
   work must use a separately authorised and budgeted fresh deployment, with
   the previous deployment's reserved expenditure and unresolved effects
   included in the operator's account-level accounting. Restoring a backup
   never replenishes the deployment's spending allowance.

The supplied Compose resource caps limit each container's CPU, memory, PID count
and log rotation. The named volume still requires host disk monitoring, backup
storage and operator-controlled retention. `compose.production.yaml` runs the
watcher independently; use `up -d reconciler`, then `run --rm mesh` for jobs.
Avoid `--abort-on-container-exit` for the watcher deployment because jobs finish
normally while reconciliation must continue.

These controls follow [pip's secure-install guidance](https://pip.pypa.io/en/stable/topics/secure-installs/)
and [Docker's Compose service controls](https://docs.docker.com/reference/compose-file/services/).

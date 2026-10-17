FROM python:3.14-alpine@sha256:f6a589d43c42b9e7f7dc67a12d37132491f362859a5d750607710cc56da3bc72 AS alpine-packages
ARG TARGETARCH
COPY docker/alpine-packages.json /build/alpine-packages.json
COPY scripts/release-apk.py /build/release-apk.py
RUN python /build/release-apk.py --lock /build/alpine-packages.json --arch "$TARGETARCH" --out /packages

FROM rust:1.99.0-alpine@sha256:0cce0a5e0e8ba67b455257a3a02a1d99005f382748789d6464460028810f1627 AS rust-build
COPY --from=alpine-packages /packages/build /packages
RUN --network=none apk add --no-network /packages/*.apk && rm -rf /packages
ENV RUSTFLAGS="-C target-feature=-crt-static" \
    LIBSQLITE3_SYS_USE_PKG_CONFIG=1
WORKDIR /build/rust-protocol
COPY .protocol/rust-protocol/Cargo.toml .protocol/rust-protocol/Cargo.lock ./
COPY .protocol/rust-protocol/src ./src
RUN cargo build --locked --release --bin mas --bin mas-gateway

FROM python:3.14-alpine@sha256:f6a589d43c42b9e7f7dc67a12d37132491f362859a5d750607710cc56da3bc72 AS python-build
WORKDIR /build/host
COPY host/pyproject.toml host/requirements-openai.txt host/requirements-bridges.txt host/requirements-build.txt ./
RUN python -m pip install --no-cache-dir --require-hashes -r requirements-build.txt \
    && python -m pip download --no-cache-dir --require-hashes --dest /wheels -r requirements-openai.txt -r requirements-bridges.txt
COPY host/LICENSE ./LICENSE
COPY host/mas_host ./mas_host
ARG MAS_SOURCE_COMMIT=""
ARG MAS_RELEASE_TAG=""
COPY --from=rust-build /build/rust-protocol/target/release/mas /build/binaries/mas
COPY --from=rust-build /build/rust-protocol/target/release/mas-gateway /build/binaries/mas-gateway
COPY .protocol/rust-protocol/Cargo.toml /build/rust-Cargo.toml
COPY scripts/write-runtime-provenance.py /build/write-runtime-provenance.py
COPY .protocol-revision /build/protocol-revision.txt
RUN --network=none python /build/write-runtime-provenance.py --host /build/host --rust-manifest /build/rust-Cargo.toml --binaries /build/binaries --source-commit "$MAS_SOURCE_COMMIT" --release-tag "$MAS_RELEASE_TAG" --protocol-revision-file /build/protocol-revision.txt
RUN --network=none python -m pip wheel --no-cache-dir --no-index --no-deps --no-build-isolation --wheel-dir /wheels . \
    && python -c 'from pathlib import Path; import hashlib; p=next(Path("/wheels").glob("mas_host-*.whl")); Path("/wheels/host-wheel.txt").write_text(str(p)+" --hash=sha256:"+hashlib.sha256(p.read_bytes()).hexdigest()+"\n")'

FROM python:3.14-alpine@sha256:f6a589d43c42b9e7f7dc67a12d37132491f362859a5d750607710cc56da3bc72
LABEL org.opencontainers.image.source="https://github.com/emmett08/mas-reference-host"
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MAS_BIN=/usr/local/bin/mas \
    MAS_GATEWAY_BIN=/usr/local/bin/mas-gateway \
    MAS_MODE=mock \
    MAS_STATE_DIR=/state \
    MAS_TOPOLOGY=/app/examples/host/mesh.json
COPY --from=alpine-packages /packages/runtime /packages
RUN --network=none apk add --no-network /packages/*.apk && rm -rf /packages
RUN addgroup -g 10001 mas \
    && adduser -D -H -u 10001 -G mas -h /state mas \
    && mkdir -p /state /app/examples \
    && chown mas:mas /state \
    && chmod 0700 /state
COPY --from=rust-build /build/rust-protocol/target/release/mas /usr/local/bin/mas
COPY --from=rust-build /build/rust-protocol/target/release/mas-gateway /usr/local/bin/mas-gateway
COPY --from=python-build /wheels /wheels
COPY host/requirements-openai.txt host/requirements-bridges.txt /wheels/
RUN --network=none python -m pip install --no-cache-dir --no-index --find-links=/wheels --require-hashes -r /wheels/requirements-openai.txt -r /wheels/requirements-bridges.txt \
    && python -m pip install --no-cache-dir --no-index --no-deps --require-hashes -r /wheels/host-wheel.txt \
    && python -m pip check \
    && python -m pip uninstall --yes pip \
    && python -c 'import ensurepip, shutil; shutil.rmtree(ensurepip.__path__[0])' \
    && rm -rf /wheels
COPY examples/host/mesh.json /app/examples/host/mesh.json
COPY examples/bridges /app/examples/bridges
COPY LICENSE THIRD_PARTY_LICENSE.txt /usr/share/doc/mas/
WORKDIR /app
USER 10001:10001
ENTRYPOINT ["python", "-m", "mas_host"]

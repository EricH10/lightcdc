# syntax=docker/dockerfile:1.7
FROM rust:1.97-bookworm AS builder

ENV RUSTUP_TOOLCHAIN=1.89.0
WORKDIR /workspace
COPY . .
RUN rustc --version \
    && cargo build --locked --release --bin lightcdc

FROM debian:bookworm-slim AS runtime

LABEL org.opencontainers.image.source="https://github.com/EricH10/lightcdc" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 lightcdc \
    && useradd --uid 10001 --gid lightcdc --no-create-home --home-dir /var/lib/lightcdc lightcdc \
    && install --directory --owner lightcdc --group lightcdc /var/lib/lightcdc /etc/lightcdc

COPY --from=builder /workspace/target/release/lightcdc /usr/local/bin/lightcdc
COPY LICENSE-MIT LICENSE-APACHE /usr/share/licenses/lightcdc/

USER 10001:10001
WORKDIR /var/lib/lightcdc
VOLUME ["/var/lib/lightcdc"]
EXPOSE 50051 9187
STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/lightcdc"]
CMD ["--help"]

# Builds one of the workspace binaries: --build-arg BIN=ingester|gateway.
FROM rust:1.98.1-bookworm AS build
ARG BIN
WORKDIR /src
# The gateway depends on the Sui monorepo (git, large): the git CLI fetches it
# much faster than libgit2, and the cache mount keeps it between builds.
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p checkpoint-${BIN} \
 && cp target/release/checkpoint-${BIN} /usr/local/bin/app

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/app /usr/local/bin/app
ENV LOG_FORMAT=json
# Few malloc arenas: large short-lived buffers across many threads otherwise
# inflate RSS.
ENV MALLOC_ARENA_MAX=2
ENTRYPOINT ["/usr/local/bin/app"]

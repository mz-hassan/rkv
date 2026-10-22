# Build:  docker build -t rkv:dev -f deploy/docker/Dockerfile .      (from the repo root)
FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler clang libclang-dev cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release -p kv-server -p pd -p kvctl -p ycsb -p chaos -p sim

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/rkv-server /src/target/release/rkv-pd /src/target/release/kvctl /src/target/release/rkv-bench /src/target/release/rkv-chaos /src/target/release/rkv-sim /usr/local/bin/
COPY deploy/docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh && mkdir -p /data
EXPOSE 2379 20160 20180
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

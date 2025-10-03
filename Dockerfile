FROM rust:alpine AS builder
WORKDIR /app

RUN rustup install nightly
RUN rustup default nightly
RUN rustup component add rustfmt --toolchain nightly

RUN echo "[source.crates-io]" >> /usr/local/cargo/config.toml
RUN echo "replace-with = 'rsproxy-sparse'" >> /usr/local/cargo/config.toml
RUN echo '[source.rsproxy]' >> /usr/local/cargo/config.toml
RUN echo 'registry = "https://rsproxy.cn/crates.io-index"' >> /usr/local/cargo/config.toml
RUN echo '[source.rsproxy-sparse]' >> /usr/local/cargo/config.toml
RUN echo 'registry = "sparse+https://rsproxy.cn/index/"' >> /usr/local/cargo/config.toml
RUN echo '[registries.rsproxy]' >> /usr/local/cargo/config.toml
RUN echo 'index = "https://rsproxy.cn/crates.io-index"' >> /usr/local/cargo/config.toml
RUN echo "[net]" >> /usr/local/cargo/config.toml
RUN echo "git-fetch-with-cli = true" >> /usr/local/cargo/config.toml

RUN sed -i 's#https\?://dl-cdn.alpinelinux.org/alpine#https://mirrors.tuna.tsinghua.edu.cn/alpine#g' /etc/apk/repositories
RUN apk add --no-cache git gcc musl-dev make protoc protobuf-dev

COPY . .

RUN cargo build --release

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/server /app/

RUN mkdir -p /etc/bedrock
COPY test/test1/test.toml /etc/bedrock/dataserver.toml

RUN mkdir -p /data/wal /data/data

EXPOSE 8888

CMD ["/app/server"]

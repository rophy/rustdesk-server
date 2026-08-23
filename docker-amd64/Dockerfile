FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev pkgconf openssl-dev openssl-libs-static file make gcc g++

WORKDIR /build
COPY . .
RUN cargo build --release --all-features

FROM scratch
COPY --from=builder /build/target/release/hbbs /usr/bin/hbbs
COPY --from=builder /build/target/release/hbbr /usr/bin/hbbr
WORKDIR /data
ENV HOME=/data
USER 1000:1000

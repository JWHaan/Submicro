# Shopping Sessions — high-performance Rust solution.
# The scorers are linux/amd64 (Intel Xeon) and build this file natively.

# ---- build stage: static musl binary ---------------------------------------
FROM rust:slim-bookworm AS build
RUN rustup target add x86_64-unknown-linux-musl \
 && apt-get update \
 && apt-get install -y --no-install-recommends musl-tools \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --target x86_64-unknown-linux-musl \
 && strip target/x86_64-unknown-linux-musl/release/shopping-sessions

# ---- runtime stage: minimal image, static binary + shell for run.sh --------
FROM alpine:3.20
# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN adduser -D -H -u 1000 -h /scratch app

COPY --from=build /build/target/x86_64-unknown-linux-musl/release/shopping-sessions /app/shopping-sessions
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh /app/shopping-sessions

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and /output and /scratch are the only writable
# places. run.sh points every temp and cache directory at /scratch.
WORKDIR /app
CMD ["/app/run.sh"]

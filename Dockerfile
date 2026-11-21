# Shopping Sessions — high-performance Rust solution.
# The scorers are linux/amd64 (Intel Xeon) and build this file natively.

# ---- build stage: static musl binary, PGO-trained --------------------------
FROM rust:slim-bookworm AS build
RUN rustup target add x86_64-unknown-linux-musl \
 && rustup component add llvm-tools-preview \
 && apt-get update \
 && apt-get install -y --no-install-recommends musl-tools \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Training data: synth emits the same generator shape as the scorer's data.
# 3M events + 750k clicks is enough for branch coverage (hansliu-dev recipe).
RUN RUSTFLAGS="-C target-cpu=x86-64-v3" \
    cargo build --release --locked --target x86_64-unknown-linux-musl --bin synth \
 && ./target/x86_64-unknown-linux-musl/release/synth \
      --events 3000000 --clicks 750000 --users 300000 --seed 42 --output /pgo/train
# Instrumented build -> training run -> merge -> profile-use rebuild.
RUN RUSTFLAGS="-C target-cpu=x86-64-v3 -Cprofile-generate=/pgo/prof" \
    cargo build --release --locked --target x86_64-unknown-linux-musl --bin shopping-sessions \
 && mkdir -p /pgo/prof /out \
 && cd /pgo/prof \
 && LLVM_PROFILE_FILE='%m_%p.profraw' \
    DATA_DIR=/pgo/train OUTPUT_DIR=/out SCRATCH_DIR=/tmp WRITE_CSV=1 \
    /build/target/x86_64-unknown-linux-musl/release/shopping-sessions \
 && $(rustc --print sysroot)/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata \
      merge -o /pgo/merged.profdata /pgo/prof \
 && cd /build \
 && RUSTFLAGS="-C target-cpu=x86-64-v3 -Cprofile-use=/pgo/merged.profdata -Cprofile-correction" \
    cargo build --release --locked --target x86_64-unknown-linux-musl \
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

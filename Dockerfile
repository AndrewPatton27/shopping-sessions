# Shopping Sessions: native Rust pipeline (std only, no crates, no network needed).
# The scorers are linux/amd64 Intel Xeons with AVX-512, so x86-64-v3 (AVX2) is safe.
# Fallback Polars image: Dockerfile.polars.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY native/Cargo.toml native/Cargo.lock ./
COPY native/src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked

FROM debian:bookworm-slim
# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
# Scorer sets DATA_DIR, OUTPUT_DIR, SCRATCH_DIR, NCPU; the binary defaults to /data and /output.
WORKDIR /app
CMD ["/app/shopping-sessions"]

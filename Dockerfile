FROM rust:1-slim-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && printf 'extern crate memchr; extern crate memmap2; extern crate rayon; fn main() {}\n' > src/main.rs && cargo build --release --locked && rm src/main.rs
COPY src ./src
RUN touch src/main.rs && cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions
USER 1000:1000
WORKDIR /scratch
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch NCPU=8
ENTRYPOINT ["/usr/local/bin/shopping-sessions"]

FROM rust:1.100.0-slim-bookworm@sha256:476f6598b8d14a1f1bc7e58efc67af206ac8a559c37a3401ce512db2582d0ff7 AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && printf 'extern crate memchr; extern crate memmap2; extern crate rayon; fn main() {}\n' > src/main.rs && cargo build --release --locked && rm src/main.rs
COPY src ./src
RUN touch src/main.rs && cargo build --release --locked

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
COPY --from=build /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions
USER 1000:1000
WORKDIR /scratch
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch NCPU=8
# madvise(MADV_HUGEPAGE) malloc's heaps and grow them 256 MB at a time: ~10x fewer page faults and TLB misses.
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1:glibc.malloc.top_pad=268435456
ENTRYPOINT ["/usr/local/bin/shopping-sessions"]

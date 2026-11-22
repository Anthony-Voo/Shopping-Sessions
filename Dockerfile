# DIAGNOSTIC BUILD (scoring-environment probe): NOT an entry. It runs several builds of the
# pipeline on data replicated from the dev set, prints a summary and exits 3 on purpose.
FROM rust:1.90-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY vendor ./vendor
COPY pgo ./pgo
# earlier versions as comparison binaries, each built exactly as it was submitted
COPY probe ./probe
RUN mkdir /bins && for v in v59 v63 v65; do \
      rm -rf src && cp -r probe/$v/src src && \
      RUSTFLAGS="-C target-cpu=cascadelake -C relocation-model=static -Cprofile-use=/src/probe/$v/merged.profdata" \
        cargo build --release --locked --offline --target-dir /tmp/told && \
      strip /tmp/told/release/sessions && cp /tmp/told/release/sessions /bins/sessions_$v || exit 1; \
    done; rm -rf src /tmp/told
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=cascadelake -C relocation-model=static -Cprofile-use=/src/pgo/merged.profdata"
RUN touch src/*.rs && cargo build --release --locked --offline && strip target/release/sessions

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /bins /app/bins
COPY --from=build /src/target/release/sessions /app/sessions
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1:glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=17179869184
ENV SESSIONS_PROBE=1
USER 1000
CMD ["/app/sessions"]

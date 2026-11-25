FROM docker.io/library/rust:1-alpine AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release --locked

FROM scratch
COPY --from=build /build/target/release/collie-whistle /collie-whistle
ENV WHISTLE_LISTEN=0.0.0.0:8080
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/collie-whistle"]

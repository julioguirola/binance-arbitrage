FROM node:24-alpine3.21 AS frontendbuilder

WORKDIR /app

COPY frontend/package.json .
COPY frontend/package-lock.json .

RUN npm i --verbose

COPY frontend .

RUN npm run build

FROM rust:slim-trixie AS backendbuilder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.* .

COPY src src

RUN cargo build --release

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=backendbuilder /app/target/release/rust-ws ./rust-ws
COPY --from=frontendbuilder /app/dist ./frontend/dist

EXPOSE 8080

CMD [ "./rust-ws" ]
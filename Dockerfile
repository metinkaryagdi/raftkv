# syntax=docker/dockerfile:1

# --- build stage: compile a static Linux binary ---
FROM golang:1.26 AS build
WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO off -> a fully static binary that runs on a scratch/alpine base.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/raftkv ./cmd/raftkv

# --- runtime stage: tiny image with a shell + curl for poking the API ---
FROM alpine:3.20
RUN apk add --no-cache curl
COPY --from=build /out/raftkv /usr/local/bin/raftkv

# Client HTTP API and inter-node gRPC (same ports in every container; Compose
# maps the HTTP port to a distinct host port per node).
EXPOSE 8001 9001
ENTRYPOINT ["raftkv"]

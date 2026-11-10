# syntax=docker/dockerfile:1
# openflux-node: the OpenFlux node agent (ofnode) and the core, one container
# per node. Build from the repository root:
#   docker build -f deploy/ofnode/Dockerfile -t openflux-node .
# Run it with deploy/ofnode/docker-compose.yml (SECRET_KEY from the augmenter).

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
# go.mod replaces pion/sctp with ./third_party/sctp: it must be there before
# the module graph is resolved.
COPY third_party ./third_party
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/openflux . && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ofnode ./cmd/ofnode

FROM alpine:3.22
# ca-certificates: every carrier is TLS to Yandex/Mail.ru; iptables: the
# RST-drop rule an l3 channel needs (host network, NET_ADMIN).
RUN apk add --no-cache ca-certificates iptables
COPY --from=build /out/openflux /usr/local/bin/openflux
COPY --from=build /out/ofnode /usr/local/bin/ofnode
VOLUME /var/lib/openflux-node
ENTRYPOINT ["/usr/local/bin/ofnode", "serve"]

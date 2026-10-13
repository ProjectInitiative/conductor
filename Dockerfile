# syntax=docker/dockerfile:1.7
# BuildKit sets TARGETOS/TARGETARCH for each requested platform. The resulting
# scratch image contains only the static controller binary: no shell, kubectl,
# or host-specific runtime is required.
FROM --platform=$BUILDPLATFORM golang:1.25.5-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY scheduler/go.mod scheduler/go.sum ./
RUN go mod download
COPY scheduler/ ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags='-s -w' -o /out/conductor ./cmd/conductor

FROM scratch
COPY --from=build /out/conductor /bin/conductor
USER 65532:65532
ENTRYPOINT ["/bin/conductor"]

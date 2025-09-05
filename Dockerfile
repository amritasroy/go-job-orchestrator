# Build
FROM golang:1.22 AS build
WORKDIR /app
COPY go.mod .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/server main.go

# Runtime
FROM alpine:3.20

# Create non-root user and prepare writable data dir as root
RUN adduser -D -u 10001 app \
    && mkdir -p /data \
    && chown -R app:app /data

# Drop privileges after filesystem prep
USER app
WORKDIR /
COPY --from=build /out/server /server

VOLUME ["/data"]
EXPOSE 8080
ENV ORCH_BASE_URL=http://job-orchestrator:8080
ENTRYPOINT ["/server"]

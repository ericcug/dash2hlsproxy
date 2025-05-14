# ---- Build Stage ----
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files to download dependencies
COPY go.mod ./
# COPY go.sum ./ # Uncomment if you have a go.sum file
RUN go mod download

# Copy the source code
COPY . .

# Build the application
# CGO_ENABLED=0 for a static binary (smaller image)
# -ldflags="-s -w" to strip debug information and further reduce size
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags="-s -w" -o dash2hlsproxy main.go

# ---- Release Stage ----
FROM alpine:latest

WORKDIR /app

# Copy the pre-built binary from the builder stage
COPY --from=builder /app/dash2hlsproxy .

# Expose port 8080 (default, can be overridden by D2H_LISTEN_ADDR)
EXPOSE 8080

# Environment variables for configuration
# D2H_CHANNELS_JSON_PATH: Path to the channels configuration file (e.g., /config/channels.json)
# D2H_LISTEN_ADDR: Address and port to listen on (e.g., :8080)

# Set the entrypoint for the container
ENTRYPOINT ["/app/dash2hlsproxy"]

# Optional: Default command if needed, but ENTRYPOINT is usually sufficient for Go apps.
# CMD ["--config", "/app/config/channels.json"] # Example, if you want a default config path if D2H_CHANNELS_JSON_PATH is not set
# However, our Go app now handles default config path internally if env var is not set.

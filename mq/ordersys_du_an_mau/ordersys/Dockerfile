# syntax=docker/dockerfile:1
# Build một lần, sinh cả 3 binary; image chạy chọn binary qua ARG SERVICE.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12:nonroot
ARG SERVICE
COPY --from=build /out/${SERVICE} /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]

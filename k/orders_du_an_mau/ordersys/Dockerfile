# Build một trong ba dịch vụ: docker build --build-arg SERVICE=order-api -t ordersys/order-api .
FROM golang:1.27 AS build
ARG SERVICE=order-api
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]

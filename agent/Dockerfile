FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/burstforge ./cmd/burstforge

FROM alpine:3.20
RUN adduser -D -u 10001 bf
COPY --from=build /out/burstforge /usr/local/bin/burstforge
USER bf
WORKDIR /results
ENTRYPOINT ["burstforge"]

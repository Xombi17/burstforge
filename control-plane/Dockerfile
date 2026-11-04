# Built from the repository root (it needs the Go sources for the
# burstforge binary, which validates scenarios and owns the schema):
#   docker build -f control-plane/Dockerfile .
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY agent/go.mod agent/go.sum ./
RUN go mod download
COPY agent/ .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/burstforge ./cmd/burstforge

FROM node:22-alpine
WORKDIR /app
COPY control-plane/package.json control-plane/package-lock.json ./
RUN npm ci --omit=dev
COPY control-plane/src ./src
COPY --from=build /out/burstforge /usr/local/bin/burstforge
ENV BURSTFORGE_BIN=/usr/local/bin/burstforge HOST=0.0.0.0 PORT=8090
USER node
EXPOSE 8090
CMD ["node", "src/server.js"]

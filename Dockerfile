# One image, three binaries (api, mockpsp, webhooksink); docker-compose picks the command.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mockpsp ./cmd/mockpsp \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/webhooksink ./cmd/webhooksink

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget && adduser -D -u 10001 app
COPY --from=build /out/ /app/
USER app
EXPOSE 8080 8081 9000
CMD ["/app/api"]

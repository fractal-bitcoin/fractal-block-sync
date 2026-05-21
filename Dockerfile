FROM golang:1.25-alpine AS build

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN apk add --no-cache ca-certificates

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
	go build -trimpath -ldflags="-s -w" -o /out/fractal-block-sync ./cmd/fractal-block-sync

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata \
	&& addgroup -S fractal-block-sync \
	&& adduser -S -G fractal-block-sync -h /data fractal-block-sync \
	&& mkdir -p /data/.bitcoin \
	&& chown -R fractal-block-sync:fractal-block-sync /data

ENV TZ=Asia/Hong_Kong

WORKDIR /data

COPY --from=build /out/fractal-block-sync /usr/local/bin/fractal-block-sync

USER fractal-block-sync

ENTRYPOINT ["fractal-block-sync"]
CMD ["--help"]

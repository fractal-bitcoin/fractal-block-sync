# fractal-block-sync

`fractal-block-sync` is a Go CLI and library for accelerating Bitcoin-compatible block synchronization with `bitcoind` RPC and Cloudflare R2.

It supports two roles:

- Provider: read raw blocks from a local full node and upload block objects plus stable historical range indexes to R2.
- User: download missing blocks from a public R2 URL, verify the raw block hash, and submit the block to a local node through RPC.

## Build

```bash
go build -o fractal-block-sync ./cmd/fractal-block-sync
```

For local development, commands can also be run with:

```bash
go run ./cmd/fractal-block-sync --help
```

## Docker

Build the image:

```bash
docker build -t fractal-block-sync:local .
```

Run one command directly:

```bash
docker run --rm fractal-block-sync:local --help
```

When the local bitcoind RPC server runs on the Docker host, use `host.docker.internal` from the container:

```bash
docker run --rm \
  -v "$HOME/.bitcoin/.cookie:/data/.bitcoin/.cookie:ro" \
  fractal-block-sync:local submit \
  --base-url https://test-fractal-blocks.fractalbitcoin.io \
  --rpc-url http://host.docker.internal:8332 \
  --cookie-file /data/.bitcoin/.cookie
```

## Docker Compose

The compose file defines two profiles:

- `upload`: provider mode, uploads blocks and stable range indexes to R2.
- `submit`: user mode, downloads and submits missing blocks.

Provider example:

```bash
ENDPOINT_URL="https://<account-id>.r2.cloudflarestorage.com" \
ACCESS_KEY_ID="<r2-access-key-id>" \
SECRET_ACCESS_KEY="<r2-secret-access-key>" \
BUCKET_NAME="<bucket-name>" \
BITCOIN_COOKIE_FILE="$HOME/.bitcoin/.cookie" \
RPC_URL="http://host.docker.internal:8332" \
docker compose --profile upload up --build
```

User example:

```bash
BASE_URL="https://test-fractal-blocks.fractalbitcoin.io" \
BITCOIN_COOKIE_FILE="$HOME/.bitcoin/.cookie" \
RPC_URL="http://host.docker.internal:8332" \
docker compose --profile submit up --build
```

For RPC basic auth instead of cookie auth, set `RPC_USER` and `RPC_PASSWORD`. The compose services run with `--follow` enabled.

## R2 Object Layout

Block objects are addressed by block hash:

```text
blocks/{hash}.blk
```

Historical range indexes are fixed-size binary files:

```text
index/range/v1/size-{range_size}/{start_height}.bin
```

With the default range size, examples are:

```text
index/range/v1/size-2880/0000000000.bin
index/range/v1/size-2880/0000002880.bin
index/range/v1/size-2880/0000005760.bin
```

Each range index contains exactly `range_size` block hashes. Hashes are stored as 32 raw bytes in RPC display order.

## Provider Setup

The upload command writes to Cloudflare R2 through the S3-compatible API. Set these environment variables:

```bash
export ENDPOINT_URL="https://<account-id>.r2.cloudflarestorage.com"
export ACCESS_KEY_ID="<r2-access-key-id>"
export SECRET_ACCESS_KEY="<r2-secret-access-key>"
export BUCKET_NAME="<bucket-name>"
```

The provider node must expose bitcoind RPC. Cookie authentication is supported:

```bash
./fractal-block-sync upload \
  --rpc-url http://127.0.0.1:8332 \
  --cookie-file ~/.bitcoin/.cookie \
  --from-height 0 \
  --range-size 2880 \
  --stable-delay 2880
```

To keep polling and uploading:

```bash
./fractal-block-sync upload \
  --rpc-url http://127.0.0.1:8332 \
  --cookie-file ~/.bitcoin/.cookie \
  --from-height 0 \
  --follow
```

The provider uploads every block from `--from-height` through the current tip. Range indexes are only published for complete ranges whose blocks are older than `--stable-delay`.

## User Setup

The submit command only needs a public R2 download base URL and local bitcoind RPC access.

```bash
./fractal-block-sync submit \
  --base-url https://test-fractal-blocks.fractalbitcoin.io \
  --rpc-url http://127.0.0.1:8332 \
  --cookie-file ~/.bitcoin/.cookie \
  --range-size 2880 \
  --recent-walk-limit 2880
```

To keep submitting blocks as local headers arrive:

```bash
./fractal-block-sync submit \
  --base-url https://test-fractal-blocks.fractalbitcoin.io \
  --rpc-url http://127.0.0.1:8332 \
  --cookie-file ~/.bitcoin/.cookie \
  --follow
```

For each next missing block, the client:

1. Calls `getblockchaininfo`.
2. Uses a range index if it exists.
3. Falls back to walking local headers with `getchaintips` and `getblockheader` if the range index returns 404.
4. Downloads `blocks/{hash}.blk`.
5. Calculates the raw block hash locally.
6. Calls `submitblock` only if the calculated hash matches the target hash.

If `blocks + 1 > headers`, the command waits for local header sync when `--follow` is enabled. Without `--follow`, it exits after reporting that headers are not ready.

## Authentication Flags

Both commands support cookie auth:

```bash
--cookie-file ~/.bitcoin/.cookie
```

They also support explicit basic auth:

```bash
--rpc-user <user> --rpc-password <password>
```

If both are provided, explicit `--rpc-user` and `--rpc-password` take precedence.

## Command Flags

Upload flags:

```text
--rpc-url        bitcoind RPC URL, default http://127.0.0.1:8332
--cookie-file    bitcoind cookie file
--rpc-user       bitcoind RPC username
--rpc-password   bitcoind RPC password
--from-height    first height to upload, default 0
--range-size     range index size, default 2880
--stable-delay   stable block delay before publishing range indexes, default 2880
--follow         keep polling and uploading
--interval       follow polling interval, default 30s
```

Submit flags:

```text
--base-url           public R2 download base URL
--rpc-url            bitcoind RPC URL, default http://127.0.0.1:8332
--cookie-file        bitcoind cookie file
--rpc-user           bitcoind RPC username
--rpc-password       bitcoind RPC password
--range-size         range index size, default 2880
--recent-walk-limit  maximum recent header walk, default 2880
--follow             keep submitting as headers arrive
--interval           follow polling interval, default 10s
```

## Testing

Run all tests with a project-local Go build cache:

```bash
GOCACHE=$(pwd)/.gocache go test ./...
```

R2 integration tests are skipped unless the R2 environment variables are set.

## Troubleshooting

`missing required environment variables`: set `ENDPOINT_URL`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, and `BUCKET_NAME` before running `upload`.

`base url is required`: pass `--base-url` to `submit`.

`waiting for local headers`: the local node has not synced enough headers. Wait for P2P header sync or run with `--follow`.

`no range index and header walk exceeds limit`: the requested block is not covered by a published historical range index, and the local header walk exceeded `--recent-walk-limit`. Wait for the provider to publish the range index or increase `--recent-walk-limit`.

`downloaded block hash mismatch`: the downloaded block does not match the target hash and is not submitted.

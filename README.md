# fractal-block-sync

`fractal-block-sync` is a block sync helper for Fractal/Bitcoin-compatible nodes. It mirrors raw blocks to Cloudflare R2 and lets other nodes download, verify, and submit missing blocks through local RPC.

It only handles block transfer and verification. It does not parse transactions, build indexes, or process BRC20, inscriptions, Ordinals, or other protocol data.

中文文档见 [README-CN.md](./README-CN.md).

## Modes

- `upload`: read blocks from a synced local node, upload block objects to R2, and publish range indexes.
- `submit`: download missing blocks from R2, verify the block hash, and submit them to a local node.

## Build

```bash
go build -o fractal-block-sync ./cmd/fractal-block-sync
docker build -t fractal-block-sync:local .
```

## Upload

Docker Compose:

```bash
ENDPOINT_URL="https://<account-id>.r2.cloudflarestorage.com" \
ACCESS_KEY_ID="<r2-access-key-id>" \
SECRET_ACCESS_KEY="<r2-secret-access-key>" \
BUCKET_NAME="<bucket-name>" \
RPC_URL="http://host.docker.internal:8332" \
BITCOIN_COOKIE_FILE="$HOME/.bitcoin/.cookie" \
RANGE_SIZE=2880 \
STABLE_DELAY=2880 \
docker compose --profile upload up --build
```

Binary:

```bash
ENDPOINT_URL="https://<account-id>.r2.cloudflarestorage.com" \
ACCESS_KEY_ID="<r2-access-key-id>" \
SECRET_ACCESS_KEY="<r2-secret-access-key>" \
BUCKET_NAME="<bucket-name>" \
./fractal-block-sync upload \
  --rpc-url http://127.0.0.1:8332 \
  --cookie-file ~/.bitcoin/.cookie \
  --range-size 2880 \
  --stable-delay 2880 \
  --follow
```

## Submit

Docker Compose:

```bash
BASE_URL="https://<public-r2-domain>" \
RPC_URL="http://host.docker.internal:8332" \
BITCOIN_COOKIE_FILE="$HOME/.bitcoin/.cookie" \
RANGE_SIZE=2880 \
docker compose --profile submit up --build
```

Binary:

```bash
./fractal-block-sync submit \
  --base-url https://<public-r2-domain> \
  --rpc-url http://127.0.0.1:8332 \
  --cookie-file ~/.bitcoin/.cookie \
  --range-size 2880 \
  --follow
```

If the local node has no headers yet and R2 range indexes already exist, enable bootstrap:

```bash
BOOTSTRAP_FROM_R2=true docker compose --profile submit up --build
```

Binary flag:

```bash
--bootstrap-from-r2
```

## Authentication

Use one of:

```text
BITCOIN_COOKIE_FILE / --cookie-file
RPC_USER + RPC_PASSWORD / --rpc-user + --rpc-password
```

## Key Settings

Upload environment variables:

```text
ENDPOINT_URL       R2 endpoint
ACCESS_KEY_ID     R2 access key
SECRET_ACCESS_KEY R2 secret key
BUCKET_NAME       R2 bucket
FROM_HEIGHT       first height to upload, default 0
RANGE_SIZE        range index size, default 2880
STABLE_DELAY      blocks near tip excluded from range indexes, default 2880
UPLOAD_WORKERS    parallel upload workers, default 4
UPLOAD_INTERVAL   polling interval, default 30s
```

Submit environment variables:

```text
BASE_URL           public R2 download URL
RANGE_SIZE         must match uploader
RECENT_WALK_LIMIT  local header walk limit, default 2880
BOOTSTRAP_FROM_R2  use R2 indexes when local headers are unavailable
SUBMIT_INTERVAL    polling interval, default 10s
```

For CLI usage, use the equivalent kebab-case flags, for example `RANGE_SIZE` -> `--range-size`.

## R2 Layout

```text
blocks/{hash}.blk
index/range/v1/size-{range_size}/{start_height}.bin
```

Range indexes are published only for complete stable ranges:

```text
stableTip = localTip - STABLE_DELAY
```

## Test Chain

For small test chains, use the same small range on uploader and submitter:

```bash
RANGE_SIZE=100
STABLE_DELAY=0
BOOTSTRAP_FROM_R2=true
```

Check an index:

```bash
curl -I "$BASE_URL/index/range/v1/size-100/0000000000.bin"
```

## Testing

```bash
GOCACHE=$(pwd)/.gocache go test ./...
```

## Troubleshooting

- `missing required environment variables`: set the R2 variables for `upload`.
- `base url is required`: set `BASE_URL` or pass `--base-url`.
- `waiting for local headers`: wait for header sync or enable `BOOTSTRAP_FROM_R2=true`.
- `waiting for local headers or R2 range index`: the target range index is not available from `BASE_URL`.
- `downloaded block hash mismatch`: the downloaded block does not match the expected hash and is not submitted.


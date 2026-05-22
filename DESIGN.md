# Bitcoind R2 Block Sync Helper Design

## Goal

Build a Go CLI and library that helps service providers and users accelerate
Bitcoin-compatible block synchronization with `bitcoind` RPC and Cloudflare R2.

The tool supports two roles:

- Provider: reads blocks from a local full node through RPC and uploads raw block
  objects plus stable historical hash indexes to R2.
- User: detects missing or slow blocks from a local node, downloads block objects
  from a public R2 URL, verifies them, and submits them to the local node through
  RPC.

Core principles:

- Store one block per R2 object.
- Address block objects by block hash, not height.
- Use compact height range hash indexes only for stable historical blocks.
- Treat existing range index bins as completed-range markers for provider resume.
- Do not create separate progress or manifest objects.
- Resolve recent block hashes from the user's local headers chain.
- Do not require `manifest.json`.
- Let users configure only a public download base URL plus local bitcoind RPC
  access.

## R2 Object Layout

Public base URL example:

```text
https://test-fractal-blocks.fractalbitcoin.io
```

Block object:

```text
blocks/{hash}.blk
```

Historical range index object:

```text
index/range/v1/size-2500/{start_height}.bin
```

Range index objects also act as provider completion markers. If a range bin
already exists, uploaders skip the whole range without checking individual block
objects.

Examples:

```text
index/range/v1/size-2500/0000000000.bin
index/range/v1/size-2500/0000002500.bin
index/range/v1/size-2500/0000005000.bin
```

The range start height is:

```text
start_height = height / range_size * range_size
```

Default values:

```text
range_size = 2500
stable_delay = 2500
recent_walk_limit = 2500
```

## Range Bin Format

Each range bin contains exactly `range_size` hashes.

Default byte size:

```text
2500 * 32 = 80000 bytes
```

Binary layout:

```text
offset 0       hash(start_height)
offset 32      hash(start_height + 1)
offset 64      hash(start_height + 2)
...
offset N*32    hash(start_height + N)
```

Hash encoding:

```text
Decode the RPC block hash hex string into 32 bytes and keep the RPC display
order.
```

Client lookup:

```text
offset = (height - start_height) * 32
hash = bin[offset : offset+32]
hash_hex = hex.EncodeToString(hash)
```

## Provider Flow

Provider command:

```bash
fractal-block-sync upload \
  --rpc-url http://127.0.0.1:8332 \
  --cookie-file ~/.bitcoin/.cookie \
  --from-height 0 \
  --to-height 4999 \
  --stable-delay 2500 \
  --follow
```

`--to-height` is optional. When omitted, the uploader continues to the current
local tip and, with `--follow`, keeps polling for new blocks. `--from-height`
selects the first range to check, not necessarily the first exact block to
upload:

```text
start_height = from_height / range_size * range_size
```

Provider R2 environment variables:

```text
ENDPOINT_URL
ACCESS_KEY_ID
SECRET_ACCESS_KEY
BUCKET_NAME
```

Docker environment variables additionally expose:

```text
FROM_HEIGHT
TO_HEIGHT
STABLE_DELAY
UPLOAD_WORKERS
UPLOAD_INTERVAL
```

RPC calls:

```text
getblockcount
getblockhash height
getblock hash 0
```

Upload algorithm:

1. Read the local bitcoind tip.
2. Compute the effective upload end height:
   ```text
   end_height = min(local_tip, to_height if set)
   ```
3. Start from the range containing `from_height`.
4. For each range:
   - HEAD the range bin:
     ```text
     index/range/v1/size-2500/{start_height}.bin
     ```
   - If it exists, skip the whole range.
   - If it does not exist, process heights in the range up to `end_height`.
5. For each height in a missing range:
   - Call `getblockhash(height)` to get the block hash.
   - HEAD `blocks/{hash}.blk`.
   - If the block object exists, skip it.
   - Otherwise call `getblock(hash, 0)` to get raw block hex.
   - Decode the hex string to raw bytes.
   - Upload the raw block to `blocks/{hash}.blk`.
6. Only create range indexes for complete stable historical ranges:
   ```text
   range_end <= tip - stable_delay
   ```
7. Once all blocks in a complete stable range are available, upload:
   ```text
   index/range/v1/size-2500/{start_height}.bin
   ```
8. Do not create range indexes for incomplete ranges or the latest
   `stable_delay` blocks.

When `--follow` is enabled, transient RPC or R2 errors are logged and retried
after the polling interval instead of exiting the process.

Provider uploads are idempotent:

- Block objects are keyed by hash.
- Range bins are deterministic for stable ranges.
- Existing objects are checked with HEAD before upload.
- PutObject uses conditional create semantics; if another uploader creates the
  same object first, the uploader treats that as success instead of overwriting.

## Provider Resumption and Sharding

The provider uses existing R2 objects as resume state:

```text
range bin exists -> range completed, skip whole range
range bin missing -> check individual block objects and fill gaps
```

This avoids separate progress files. A failed upload can leave some block
objects present without the range bin. On the next run, the uploader checks each
block object in that range, skips existing blocks, uploads missing blocks, and
then creates the range bin once the range is complete and stable.

Multiple uploaders can work on different height shards by setting non-overlapping
`FROM_HEIGHT` and `TO_HEIGHT` ranges. Operators should align shard boundaries to
the 2500-block range size where practical:

```text
FROM_HEIGHT=0      TO_HEIGHT=4999
FROM_HEIGHT=5000  TO_HEIGHT=9999
FROM_HEIGHT=10000 TO_HEIGHT=14999
```

If shards overlap, correctness is preserved by hash-addressed block objects,
range-bin checks, and conditional object creation, though the uploaders may do
extra HEAD requests.

## User Flow

User command:

```bash
fractal-block-sync submit \
  --base-url https://test-fractal-blocks.fractalbitcoin.io \
  --rpc-url http://127.0.0.1:8332 \
  --cookie-file ~/.bitcoin/.cookie \
  --recent-walk-limit 2500 \
  --follow
```

RPC calls:

```text
getblockchaininfo
getblockhash
getchaintips
getblockheader
submitblock
```

Submit algorithm for the next missing block:

1. Call `getblockchaininfo`.
2. Compute:
   ```text
   target = blocks + 1
   ```
3. If `target > headers`, wait for the local node to sync more headers through
   P2P.
4. Try the historical range index first:
   ```text
   GET /index/range/v1/size-2500/{start_height}.bin
   ```
5. If the range bin exists:
   - Read the target hash from the fixed offset.
6. If the range bin returns 404:
   - Treat the target as a recent block.
   - Call `getchaintips` to find a suitable headers tip.
   - Walk backward with `getblockheader(hash)` and `previousblockhash`.
   - Stop when the target height is reached.
   - Enforce `recent_walk_limit`.
7. Download:
   ```text
   GET /blocks/{hash}.blk
   ```
8. Calculate the raw block hash locally and require it to equal the target hash.
9. Convert raw block bytes to hex and call:
   ```text
   submitblock blockhex
   ```
10. Continue if the RPC result is `null` or an already-known style result.

## No Manifest

The client does not need a global provider metadata object, and the provider does
not need separate progress objects.

Decision rule:

```text
range bin exists -> use historical index
range bin 404    -> use local headers walk
```

This keeps R2 as simple static object storage. The same range bin existence rule
is also used by the provider to resume completed historical ranges.

## Reorg Handling

Historical blocks:

- Range bins are created only after `stable_delay`.
- Increase `stable_delay` if the chain has higher deep reorg risk.

Recent blocks:

- Recent heights do not use R2 height indexes.
- The user derives recent hashes from their local headers chain.
- This makes recent block downloads follow the user's current node view.

If no range bin exists and the required header walk exceeds
`recent_walk_limit`, the client should fail with an actionable error:

```text
no range index and header walk exceeds limit
```

Possible operator actions:

- Wait for the provider to publish the historical range bin.
- Increase `--recent-walk-limit`.

## Implementation Packages

Suggested package layout:

```text
r2store       R2 upload and public HTTP download
btcrpc        bitcoind JSON-RPC client
blockhash     raw block hash calculation
rangeindex    range bin encoding, decoding, and path calculation
cmd/fractal-block-sync  CLI
```

Implementation order:

1. `rangeindex`: bin encode/decode/path tests.
2. `btcrpc`: JSON-RPC client with mock server tests.
3. `r2store`: reuse the existing R2 upload/download code.
4. `upload` command.
5. `submit` command.
6. Integration tests for R2 when environment variables are available.

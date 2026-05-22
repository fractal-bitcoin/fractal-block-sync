# fractal-block-sync 部署与使用说明

`fractal-block-sync` 是配合 Fractal/Bitcoin-compatible 节点使用的区块辅助同步程序。它把原始区块镜像到 Cloudflare R2，并让其他节点从 R2 下载、校验、提交缺失区块。

本项目只处理区块传输和校验：不解析交易，不构建业务索引，不处理 BRC20、铭文、Ordinals 或其他协议数据。

## 运行模式

- `upload`：从已同步的本地节点读取区块，上传到 R2，并发布 range index。
- `submit`：从 R2 下载缺失区块，校验 hash 后提交给本地节点。

## 编译

```bash
go build -o fractal-block-sync ./cmd/fractal-block-sync
docker build -t fractal-block-sync:local .
```

## 上传端

Docker Compose：

```bash
ENDPOINT_URL="https://<account-id>.r2.cloudflarestorage.com" \
ACCESS_KEY_ID="<r2-access-key-id>" \
SECRET_ACCESS_KEY="<r2-secret-access-key>" \
BUCKET_NAME="<bucket-name>" \
RPC_URL="http://host.docker.internal:8332" \
BITCOIN_COOKIE_FILE="$HOME/.bitcoin/.cookie" \
docker compose --profile upload up --build
```

二进制：

```bash
ENDPOINT_URL="https://<account-id>.r2.cloudflarestorage.com" \
ACCESS_KEY_ID="<r2-access-key-id>" \
SECRET_ACCESS_KEY="<r2-secret-access-key>" \
BUCKET_NAME="<bucket-name>" \
./fractal-block-sync upload \
  --rpc-url http://127.0.0.1:8332 \
  --cookie-file ~/.bitcoin/.cookie \
  --follow
```

## 下载端

Docker Compose：

```bash
BASE_URL="https://<public-r2-domain>" \
RPC_URL="http://host.docker.internal:8332" \
BITCOIN_COOKIE_FILE="$HOME/.bitcoin/.cookie" \
docker compose --profile submit up --build
```

二进制：

```bash
./fractal-block-sync submit \
  --base-url https://<public-r2-domain> \
  --rpc-url http://127.0.0.1:8332 \
  --cookie-file ~/.bitcoin/.cookie \
  --follow
```

如果本地节点还没有 headers，但 R2 上已经有 range index，可以开启：

```bash
BOOTSTRAP_FROM_R2=true docker compose --profile submit up --build
```

二进制参数：

```bash
--bootstrap-from-r2
```

## RPC 认证

二选一：

```text
BITCOIN_COOKIE_FILE / --cookie-file
RPC_USER + RPC_PASSWORD / --rpc-user + --rpc-password
```

## 关键配置

上传端环境变量：

```text
ENDPOINT_URL       R2 endpoint
ACCESS_KEY_ID     R2 access key
SECRET_ACCESS_KEY R2 secret key
BUCKET_NAME       R2 bucket
FROM_HEIGHT       从该高度所在 range 开始检查，默认 0
TO_HEIGHT         结束上传高度，包含该高度；为空表示当前 tip
STABLE_DELAY      距离 tip 多少个块以内不发布 index，默认 2500
UPLOAD_WORKERS    并发上传数量，默认 4
UPLOAD_INTERVAL   轮询间隔，默认 30s
```

下载端环境变量：

```text
BASE_URL           公开 R2 下载地址
RECENT_WALK_LIMIT  本地 header 回溯上限，默认 2500
BOOTSTRAP_FROM_R2  headers 不足时是否直接使用 R2 index
SUBMIT_INTERVAL    轮询间隔，默认 10s
```

直接运行二进制时，使用对应的 kebab-case 参数，例如 `FROM_HEIGHT` 对应 `--from-height`。

## R2 对象

```text
blocks/{hash}.blk
index/range/v1/size-{range_size}/{start_height}.bin
```

只有完整且稳定的 range 会发布 index：

```text
stableTip = localTip - STABLE_DELAY
```

## 测试链

测试链高度较低时，可以降低稳定确认窗口并开启 R2 bootstrap：

```bash
STABLE_DELAY=0
BOOTSTRAP_FROM_R2=true
```

检查 index：

```bash
curl -I "$BASE_URL/index/range/v1/size-2500/0000000000.bin"
```

## 测试

```bash
GOCACHE=$(pwd)/.gocache go test ./...
```

## 常见问题

- `missing required environment variables`：上传端缺少 R2 配置。
- `base url is required`：下载端缺少 `BASE_URL` 或 `--base-url`。
- `waiting for local headers`：等待本地 headers 同步，或在 R2 index 已存在时开启 `BOOTSTRAP_FROM_R2=true`。
- `waiting for local headers or R2 range index`：对应 range index 尚未发布或 `BASE_URL` 不可访问。
- `downloaded block hash mismatch`：下载区块与目标 hash 不匹配，程序不会提交。

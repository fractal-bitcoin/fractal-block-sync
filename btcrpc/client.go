package btcrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
)

// Client is a small bitcoind JSON-RPC client.
type Client struct {
	url        string
	username   string
	password   string
	httpClient *http.Client
	nextID     atomic.Uint64
}

// Option customizes a Client.
type Option func(*Client) error

// WithHTTPClient sets the HTTP client used for RPC calls.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) error {
		if httpClient != nil {
			c.httpClient = httpClient
		}
		return nil
	}
}

// WithBasicAuth sets JSON-RPC HTTP basic auth credentials.
func WithBasicAuth(username string, password string) Option {
	return func(c *Client) error {
		c.username = username
		c.password = password
		return nil
	}
}

// WithCookieFile reads bitcoind cookie credentials from path.
func WithCookieFile(path string) Option {
	return func(c *Client) error {
		path = strings.TrimSpace(path)
		if path == "" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read cookie file: %w", err)
		}
		username, password, ok := strings.Cut(strings.TrimSpace(string(data)), ":")
		if !ok {
			return fmt.Errorf("invalid cookie file %q", path)
		}
		c.username = username
		c.password = password
		return nil
	}
}

// New creates a JSON-RPC client for bitcoind.
func New(rpcURL string, opts ...Option) (*Client, error) {
	rpcURL = strings.TrimSpace(rpcURL)
	if rpcURL == "" {
		return nil, errors.New("rpc url is required")
	}
	client := &Client{
		url:        rpcURL,
		httpClient: http.DefaultClient,
	}
	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, err
		}
	}
	return client, nil
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *RPCError       `json:"error"`
	ID     uint64          `json:"id"`
}

// RPCError is a bitcoind JSON-RPC error object.
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
}

// Call invokes one JSON-RPC method and unmarshals the result into result.
func (c *Client) Call(ctx context.Context, method string, params []any, result any) error {
	if strings.TrimSpace(method) == "" {
		return errors.New("rpc method is required")
	}
	if params == nil {
		params = []any{}
	}

	payload, err := json.Marshal(rpcRequest{
		JSONRPC: "1.0",
		ID:      c.nextID.Add(1),
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return fmt.Errorf("marshal rpc request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create rpc request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.username != "" || c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("call rpc %s: %w", method, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read rpc %s response: %w", method, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("rpc %s http status %d: %s", method, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var decoded rpcResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return fmt.Errorf("decode rpc %s response: %w", method, err)
	}
	if decoded.Error != nil {
		return decoded.Error
	}
	if result == nil {
		return nil
	}
	if string(decoded.Result) == "null" {
		return nil
	}
	if err := json.Unmarshal(decoded.Result, result); err != nil {
		return fmt.Errorf("decode rpc %s result: %w", method, err)
	}
	return nil
}

// BlockchainInfo is the subset of getblockchaininfo used by this tool.
type BlockchainInfo struct {
	Blocks  uint64 `json:"blocks"`
	Headers uint64 `json:"headers"`
}

// ChainTip is the subset of getchaintips entries used by this tool.
type ChainTip struct {
	Height uint64 `json:"height"`
	Hash   string `json:"hash"`
	Status string `json:"status"`
}

// BlockHeader is the subset of getblockheader used by this tool.
type BlockHeader struct {
	Hash              string `json:"hash"`
	Height            uint64 `json:"height"`
	PreviousBlockHash string `json:"previousblockhash"`
}

func (c *Client) GetBlockCount(ctx context.Context) (uint64, error) {
	var count uint64
	err := c.Call(ctx, "getblockcount", nil, &count)
	return count, err
}

func (c *Client) GetBlockHash(ctx context.Context, height uint64) (string, error) {
	var hash string
	err := c.Call(ctx, "getblockhash", []any{height}, &hash)
	return hash, err
}

func (c *Client) GetBlockRawHex(ctx context.Context, hash string) (string, error) {
	var rawHex string
	err := c.Call(ctx, "getblock", []any{hash, 0, true}, &rawHex)
	return rawHex, err
}

func (c *Client) GetBlockchainInfo(ctx context.Context) (BlockchainInfo, error) {
	var info BlockchainInfo
	err := c.Call(ctx, "getblockchaininfo", nil, &info)
	return info, err
}

func (c *Client) GetChainTips(ctx context.Context) ([]ChainTip, error) {
	var tips []ChainTip
	err := c.Call(ctx, "getchaintips", nil, &tips)
	return tips, err
}

func (c *Client) GetBlockHeader(ctx context.Context, hash string) (BlockHeader, error) {
	var header BlockHeader
	err := c.Call(ctx, "getblockheader", []any{hash}, &header)
	return header, err
}

func (c *Client) SubmitBlock(ctx context.Context, blockHex string) (string, error) {
	var result string
	err := c.Call(ctx, "submitblock", []any{blockHex}, &result)
	return result, err
}

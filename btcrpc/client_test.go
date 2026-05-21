package btcrpc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

type capturedRequest struct {
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
}

func TestCallSendsJSONRPCAndBasicAuth(t *testing.T) {
	var captured capturedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "user" || pass != "pass" {
			t.Fatalf("BasicAuth = %q/%q/%v, want user/pass/true", user, pass, ok)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("Decode request returned error: %v", err)
		}
		_, _ = w.Write([]byte(`{"result":"abc","error":null,"id":1}`))
	}))
	defer server.Close()

	client, err := New(server.URL, WithBasicAuth("user", "pass"))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	var got string
	if err := client.Call(context.Background(), "getblockhash", []any{uint64(1)}, &got); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if got != "abc" {
		t.Fatalf("result = %q, want abc", got)
	}
	if captured.Method != "getblockhash" || len(captured.Params) != 1 || captured.Params[0].(float64) != 1 {
		t.Fatalf("captured request = %+v", captured)
	}
}

func TestCallReturnsRPCError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"result":null,"error":{"code":-5,"message":"not found"},"id":1}`))
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	err = client.Call(context.Background(), "getblock", nil, nil)
	if err == nil {
		t.Fatal("Call returned nil error")
	}
	if rpcErr, ok := err.(*RPCError); !ok || rpcErr.Code != -5 {
		t.Fatalf("error = %#v, want RPCError code -5", err)
	}
}

func TestWithCookieFile(t *testing.T) {
	dir := t.TempDir()
	cookiePath := filepath.Join(dir, ".cookie")
	if err := os.WriteFile(cookiePath, []byte("__cookie__:secret"), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "__cookie__" || pass != "secret" {
			t.Fatalf("BasicAuth = %q/%q/%v, want cookie credentials", user, pass, ok)
		}
		_, _ = w.Write([]byte(`{"result":1,"error":null,"id":1}`))
	}))
	defer server.Close()

	client, err := New(server.URL, WithCookieFile(cookiePath))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if _, err := client.GetBlockCount(context.Background()); err != nil {
		t.Fatalf("GetBlockCount returned error: %v", err)
	}
}

func TestHighLevelMethods(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req capturedRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode request returned error: %v", err)
		}
		switch req.Method {
		case "getblockchaininfo":
			_, _ = w.Write([]byte(`{"result":{"blocks":10,"headers":12},"error":null,"id":1}`))
		case "getchaintips":
			_, _ = w.Write([]byte(`{"result":[{"height":12,"hash":"tip","status":"headers-only"}],"error":null,"id":1}`))
		case "getblockheader":
			_, _ = w.Write([]byte(`{"result":{"hash":"tip","height":12,"previousblockhash":"prev"},"error":null,"id":1}`))
		case "submitblock":
			_, _ = w.Write([]byte(`{"result":null,"error":null,"id":1}`))
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	client, err := New(server.URL)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	info, err := client.GetBlockchainInfo(context.Background())
	if err != nil || info.Blocks != 10 || info.Headers != 12 {
		t.Fatalf("GetBlockchainInfo = %+v, %v", info, err)
	}
	tips, err := client.GetChainTips(context.Background())
	if err != nil || len(tips) != 1 || tips[0].Hash != "tip" {
		t.Fatalf("GetChainTips = %+v, %v", tips, err)
	}
	header, err := client.GetBlockHeader(context.Background(), "tip")
	if err != nil || header.PreviousBlockHash != "prev" {
		t.Fatalf("GetBlockHeader = %+v, %v", header, err)
	}
	result, err := client.SubmitBlock(context.Background(), "00")
	if err != nil || result != "" {
		t.Fatalf("SubmitBlock = %q, %v", result, err)
	}
}

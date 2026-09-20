package sink

import (
	"bytes"
	"context"
	"crypto/hmac"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/event"
)

const testSecret = "0123456789abcdef0123456789abcdef"

type received struct {
	body    string
	headers http.Header
}

type receiver struct {
	mu       sync.Mutex
	requests []received
	respond  func(n int, w http.ResponseWriter)
}

func (r *receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	r.requests = append(r.requests, received{string(body), req.Header.Clone()})
	n := len(r.requests)
	r.mu.Unlock()
	r.respond(n, w)
}

func (r *receiver) seen() []received {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]received(nil), r.requests...)
}

func newWebhook(t *testing.T, respond func(n int, w http.ResponseWriter)) (*Webhook, *receiver) {
	t.Helper()
	rcv := &receiver{respond: respond}
	srv := httptest.NewServer(rcv)
	t.Cleanup(srv.Close)

	wh := NewWebhook(WebhookConfig{
		URL:      srv.URL,
		Secret:   testSecret,
		Timeout:  200 * time.Millisecond,
		RetryMin: time.Millisecond,
		RetryMax: 5 * time.Millisecond,
	}, zerolog.Nop())
	t.Cleanup(func() { _ = wh.Close() })
	return wh, rcv
}

func send(t *testing.T, ctx context.Context, wh *Webhook, body string) error {
	t.Helper()
	result := make(chan error, 1)
	go wh.Send(ctx, &event.Batch{Buf: []byte(body), Events: strings.Count(body, "\n")}, func(err error) { result <- err })
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Send never called done")
		return nil
	}
}

func verify(t *testing.T, header, body string, now time.Time, tolerance time.Duration) bool {
	t.Helper()
	parts := strings.SplitN(header, ",", 2)
	if len(parts) != 2 || !strings.HasPrefix(parts[0], "t=") || !strings.HasPrefix(parts[1], "v1=") {
		t.Fatalf("malformed signature header %q", header)
	}
	unix, err := strconv.ParseInt(strings.TrimPrefix(parts[0], "t="), 10, 64)
	if err != nil {
		t.Fatalf("bad timestamp in %q", header)
	}
	at := time.Unix(unix, 0)
	if now.Sub(at).Abs() > tolerance {
		return false
	}
	return hmac.Equal([]byte(Sign([]byte(testSecret), at, []byte(body))), []byte(header))
}

func TestWebhook_Send(t *testing.T) {
	const body = "{\"op\":\"insert\"}\n{\"op\":\"update\"}\n"

	t.Run("posts ndjson with a verifiable signature", func(t *testing.T) {
		wh, rcv := newWebhook(t, func(_ int, w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) })

		if err := send(t, context.Background(), wh, body); err != nil {
			t.Fatal(err)
		}
		got := rcv.seen()[0]
		if got.body != body || got.headers.Get("Content-Type") != contentTypeNDJSON || got.headers.Get(HeaderEvents) != "2" {
			t.Fatalf("body=%q headers=%v", got.body, got.headers)
		}
		if !verify(t, got.headers.Get(HeaderSignature), got.body, time.Now(), time.Minute) {
			t.Fatal("signature does not verify")
		}
		if verify(t, got.headers.Get(HeaderSignature), got.body+"x", time.Now(), time.Minute) {
			t.Fatal("signature verified a tampered body")
		}
		if verify(t, got.headers.Get(HeaderSignature), got.body, time.Now().Add(time.Hour), time.Minute) {
			t.Fatal("signature accepted outside the replay window")
		}
	})

	t.Run("retries server errors with the same idempotency key", func(t *testing.T) {
		wh, rcv := newWebhook(t, func(n int, w http.ResponseWriter) {
			if n < 3 {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			w.WriteHeader(http.StatusOK)
		})

		if err := send(t, context.Background(), wh, body); err != nil {
			t.Fatal(err)
		}
		reqs := rcv.seen()
		if len(reqs) != 3 {
			t.Fatalf("got %d requests, want 3", len(reqs))
		}
		for i, r := range reqs {
			if r.headers.Get(HeaderIdempotencyKey) != reqs[0].headers.Get(HeaderIdempotencyKey) || r.headers.Get(HeaderIdempotencyKey) == "" {
				t.Fatalf("request %d changed the idempotency key", i)
			}
			if r.headers.Get(HeaderAttempt) != strconv.Itoa(i+1) || r.body != body {
				t.Fatalf("request %d: attempt=%s body=%q", i, r.headers.Get(HeaderAttempt), r.body)
			}
		}
	})

	t.Run("retries 429 and a timed out request", func(t *testing.T) {
		wh, rcv := newWebhook(t, func(n int, w http.ResponseWriter) {
			switch n {
			case 1:
				w.WriteHeader(http.StatusTooManyRequests)
			case 2:
				time.Sleep(400 * time.Millisecond)
			default:
				w.WriteHeader(http.StatusOK)
			}
		})

		if err := send(t, context.Background(), wh, body); err != nil {
			t.Fatal(err)
		}
		if n := len(rcv.seen()); n != 3 {
			t.Fatalf("got %d requests, want 3", n)
		}
	})

	t.Run("client error rejects the batch without retrying", func(t *testing.T) {
		wh, rcv := newWebhook(t, func(_ int, w http.ResponseWriter) {
			http.Error(w, "schema mismatch", http.StatusUnprocessableEntity)
		})

		err := send(t, context.Background(), wh, body)
		if !errors.Is(err, ErrRejected) || !strings.Contains(err.Error(), "schema mismatch") {
			t.Fatalf("err = %v, want ErrRejected with the receiver's reason", err)
		}
		if n := len(rcv.seen()); n != 1 {
			t.Fatalf("got %d requests, want 1", n)
		}
	})

	t.Run("redirect is rejected instead of followed", func(t *testing.T) {
		wh, _ := newWebhook(t, func(_ int, w http.ResponseWriter) {
			w.Header().Set("Location", "http://127.0.0.1:1/elsewhere")
			w.WriteHeader(http.StatusFound)
		})

		if err := send(t, context.Background(), wh, body); !errors.Is(err, ErrRejected) {
			t.Fatalf("err = %v, want ErrRejected", err)
		}
	})

	t.Run("cancel stops an endless retry loop", func(t *testing.T) {
		wh, rcv := newWebhook(t, func(_ int, w http.ResponseWriter) { w.WriteHeader(http.StatusServiceUnavailable) })
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			for len(rcv.seen()) < 3 {
				time.Sleep(time.Millisecond)
			}
			cancel()
		}()
		if err := send(t, ctx, wh, body); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})

	t.Run("empty batch is acknowledged without a request", func(t *testing.T) {
		wh, rcv := newWebhook(t, func(_ int, w http.ResponseWriter) { w.WriteHeader(http.StatusOK) })

		if err := send(t, context.Background(), wh, ""); err != nil {
			t.Fatal(err)
		}
		if n := len(rcv.seen()); n != 0 {
			t.Fatalf("got %d requests, want 0", n)
		}
	})
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 21, 7, 28, 0, 0, time.UTC)
	tests := []struct {
		name string
		in   string
		want time.Duration
	}{
		{"seconds", "7", 7 * time.Second},
		{"seconds above our own backoff cap are kept", "120", 2 * time.Minute},
		{"http date", "Wed, 21 Oct 2026 07:30:00 GMT", 2 * time.Minute},
		{"http date in the past", "Wed, 21 Oct 2026 07:00:00 GMT", 0},
		{"absurd value hits the ceiling", "864000", maxRetryAfter},
		{"empty", "", 0},
		{"garbage", "soon", 0},
		{"negative", "-3", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseRetryAfter(tt.in, now); got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWebhook_SendWaitsOutRetryAfterBeyondItsOwnBackoffCap(t *testing.T) {
	wh, rcv := newWebhook(t, func(n int, w http.ResponseWriter) {
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	started := time.Now()
	if err := send(t, context.Background(), wh, "{}\n"); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(started); waited < time.Second {
		t.Fatalf("retried after %v, the receiver asked for 1s and RetryMax is 5ms", waited)
	}
	if n := len(rcv.seen()); n != 2 {
		t.Fatalf("got %d requests, want 2", n)
	}
}

func TestWebhook_postErrorsDoNotLeakTheURL(t *testing.T) {
	wh := NewWebhook(WebhookConfig{
		URL: "http://127.0.0.1:1/hooks/path-token-abc?token=query-token-xyz", Secret: testSecret,
		Timeout: time.Second, RetryMin: time.Millisecond, RetryMax: time.Millisecond,
	}, zerolog.Nop())
	t.Cleanup(func() { _ = wh.Close() })

	_, err := wh.post(context.Background(), &event.Batch{Buf: []byte("{}\n"), Events: 1}, "key", 0)
	if err == nil {
		t.Fatal("expected a connection error")
	}
	for _, secret := range []string{"path-token-abc", "query-token-xyz"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaks %q: %v", secret, err)
		}
	}
}

func BenchmarkSign(b *testing.B) {
	body := bytes.Repeat([]byte(`{"table":"public.users","op":"insert","new":{"id":7}}`+"\n"), 1200)
	now := time.Now()
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for b.Loop() {
		Sign([]byte(testSecret), now, body)
	}
}

func BenchmarkWebhook_Send(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	wh := NewWebhook(WebhookConfig{URL: srv.URL, Secret: testSecret, Timeout: 5 * time.Second, RetryMin: time.Millisecond, RetryMax: time.Millisecond}, zerolog.Nop())
	defer func() { _ = wh.Close() }()
	batch := &event.Batch{Buf: bytes.Repeat([]byte(`{"table":"public.users","op":"insert","new":{"id":7}}`+"\n"), 1200), Events: 1200}

	b.SetBytes(int64(len(batch.Buf)))
	b.ReportAllocs()
	for b.Loop() {
		wh.Send(context.Background(), batch, func(err error) {
			if err != nil {
				b.Fatal(err)
			}
		})
	}
}

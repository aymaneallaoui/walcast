package sink

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/backoff"
	"github.com/aymaneallaoui/walcast/internal/event"
)

const (
	HeaderSignature      = "X-Walcast-Signature"
	HeaderIdempotencyKey = "X-Walcast-Idempotency-Key"
	HeaderEvents         = "X-Walcast-Events"
	HeaderAttempt        = "X-Walcast-Attempt"
	headerRequestID      = "X-Request-Id"

	contentTypeNDJSON = "application/x-ndjson"
	userAgent         = "walcast"

	maxIdleConns    = 4
	idleConnTimeout = 90 * time.Second
	maxRetryAfter   = time.Hour
	drainLimit      = 64 << 10
	maxRequestIDLen = 64
)

type WebhookConfig struct {
	URL        string
	Secret     string
	Timeout    time.Duration
	RetryMin   time.Duration
	RetryMax   time.Duration
	HTTPClient *http.Client
}

// Webhook delivers one batch at a time and retries in place, so a retried batch can never be
// overtaken by a later one and per-row order survives receiver outages.
type Webhook struct {
	cfg    WebhookConfig
	secret []byte
	client *http.Client
	log    zerolog.Logger
	now    func() time.Time
	body   []byte
}

func NewWebhook(cfg WebhookConfig, log zerolog.Logger) *Webhook {
	client := cfg.HTTPClient
	if client == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConns = maxIdleConns
		transport.MaxIdleConnsPerHost = maxIdleConns
		transport.IdleConnTimeout = idleConnTimeout
		client = &http.Client{
			Transport: transport,
			Timeout:   cfg.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	return &Webhook{cfg: cfg, secret: []byte(cfg.Secret), client: client, log: log, now: time.Now}
}

func (w *Webhook) Send(ctx context.Context, b *event.Batch, done func(error)) {
	if len(b.Buf) == 0 {
		done(nil)
		return
	}

	// net/http may keep reading a request body after a cancelled or failed round trip, while the
	// batch goes back to the pool right after done. So requests read a private copy, never b.Buf.
	body := w.body[:0]
	body = append(body, b.Buf...)
	w.body = nil
	digest := sha256.Sum256(body)
	key := hex.EncodeToString(digest[:16])

	for attempt := 0; ; attempt++ {
		retryAfter, settled, err := w.post(ctx, body, b.Events, key, attempt)
		if !settled {
			body = bytes.Clone(body)
		}
		if err == nil {
			w.body = body
			done(nil)
			return
		}
		if ctx.Err() != nil {
			done(ctx.Err())
			return
		}
		if errors.Is(err, ErrRejected) {
			done(err)
			return
		}

		delay := max(backoff.Delay(attempt, w.cfg.RetryMin, w.cfg.RetryMax), retryAfter)
		w.log.Warn().Err(err).Int("attempt", attempt+1).Dur("retry_in", delay).Msg("webhook delivery failed")

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			done(ctx.Err())
			return
		}
	}
}

func (w *Webhook) Close() error {
	w.client.CloseIdleConnections()
	return nil
}

// post reports settled once the response body is closed: only then has the transport let go of body.
func (w *Webhook) post(ctx context.Context, body []byte, events int, key string, attempt int) (retryAfter time.Duration, settled bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return 0, true, fmt.Errorf("%w: build request: %w", ErrRejected, err)
	}
	req.Header.Set("Content-Type", contentTypeNDJSON)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set(HeaderIdempotencyKey, key)
	req.Header.Set(HeaderEvents, strconv.Itoa(events))
	req.Header.Set(HeaderAttempt, strconv.Itoa(attempt+1))
	req.Header.Set(HeaderSignature, Sign(w.secret, w.now(), body))

	resp, err := w.client.Do(req)
	if err != nil {
		return 0, false, fmt.Errorf("post: %w", withoutURL(err))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit))
	_ = resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return 0, true, nil
	case retryable(resp.StatusCode):
		return parseRetryAfter(resp.Header.Get("Retry-After"), w.now()), true, fmt.Errorf("receiver answered %s", resp.Status)
	default:
		return 0, true, fmt.Errorf("%w: receiver answered %s%s", ErrRejected, resp.Status, requestID(resp.Header))
	}
}

// requestID is the only receiver-controlled text allowed into errors: a response body can echo
// row data, and these errors reach logs and stderr.
func requestID(h http.Header) string {
	id := h.Get(headerRequestID)
	if id == "" {
		return ""
	}
	return " (request id " + strconv.QuoteToASCII(id[:min(len(id), maxRequestIDLen)]) + ")"
}

// withoutURL drops the request URL that net/http puts in its errors: these errors are logged on
// every retry, and webhook URLs often carry a token in the path or query.
func withoutURL(err error) error {
	if urlErr, ok := errors.AsType[*url.Error](err); ok {
		return urlErr.Err
	}
	return err
}

func retryable(status int) bool {
	return status >= 500 || status == http.StatusRequestTimeout || status == http.StatusTooManyRequests
}

// parseRetryAfter accepts both forms of the header. WEBHOOK_RETRY_MAX bounds only our own backoff,
// not the receiver's request; maxRetryAfter stops a bogus value from holding Postgres WAL for days.
func parseRetryAfter(v string, now time.Time) time.Duration {
	var delay time.Duration
	if seconds, err := strconv.ParseInt(v, 10, 64); err == nil {
		delay = time.Duration(min(max(seconds, 0), int64(maxRetryAfter/time.Second))) * time.Second
	} else if at, err := http.ParseTime(v); err == nil {
		delay = at.Sub(now)
	}
	return min(max(delay, 0), maxRetryAfter)
}

// Sign returns "t=<unix>,v1=<hex>" where v1 is HMAC-SHA256 over "<unix>.<body>". Binding the
// timestamp into the MAC lets a receiver reject captured requests that are replayed later.
func Sign(secret []byte, at time.Time, body []byte) string {
	ts := strconv.FormatInt(at.Unix(), 10)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(ts))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return "t=" + ts + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

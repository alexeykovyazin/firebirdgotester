package schedule

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	const hexDigits = "0123456789abcdef"
	for i := range b {
		b[i] = hexDigits[b[i]&0x0f]
	}
	return string(b)
}

type notifyJob struct {
	url     string
	payload []byte
}

// Notifier delivers run-finished webhooks asynchronously: Dispatch never
// blocks the engine, deliveries retry 3 times with backoff, and bodies are
// HMAC-signed when a secret is configured.
type Notifier struct {
	secret string
	client *http.Client
	ch     chan notifyJob
	done   chan struct{}
}

// NewNotifier starts the background delivery worker.
func NewNotifier(secret string) *Notifier {
	n := &Notifier{
		secret: secret,
		client: &http.Client{Timeout: 10 * time.Second},
		ch:     make(chan notifyJob, 64),
		done:   make(chan struct{}),
	}
	go n.loop()
	return n
}

// Dispatch queues a webhook; drops (with a log line) when the queue is full.
func (n *Notifier) Dispatch(url string, payload interface{}) {
	if url == "" || n == nil {
		return
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	select {
	case n.ch <- notifyJob{url: url, payload: body}:
	default:
		log.Printf("webhook queue full, dropping notification for %s", url)
	}
}

// Stop terminates the worker after the current delivery.
func (n *Notifier) Stop() {
	select {
	case <-n.done:
	default:
		close(n.done)
	}
}

func (n *Notifier) loop() {
	for {
		select {
		case <-n.done:
			return
		case job := <-n.ch:
			n.deliver(job)
		}
	}
}

var webhookRetries = []time.Duration{0, 2 * time.Second, 4 * time.Second}

// webhookURLOk validates the destination scheme: the notifier POSTs JSON to
// whatever URL a schedule carries, so file://, gopher:// and friends must be
// rejected up front.
func webhookURLOk(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return false
	}
	scheme := strings.ToLower(u.Scheme)
	return scheme == "http" || scheme == "https"
}

func (n *Notifier) deliver(job notifyJob) {
	if !webhookURLOk(job.url) {
		log.Printf("webhook: refusing non-http(s) url %q", job.url)
		return
	}
	for i, backoff := range webhookRetries {
		if backoff > 0 {
			select {
			case <-n.done:
				return
			case <-time.After(backoff):
			}
		}
		req, err := http.NewRequest(http.MethodPost, job.url, bytes.NewReader(job.payload))
		if err != nil {
			log.Printf("webhook: bad url %s: %v", job.url, err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-FBLoadGen-Event", "run.finished")
		if n.secret != "" {
			mac := hmac.New(sha256.New, []byte(n.secret))
			mac.Write(job.payload)
			req.Header.Set("X-FBLoadGen-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
		}
		resp, err := n.client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return
			}
			err = &statusCodeError{code: resp.StatusCode}
		}
		if i == len(webhookRetries)-1 {
			log.Printf("webhook to %s failed after %d attempts: %v", job.url, len(webhookRetries), err)
			return
		}
		log.Printf("webhook to %s attempt %d failed: %v", job.url, i+1, err)
	}
}

type statusCodeError struct{ code int }

func (e *statusCodeError) Error() string {
	return http.StatusText(e.code)
}

package gofeed_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/mmcdole/gofeed"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func regressionResponse(req *http.Request, status int, body io.ReadCloser) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     make(http.Header),
		Body:       body,
		Request:    req,
	}
}

// scriptBody is a response body whose reads and closes are fully controlled
// and recorded, so tests can assert on read volume and close count.
type scriptBody struct {
	reader   io.Reader
	read     int64
	closes   int
	closeErr error
}

func (b *scriptBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += int64(n)
	return n, err
}

func (b *scriptBody) Close() error {
	b.closes++
	return b.closeErr
}

// dataErrReader returns err together with the final data bytes, like a
// chunked transport that fails mid-stream.
type dataErrReader struct {
	reader *strings.Reader
	err    error
}

func (r dataErrReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 && r.reader.Len() == 0 {
		return n, r.err
	}
	return n, err
}

const (
	regressionRSS  = `<rss version="2.0"><channel><title>rss-title</title></channel></rss>`
	regressionAtom = `<feed xmlns="http://www.w3.org/2005/Atom"><title>atom-title</title></feed>`
	regressionJSON = `{"version":"https://jsonfeed.org/version/1.1","title":"json-title","items":[]}`
)

// regressionFeedTransport serves one of the three feed formats per URL path,
// entirely in-process.
func regressionFeedTransport(req *http.Request) (*http.Response, error) {
	var doc string
	switch req.URL.Path {
	case "/rss":
		doc = regressionRSS
	case "/atom":
		doc = regressionAtom
	case "/json":
		doc = regressionJSON
	default:
		return regressionResponse(req, http.StatusNotFound, io.NopCloser(strings.NewReader(""))), nil
	}
	return regressionResponse(req, http.StatusOK, io.NopCloser(strings.NewReader(doc))), nil
}

// A Parser configured once and then only read must be safe for concurrent
// ParseURL/ParseURLWithContext from the very first call, across all three
// feed formats, without a Client being set. The default client resolves
// through http.DefaultTransport, which this test swaps for an in-process
// transport so no network is touched.
func TestRegressionSharedParserConcurrentFirstUse(t *testing.T) {
	original := http.DefaultTransport
	http.DefaultTransport = roundTripFunc(regressionFeedTransport)
	t.Cleanup(func() { http.DefaultTransport = original })

	p := gofeed.NewParser()
	require.Nil(t, p.Client)

	paths := []string{"/rss", "/atom", "/json"}
	titles := map[string]string{"/rss": "rss-title", "/atom": "atom-title", "/json": "json-title"}

	const goroutines = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*len(paths)*2)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for _, path := range paths {
				url := "https://example.com" + path
				feed, err := p.ParseURL(url)
				if err != nil {
					errs <- fmt.Errorf("ParseURL(%s): %w", path, err)
					continue
				}
				if feed.Title != titles[path] {
					errs <- fmt.Errorf("ParseURL(%s): title %q", path, feed.Title)
				}
				feed, err = p.ParseURLWithContext(url, context.Background())
				if err != nil {
					errs <- fmt.Errorf("ParseURLWithContext(%s): %w", path, err)
					continue
				}
				if feed.Title != titles[path] {
					errs <- fmt.Errorf("ParseURLWithContext(%s): title %q", path, feed.Title)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// The parser is read-only after configuration: no default may be written
	// back into it (that write-back is what raced before the fix).
	require.Nil(t, p.Client)
}

// An explicitly configured Client must be used for every request and never
// replaced by the package default.
func TestRegressionCustomClientUsedEveryTime(t *testing.T) {
	var mu sync.Mutex
	var calls int
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return regressionFeedTransport(req)
	})}

	p := gofeed.NewParser()
	p.Client = client

	const requests = 8
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.ParseURL("https://example.com/rss"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, requests, calls)
	require.Same(t, client, p.Client)
}

// When the read that crosses MaxByteSize also carries an underlying
// transport error, the overflow must win: the response is too large
// regardless of what else went wrong on that read.
func TestRegressionMaxByteSizeOverflowBeatsReadError(t *testing.T) {
	boom := errors.New("chunked transfer failed")
	doc := `<rss version="2.0"><channel><title>` + strings.Repeat("x", 5000) + `</title></channel></rss>`
	const limit = 4096

	body := &scriptBody{reader: dataErrReader{strings.NewReader(doc), boom}}
	p := gofeed.NewParser()
	p.MaxByteSize = limit
	p.Client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return regressionResponse(req, http.StatusOK, body), nil
	})}

	feed, err := p.ParseURL("https://example.com/rss")
	require.Nil(t, feed)
	require.ErrorIs(t, err, gofeed.ErrResponseTooLarge)
	require.LessOrEqual(t, body.read, int64(limit)+1, "read at most one byte past the limit")
	require.Equal(t, 1, body.closes)
}

// The mirror case: without overflow, an underlying read error keeps its
// identity for errors.Is.
func TestRegressionMaxByteSizeReadErrorPreserved(t *testing.T) {
	boom := errors.New("chunked transfer failed")
	doc := `<rss version="2.0"><channel><title>` + strings.Repeat("x", 2000) + `</title></channel></rss>`

	body := &scriptBody{reader: dataErrReader{strings.NewReader(doc), boom}}
	p := gofeed.NewParser()
	p.MaxByteSize = 1 << 20
	p.Client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return regressionResponse(req, http.StatusOK, body), nil
	})}

	feed, err := p.ParseURL("https://example.com/rss")
	require.Nil(t, feed)
	require.ErrorIs(t, err, boom)
	require.NotErrorIs(t, err, gofeed.ErrResponseTooLarge)
	require.Equal(t, 1, body.closes)
}

// A failing Body.Close must not mask an earlier status, read, or parse
// error; it only surfaces when everything else succeeded. The body is closed
// exactly once on every path.
func TestRegressionCloseErrorNeverOverridesEarlierError(t *testing.T) {
	closeErr := errors.New("body close failed")
	oversizeDoc := `<rss version="2.0"><channel><title>` + strings.Repeat("x", 5000) + `</title></channel></rss>`

	cases := []struct {
		name      string
		status    int
		doc       string
		limit     int64
		closeErr  error
		wantErr   error // nil means success
		checkHTTP bool   // expect gofeed.HTTPError with the status code
	}{
		{name: "status error beats close error", status: http.StatusServiceUnavailable, doc: "down", closeErr: closeErr, checkHTTP: true},
		{name: "parse error beats close error", status: http.StatusOK, doc: "not a feed at all", closeErr: closeErr, wantErr: gofeed.ErrFeedTypeNotDetected},
		{name: "limit error beats close error", status: http.StatusOK, doc: oversizeDoc, limit: 4096, closeErr: closeErr, wantErr: gofeed.ErrResponseTooLarge},
		{name: "close error surfaces on success", status: http.StatusOK, doc: regressionRSS, closeErr: closeErr, wantErr: closeErr},
		{name: "clean success", status: http.StatusOK, doc: regressionRSS},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := &scriptBody{reader: strings.NewReader(tc.doc), closeErr: tc.closeErr}
			p := gofeed.NewParser()
			p.MaxByteSize = tc.limit
			p.Client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return regressionResponse(req, tc.status, body), nil
			})}

			feed, err := p.ParseURLWithContext("https://example.com/feed", context.Background())
			switch {
			case tc.checkHTTP:
				var httpErr gofeed.HTTPError
				require.ErrorAs(t, err, &httpErr)
				require.Equal(t, tc.status, httpErr.StatusCode)
				require.NotErrorIs(t, err, closeErr)
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
				if tc.wantErr != closeErr {
					require.NotErrorIs(t, err, closeErr)
				}
			default:
				require.NoError(t, err)
				require.NotNil(t, feed)
			}
			require.Equal(t, 1, body.closes, "body closed exactly once")
		})
	}
}

package gofeed_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mmcdole/gofeed"
	"github.com/stretchr/testify/require"
)

const (
	regressionRSSFeed  = `<rss version="2.0"><channel><title>regression-rss</title></channel></rss>`
	regressionAtomFeed = `<feed xmlns="http://www.w3.org/2005/Atom"><title>regression-atom</title></feed>`
	regressionJSONFeed = `{"version":"https://jsonfeed.org/version/1.1","title":"regression-json","items":[]}`
)

type regressionRoundTripper func(*http.Request) (*http.Response, error)

func (f regressionRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func regressionResponse(status int, body io.ReadCloser) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     make(http.Header),
		Body:       body,
	}
}

// trackedBody counts Close calls and can fail Close on demand.
type trackedBody struct {
	r        io.Reader
	closes   int
	closeErr error
}

func (b *trackedBody) Read(p []byte) (int, error) { return b.r.Read(p) }

func (b *trackedBody) Close() error {
	b.closes++
	return b.closeErr
}

// chunkedReader delivers the wrapped reader in fixed-size pieces.
type chunkedReader struct {
	r     io.Reader
	chunk int
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if len(p) > c.chunk {
		p = p[:c.chunk]
	}
	return c.r.Read(p)
}

// dataWithErrorReader returns err attached to the final data-carrying Read,
// mimicking a transport that fails while delivering the last chunk.
type dataWithErrorReader struct {
	data []byte
	err  error
	read int64
}

func (r *dataWithErrorReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	r.read += int64(n)
	if len(r.data) == 0 {
		return n, r.err
	}
	return n, nil
}

// TestRegressionConcurrentSharedParserFirstUse covers a read-only Parser
// shared across goroutines whose first-ever ParseURL/ParseURLWithContext
// calls race: the lazy default-client assignment used to write back to the
// Parser. Run with -race.
func TestRegressionConcurrentSharedParserFirstUse(t *testing.T) {
	const workers = 32

	t.Run("DefaultClient", func(t *testing.T) {
		// No Client set: every goroutine resolves the shared default client
		// concurrently. The ftp:// scheme makes client.Do fail in-process,
		// so the test never touches the network.
		p := gofeed.NewParser()
		start := make(chan struct{})
		errs := make(chan error, workers*2)
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, err := p.ParseURL("ftp://gofeed.invalid/feed"); err == nil {
					errs <- errors.New("ParseURL: expected an error")
				}
				if _, err := p.ParseURLWithContext("ftp://gofeed.invalid/feed", context.Background()); err == nil {
					errs <- errors.New("ParseURLWithContext: expected an error")
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	})

	t.Run("CustomClientThreeFormats", func(t *testing.T) {
		feeds := map[string]string{
			"/rss":  regressionRSSFeed,
			"/atom": regressionAtomFeed,
			"/json": regressionJSONFeed,
		}
		var calls int64
		transport := regressionRoundTripper(func(req *http.Request) (*http.Response, error) {
			atomic.AddInt64(&calls, 1)
			doc, ok := feeds[req.URL.Path]
			if !ok {
				return regressionResponse(http.StatusNotFound, io.NopCloser(strings.NewReader(""))), nil
			}
			return regressionResponse(http.StatusOK, io.NopCloser(strings.NewReader(doc))), nil
		})
		p := gofeed.NewParser()
		p.Client = &http.Client{Transport: transport}

		const perFormat = 16
		start := make(chan struct{})
		errs := make(chan error, perFormat*len(feeds)*2)
		var wg sync.WaitGroup
		for path := range feeds {
			for i := 0; i < perFormat; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					url := "http://gofeed.invalid" + path
					if _, err := p.ParseURL(url); err != nil {
						errs <- err
					}
					if _, err := p.ParseURLWithContext(url, context.Background()); err != nil {
						errs <- err
					}
				}()
			}
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		// The custom client must serve every single request; had any call
		// fallen back to the default client, the .invalid host would have
		// failed and the count would be short.
		require.Equal(t, int64(perFormat*len(feeds)*2), atomic.LoadInt64(&calls))
	})
}

// TestRegressionMaxByteSizeOverflowBeatsReadError covers a chunked upstream
// whose final Read delivers both the bytes crossing MaxByteSize and a non-EOF
// transport error: the overflow must win. When the limit is not exceeded the
// transport error must propagate unchanged.
func TestRegressionMaxByteSizeOverflowBeatsReadError(t *testing.T) {
	boom := errors.New("transport failed mid-body")
	const limit = 100

	// A complete RSS document exactly one byte over the limit, so the read
	// that detects overflow is also the read that returns the error.
	doc := "<rss><channel><title>" + strings.Repeat("x", limit+1-len("<rss><channel><title>")-len("</title></channel></rss>")) + "</title></channel></rss>"
	require.Equal(t, int64(limit+1), int64(len(doc)))

	newParser := func(reader io.Reader, body **trackedBody, max int64) *gofeed.Parser {
		b := &trackedBody{r: reader}
		*body = b
		p := gofeed.NewParser()
		p.MaxByteSize = max
		p.Client = &http.Client{Transport: regressionRoundTripper(func(*http.Request) (*http.Response, error) {
			return regressionResponse(http.StatusOK, b), nil
		})}
		return p
	}

	for _, chunk := range []int{1, 16, 4096} {
		t.Run(fmt.Sprintf("OverflowWins/chunk=%d", chunk), func(t *testing.T) {
			reader := &dataWithErrorReader{data: []byte(doc), err: boom}
			var body *trackedBody
			p := newParser(&chunkedReader{r: reader, chunk: chunk}, &body, limit)

			feed, err := p.ParseURL("http://gofeed.invalid/feed")
			require.ErrorIs(t, err, gofeed.ErrResponseTooLarge)
			require.NotErrorIs(t, err, boom)
			require.Nil(t, feed)
			require.LessOrEqual(t, reader.read, int64(limit)+1, "read at most one byte past the limit")
			require.Equal(t, 1, body.closes)
		})

		t.Run(fmt.Sprintf("NoOverflowKeepsError/chunk=%d", chunk), func(t *testing.T) {
			reader := &dataWithErrorReader{data: []byte(doc), err: boom}
			var body *trackedBody
			p := newParser(&chunkedReader{r: reader, chunk: chunk}, &body, int64(len(doc)))

			feed, err := p.ParseURL("http://gofeed.invalid/feed")
			require.ErrorIs(t, err, boom)
			require.NotErrorIs(t, err, gofeed.ErrResponseTooLarge)
			require.Nil(t, feed)
			require.Equal(t, 1, body.closes)
		})
	}
}

// TestRegressionCloseErrorDoesNotMaskEarlierError covers a response body
// whose Close fails: the close error may only surface when the request was
// otherwise fully successful, and the body is closed exactly once per call.
func TestRegressionCloseErrorDoesNotMaskEarlierError(t *testing.T) {
	closeBoom := errors.New("body close failed")
	readBoom := errors.New("body read failed")

	newParser := func(body *trackedBody, status int, max int64) *gofeed.Parser {
		p := gofeed.NewParser()
		p.MaxByteSize = max
		p.Client = &http.Client{Transport: regressionRoundTripper(func(*http.Request) (*http.Response, error) {
			return regressionResponse(status, body), nil
		})}
		return p
	}

	t.Run("StatusError", func(t *testing.T) {
		body := &trackedBody{r: strings.NewReader("server on fire"), closeErr: closeBoom}
		feed, err := newParser(body, http.StatusServiceUnavailable, 0).ParseURL("http://gofeed.invalid/feed")
		require.Nil(t, feed)
		var httpErr gofeed.HTTPError
		require.ErrorAs(t, err, &httpErr)
		require.Equal(t, http.StatusServiceUnavailable, httpErr.StatusCode)
		require.NotErrorIs(t, err, closeBoom)
		require.Equal(t, 1, body.closes)
	})

	t.Run("ReadError", func(t *testing.T) {
		reader := &dataWithErrorReader{data: []byte(regressionRSSFeed), err: readBoom}
		body := &trackedBody{r: reader, closeErr: closeBoom}
		feed, err := newParser(body, http.StatusOK, 0).ParseURL("http://gofeed.invalid/feed")
		require.Nil(t, feed)
		require.ErrorIs(t, err, readBoom)
		require.NotErrorIs(t, err, closeBoom)
		require.Equal(t, 1, body.closes)
	})

	t.Run("TooLarge", func(t *testing.T) {
		body := &trackedBody{r: strings.NewReader(regressionRSSFeed), closeErr: closeBoom}
		feed, err := newParser(body, http.StatusOK, 10).ParseURL("http://gofeed.invalid/feed")
		require.Nil(t, feed)
		require.ErrorIs(t, err, gofeed.ErrResponseTooLarge)
		require.NotErrorIs(t, err, closeBoom)
		require.Equal(t, 1, body.closes)
	})

	t.Run("ParseError", func(t *testing.T) {
		body := &trackedBody{r: strings.NewReader("this is not a feed"), closeErr: closeBoom}
		feed, err := newParser(body, http.StatusOK, 0).ParseURL("http://gofeed.invalid/feed")
		require.Nil(t, feed)
		require.ErrorIs(t, err, gofeed.ErrFeedTypeNotDetected)
		require.NotErrorIs(t, err, closeBoom)
		require.Equal(t, 1, body.closes)
	})

	t.Run("SuccessWithCloseError", func(t *testing.T) {
		body := &trackedBody{r: strings.NewReader(regressionRSSFeed), closeErr: closeBoom}
		feed, err := newParser(body, http.StatusOK, 0).ParseURL("http://gofeed.invalid/feed")
		require.ErrorIs(t, err, closeBoom)
		require.NotNil(t, feed)
		require.Equal(t, 1, body.closes)
	})

	t.Run("Success", func(t *testing.T) {
		body := &trackedBody{r: strings.NewReader(regressionRSSFeed)}
		feed, err := newParser(body, http.StatusOK, 0).ParseURL("http://gofeed.invalid/feed")
		require.NoError(t, err)
		require.NotNil(t, feed)
		require.Equal(t, "regression-rss", feed.Title)
		require.Equal(t, 1, body.closes)
	})
}

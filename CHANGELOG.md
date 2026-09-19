# Changelog

## Unreleased

### Fixed: data race on first concurrent use of a shared `Parser`

Call chain: `Parser.ParseURL` / `Parser.ParseURLWithContext` → `httpClient()`.
When `Parser.Client` was left unset, `httpClient()` lazily wrote the shared
default client back into the `Client` field. A crawler sharing one
configured-then-read-only `Parser` across goroutines therefore raced on that
field the first time `ParseURL` ran concurrently; after the first call the
field stayed populated, which is why the race "disappeared" after warmup.
`httpClient()` now returns the shared default without storing it, matching
the existing translator accessors, so a read-only `Parser` is safe for
concurrent `ParseURL`, `ParseURLWithContext`, and `Parse` across RSS, Atom,
and JSON feeds from the very first call. An explicitly assigned `Client` is
still used for every request and is never replaced. Ordinary successful
responses did not expose this because the write-back is idempotent and
single-threaded callers never observe a torn read — only `-race` on concurrent
first use did.

### Fixed: `ErrResponseTooLarge` now outranks a read error on the same final read

Call chain: `Parser.ParseURLWithContext` → `limitedReader.Read` →
`Parser.Parse`. With a chunked upstream, the last `Read` can deliver the
bytes that cross `MaxByteSize` together with a non-EOF transport error.
Previously the transport error was kept and the overflow went unreported, so
callers saw the transport error and `errors.Is(err, ErrResponseTooLarge)` was
false. The overflow now wins: once more than `MaxByteSize` bytes have been
read, the read reports `ErrResponseTooLarge` regardless of any error
attached to the same read, and at most one byte past the limit is ever
pulled from the body to make that determination. When the limit is not
exceeded, an underlying read error is propagated unchanged and remains
visible to `errors.Is`. Ordinary successful responses did not expose this
because a well-behaved body returns `io.EOF` separately from data, so the
overflowing read never carried a conflicting error.

### Fixed: `Body.Close` errors no longer mask earlier failures

Call chain: `Parser.ParseURLWithContext` → deferred `resp.Body.Close()`.
The deferred close unconditionally assigned a close failure to the returned
error, so a 503 response surfaced the close error instead of the `HTTPError`
carrying the status code, and the same clobbering applied to 2xx responses
whose body had already failed the size limit or parsing. The close error is
now only returned when no earlier error exists — status, read, and parse
errors take priority — and the body is still closed exactly once on every
return path. Ordinary successful responses did not expose this because
closing a fully drained, healthy body rarely fails; the overwrite only
showed when a failure response also happened to have a failing `Close`.

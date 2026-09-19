# Changelog

## Unreleased

### Fixed: data race on `Parser.Client` during first concurrent `ParseURL`

- **Symptom:** a `Parser` without an explicitly set `Client`, shared across
  goroutines as a read-only configuration, reported concurrent read/write
  access on the `Client` field under the race detector when many goroutines
  made their first `ParseURL` call at the same time. Results were usually
  correct, and the reports stopped after one warm-up call.
- **Call chain:** `ParseURL` / `ParseURLWithContext` resolve the HTTP client
  through an internal accessor. When `Parser.Client` was unset, the accessor
  lazily wrote the shared default client back into the struct field before
  returning it, so N concurrent first calls performed N unsynchronized
  writes to the same field.
- **Fix:** the accessor now returns the shared default client without
  writing it back. A parser that is only read after construction is safe for
  concurrent use by `ParseURL`, `ParseURLWithContext`, and `Parse` across
  all three feed formats. An explicitly assigned custom `Client` is still
  used for every request and is never replaced by the default.
- **Why normal successful responses did not expose it:** every racing
  goroutine wrote the identical pointer, so the data race was benign in
  practice — only the race detector (or a formal memory-model check) can
  observe it. After the first call completed, the field stayed non-nil and
  every later call took the read-only branch, which is why a single warm-up
  call made the reports disappear.

### Fixed: `ErrResponseTooLarge` lost to a transport error on the same read

- **Symptom:** against a chunked upstream whose final `Read` delivered both
  the bytes crossing `MaxByteSize` and a non-EOF transport error, callers
  received the transport error and `errors.Is(err, ErrResponseTooLarge)`
  was false, even though the response did exceed the limit.
- **Call chain:** `ParseURLWithContext` wraps the response body in an
  internal limited reader. When a single underlying `Read` returned more
  bytes than the remaining budget together with an error, the overflow was
  only reported if that error was nil or `io.EOF`; any other error was kept
  and the overflow was silently dropped.
- **Fix / error priority:** once the byte count proves the response exceeds
  a positive `MaxByteSize`, `ErrResponseTooLarge` takes priority over any
  error attached to the same read. At most one byte beyond the limit is
  read from the body to make that determination. When the limit is not
  exceeded, an underlying read error propagates unchanged and remains
  findable with `errors.Is`.
- **Why normal successful responses did not expose it:** a healthy response
  ends with `io.EOF` (or exactly at the limit), and the old code already
  converted those cases to `ErrResponseTooLarge`. Only a transport that
  fails on the very read carrying the overflow byte — rare for well-behaved
  servers — surfaced the misplaced priority.

### Fixed: response body `Close` error masking the real error

- **Symptom:** when a request had already failed — a non-2xx status such as
  503, a body read error, a `MaxByteSize` overflow, or a feed parse error —
  and closing the response body also failed, the caller saw the close error
  instead of the meaningful one (e.g. the `HTTPError` carrying the status
  code).
- **Call chain:** `ParseURLWithContext` defers `resp.Body.Close()` and
  unconditionally assigned a close failure to the named return error,
  overwriting whatever the status check, the limited reader, or the feed
  parser had already produced.
- **Fix / error priority:** a close error is returned only when the request
  was otherwise completely successful; an existing status, read, or parse
  error always wins. The body is still closed exactly once on every return
  path, including context cancellation.
- **Why normal successful responses did not expose it:** successful
  responses have no earlier error to mask, and well-behaved bodies close
  cleanly, so the unconditional assignment was a no-op in the common case.
  It only became visible when a failing upstream also produced a failing
  `Close`.

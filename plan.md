# Project: `api-resilience`

## Goal

A small, standalone Rust library crate that gives any HTTP-based API wrapper crate retry and rate-limiting behavior with minimal integration effort. It is infrastructure, not a domain library — it must know nothing about FRED, ECB, or any specific API.

It will be depended on by `fred-wrapper` first, and by future wrapper crates (e.g. `ecb-wrapper`, `cftc-wrapper`) later. Each consuming wrapper owns its *own* rate-limit/retry configuration (e.g. FRED's limits are independent of CFTC's) — this crate provides the mechanism, not shared state across wrappers.

## Non-goals (explicitly out of scope)

- No HTTP client implementation. This crate does not know about `reqwest`, URLs, headers, or JSON — it operates purely on `tower::Service`.
- No circuit breaker, bulkhead, caching, or hedging in v1. Only retry + rate limiting. (`tower-resilience` supports these; do not wire them in yet — keep the public surface minimal until a real consumer needs more.)
- No centralized/cross-wrapper coordination (e.g. a global request queue across sources). Each wrapper's resilient service is independent.
- No async runtime other than `tokio`.

## Design summary (already decided — do not re-derive)

- Built on `tower::Service` / `tower::Layer` as the integration contract. A consuming wrapper implements `tower::Service<Req>` once for its raw transport (e.g. `RawFredTransport`), and this crate wraps that service with rate-limit + retry layers.
- Built on the `tower-resilience` crate family for the actual retry and rate-limiter algorithms — do not hand-roll retry/backoff or token-bucket logic; that's a solved problem and reimplementing it is out of scope.
- Public API is intentionally thin: a `ResilienceConfig` struct and a `resilient(inner, cfg)` function (or equivalent builder). Consumers should not need to touch `tower-resilience` types directly for the common case.

## Crate layout

```
api-resilience/
├── Cargo.toml
├── README.md
├── src/
│   ├── lib.rs        # public re-exports + `resilient()` entry point
│   ├── config.rs      # ResilienceConfig
│   └── error.rs       # ResilienceError wrapper type
└── tests/
    └── integration.rs # tests against a fake in-memory tower::Service
```

Keep it flat. Do not add module directories or split further unless a phase below explicitly calls for it.

## Dependencies (`Cargo.toml`)

```toml
[dependencies]
tower = { version = "0.5", features = ["retry", "limit", "util"] }
tower-resilience = { version = "0.9", features = ["retry", "ratelimiter"] }
tokio = { version = "1", features = ["rt", "time", "macros"] }
thiserror = "2"

[dev-dependencies]
tokio = { version = "1", features = ["full", "test-util"] }
```

Verify current version numbers against crates.io / docs.rs at implementation time — the versions above are a starting point, not guaranteed current.

## Public API to implement

```rust
// config.rs
pub struct ResilienceConfig {
    pub requests_per_second: u32,
    pub max_retries: u32,
    // add `initial_backoff: Duration` and `max_backoff: Duration` if
    // tower-resilience's RetryLayer builder requires/benefits from them —
    // check its actual builder API rather than assuming.
}

// error.rs
// Wraps errors that can originate from the resilience layers themselves
// (e.g. rate limiter rejection) distinctly from the inner service's own
// error type `E`, so callers can distinguish "my request failed" from
// "resilience layer rejected/exhausted retries."
#[derive(Debug, thiserror::Error)]
pub enum ResilienceError<E> {
    #[error("inner service error: {0}")]
    Inner(E),
    #[error("rate limited")]
    RateLimited,
    #[error("retries exhausted after {attempts} attempts")]
    RetriesExhausted { attempts: u32 },
}

// lib.rs
pub fn resilient<S, Req>(
    inner: S,
    cfg: ResilienceConfig,
) -> impl tower::Service<Req, Response = S::Response, Error = ResilienceError<S::Error>>
where
    S: tower::Service<Req> + Clone + Send + 'static,
    S::Future: Send,
    Req: Send + 'static,
{
    // implementation
}
```

Treat the signatures above as a strong starting point, not gospel — if `tower-resilience`'s actual `RetryLayer`/`RateLimiterLayer` APIs (check docs.rs for the installed version) require different bounds or a different composition order, adapt to match reality and note the deviation in the PR description.

## Implementation phases

### Phase 1 — Scaffold
- `cargo new --lib api-resilience`
- Add dependencies above
- Empty `resilient()` function that just returns `inner` unmodified (compiles, does nothing yet)
- `cargo build` passes

**Acceptance:** crate compiles with the public API shape above, even if unimplemented.

### Phase 2 — Rate limiting
- Wire `tower_resilience::ratelimiter::RateLimiterLayer` using `cfg.requests_per_second`
- Write an integration test using a fake `Service` (a closure-backed service or `tower::service_fn`) that counts invocations, and assert that N rapid calls are throttled to the configured rate (use `tokio::time::pause()` / `advance()` from `test-util` rather than real sleeps)

**Acceptance:** test proves throttling actually happens, not just that the code compiles.

### Phase 3 — Retry
- Wire `tower_resilience::retry::RetryLayer` using `cfg.max_retries` with exponential backoff
- Write an integration test using a fake service that fails N times then succeeds, and assert the call eventually succeeds within the retry budget
- Write a second test proving that exceeding `max_retries` surfaces `ResilienceError::RetriesExhausted`, not a panic or a silently swallowed error

**Acceptance:** both success-after-retries and exhausted-retries paths are tested.

### Phase 4 — Compose and finalize error handling
- Compose rate-limit + retry via `tower::ServiceBuilder` in the order: rate limit outermost, retry innermost (i.e. each retry attempt still respects the rate limit — confirm this ordering is actually correct for the intended semantics before finalizing; reason about it explicitly rather than guessing)
- Ensure `S::Error` is properly wrapped into `ResilienceError::Inner` rather than lost
- Add doc comments (`///`) on every public item — this crate's whole value is being easy for a wrapper author to pick up without reading `tower-resilience` source, so documentation quality matters here more than usual

**Acceptance:** `cargo doc --open` produces a clean, readable page for the crate root.

### Phase 5 — README and worked example
- README should show the exact end-to-end shape a consumer (like `fred-wrapper`) would use: implement `tower::Service` for a raw transport, call `resilient(transport, cfg)`, use the result.
- Include one runnable example under `examples/` if feasible (e.g. `examples/basic.rs` wrapping a `tower::service_fn` that simulates an HTTP call).

**Acceptance:** a developer who has never seen this crate can read the README and integrate it in under 10 minutes.

## Definition of done

- `cargo build`, `cargo test`, `cargo clippy --all-targets -- -D warnings`, and `cargo doc` all pass cleanly
- Every public item has a doc comment
- Tests cover: normal pass-through, rate-limit throttling, retry-then-succeed, retry-exhausted
- No dependency on anything outside `tower`, `tower-resilience`, `tokio`, `thiserror`
- README exists and matches the actual public API (keep it in sync if the API changes during implementation)

## Explicit request to the agent

If any step above conflicts with the actual current API of `tower` or `tower-resilience` (these are real external crates and this plan was written without direct access to their exact current source), prefer the real library's actual API over this plan's assumptions, and leave a short note in the README or a code comment explaining the deviation.

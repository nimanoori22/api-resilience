# api-resilience

Small retry and rate-limit middleware for Tower-based API wrapper crates. It has no
HTTP-client dependency: your wrapper supplies a `tower::Service`, and this crate
supplies the resilience policy.

```rust
use api_resilience::{resilient, ResilienceConfig};
use tower::ServiceExt;

# async fn example() {
let raw_transport = tower::service_fn(|request: String| async move {
    // Put your reqwest, hyper, or other HTTP-client call here.
    Ok::<_, std::io::Error>(format!("received {request}"))
});

let config = ResilienceConfig {
    requests_per_second: 5,
    max_retries: 2,
    ..ResilienceConfig::default()
};
let response = resilient(raw_transport, config)
    .oneshot("observations?series_id=GDP".to_owned())
    .await?;
assert_eq!(response, "received observations?series_id=GDP");
# Ok::<(), api_resilience::ResilienceError<std::io::Error>>(())
# }
```

`max_retries` means retries after the first request: `2` allows three total
attempts. Retries use exponential backoff starting at `initial_backoff`, capped by
`max_backoff`. Every attempt passes through the rate limiter, including retries.

The rate limiter waits for up to `rate_limit_timeout` for a permit, then returns
`ResilienceError::RateLimited`. Retryable inner request errors that persist through
all attempts return `ResilienceError::Failed`, preserving both the final error and
the actual number of attempts.

## Errors

Match `ResilienceError` when a caller needs to differentiate local throttling from
an exhausted upstream request:

```rust
# use api_resilience::ResilienceError;
# fn handle(error: ResilienceError<std::io::Error>) {
match error {
    ResilienceError::RateLimited => { /* wait or shed work */ }
    ResilienceError::Failed { error, attempts } => eprintln!("failed after {attempts} attempts: {error}"),
    ResilienceError::Inner(error) => eprintln!("transport error: {error}"),
}
# }
```

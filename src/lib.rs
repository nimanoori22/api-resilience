//! Retry and rate-limit middleware for HTTP-oriented Tower services.
//!
//! This crate deliberately knows nothing about HTTP clients, URLs, or response
//! formats. Implement [`tower::Service`] for a raw transport, then pass it to
//! [`resilient`]. Each returned service owns its own rate-limit state.
//!
//! ```
//! use api_resilience::{resilient, ResilienceConfig};
//! use tower::ServiceExt;
//!
//! # async fn example() {
//! let transport = tower::service_fn(|request: String| async move {
//!     Ok::<_, std::io::Error>(format!("response for {request}"))
//! });
//!
//! let mut service = resilient(transport, ResilienceConfig::default());
//! let response = service.oneshot("series?symbol=GDP".to_owned()).await.unwrap();
//! assert_eq!(response, "response for series?symbol=GDP");
//! # }
//! ```

mod config;
mod error;

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    task::{Context, Poll},
};

use tower::{Layer, Service, ServiceExt};
use tower_resilience::{
    ratelimiter::{RateLimiter, RateLimiterLayer, RateLimiterServiceError},
    retry::{ExponentialBackoff, RetryLayer},
};

pub use config::ResilienceConfig;
pub use error::ResilienceError;

/// Wraps a raw Tower service with per-instance rate limiting and retry behavior.
///
/// A retryable error from `inner` is retried with exponential backoff. The
/// rate limiter is inside the retry layer, so *each* attempt consumes a permit.
/// A rate-limit rejection is not retried; it becomes [`ResilienceError::RateLimited`].
///
/// `Req` must be [`Clone`] because retrying requires re-sending the request.
/// `max_retries` is the number of retries after the first request, rather than
/// the total number of attempts used by `tower-resilience` internally.
pub fn resilient<S, Req>(
    inner: S,
    cfg: ResilienceConfig,
) -> impl Service<
    Req, 
    Response = S::Response, 
    Error = ResilienceError<S::Error>, 
    Future = Pin<Box<dyn std::future::Future<Output = Result<S::Response, ResilienceError<S::Error>>> + Send>>>
where
    S: Service<Req> + Clone + Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
    Req: Clone + Send + 'static,
{
    let attempts = cfg.max_retries.saturating_add(1);
    let rate_limited = RateLimiterLayer::builder()
        .limit_for_period(cfg.requests_per_second as usize)
        .refresh_period(std::time::Duration::from_secs(1))
        .timeout_duration(cfg.rate_limit_timeout)
        .build()
        .layer(inner);
    ResilientService {
        inner: rate_limited,
        max_attempts: attempts,
        initial_backoff: cfg.initial_backoff,
        max_backoff: cfg.max_backoff,
    }
}

struct ResilientService<Raw> {
    inner: RateLimiter<Raw>,
    max_attempts: u32,
    initial_backoff: std::time::Duration,
    max_backoff: std::time::Duration,
}

impl<Raw, Req, E> Service<Req> for ResilientService<Raw>
where
    Raw: Service<Req, Error = E> + Clone + Send + 'static,
    Raw::Response: Send + 'static,
    Raw::Future: Send + 'static,
    E: Send + 'static,
    Req: Clone + Send + 'static,
{
    type Response = Raw::Response;
    type Error = ResilienceError<E>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|error| match error {
            RateLimiterServiceError::RateLimited => ResilienceError::RateLimited,
            RateLimiterServiceError::Inner(error) => ResilienceError::Inner(error),
        })
    }

    fn call(&mut self, request: Req) -> Self::Future {
        // `tower-resilience` returns the final inner error rather than an
        // exhaustion-specific error. Its per-layer `on_error` callback is the
        // only supplied per-call signal that retries were actually exhausted.
        let observed_attempts = Arc::new(AtomicU32::new(0));
        let callback_attempts = Arc::clone(&observed_attempts);
        let mut retry = RetryLayer::<Req, Raw::Response, RateLimiterServiceError<E>>::builder()
            .max_attempts(self.max_attempts as usize)
            .backoff(ExponentialBackoff::new(self.initial_backoff).max_interval(self.max_backoff))
            .retry_on(|error| matches!(error, RateLimiterServiceError::Inner(_)))
            .on_error(move |attempts| {
                callback_attempts.store(attempts.min(u32::MAX as usize) as u32, Ordering::Relaxed);
            })
            .build()
            .layer(self.inner.clone());
        Box::pin(async move {
            retry.ready().await.map_err(|error| match error {
                RateLimiterServiceError::RateLimited => ResilienceError::RateLimited,
                RateLimiterServiceError::Inner(error) => ResilienceError::Inner(error),
            })?;

            match retry.call(request).await {
                Ok(response) => Ok(response),
                Err(RateLimiterServiceError::RateLimited) => Err(ResilienceError::RateLimited),
                Err(RateLimiterServiceError::Inner(error)) => {
                    match observed_attempts.load(Ordering::Relaxed) {
                        0 => Err(ResilienceError::Inner(error)),
                        attempts => Err(ResilienceError::Failed { error, attempts }),
                    }
                }
            }
        })
    }
}

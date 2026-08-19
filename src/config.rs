use std::time::Duration;

/// Settings for one independently resilient API transport.
///
/// Each call to [`crate::resilient`] creates independent rate-limit state. Configure
/// every upstream API separately; this type does not coordinate limits across crates
/// or service instances.
#[derive(Clone, Debug)]
pub struct ResilienceConfig {
    /// Maximum number of request attempts allowed during each one-second window.
    pub requests_per_second: u32,
    /// Number of additional attempts after an initial failed request.
    ///
    /// For example, `max_retries: 2` permits at most three total attempts.
    pub max_retries: u32,
    /// Delay before the first retry; later delays double, up to [`Self::max_backoff`].
    pub initial_backoff: Duration,
    /// Upper bound for an exponential retry delay.
    pub max_backoff: Duration,
    /// Longest time a request may wait for a rate-limit permit before it is rejected.
    pub rate_limit_timeout: Duration,
}

impl Default for ResilienceConfig {
    fn default() -> Self {
        Self {
            requests_per_second: 10,
            max_retries: 2,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            rate_limit_timeout: Duration::from_secs(1),
        }
    }
}

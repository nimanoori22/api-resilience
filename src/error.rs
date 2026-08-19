/// Errors returned by a service created with [`crate::resilient`].
///
/// Match on this type to distinguish a transport error, a local rate-limit
/// rejection, and an error returned after retry attempts were exhausted.
#[derive(Debug, thiserror::Error)]
pub enum ResilienceError<E> {
    /// An error reported by the inner service without retry exhaustion.
    #[error("inner service error: {0}")]
    Inner(E),
    /// A request could not obtain a rate-limit permit before the configured timeout.
    #[error("rate limited")]
    RateLimited,
    /// A retryable inner-service error that persisted through every permitted attempt.
    #[error("request failed after {attempts} attempts: {error}")]
    Failed {
        /// The final error returned by the inner service.
        error: E,
        /// Total attempts made, including the initial request.
        attempts: u32,
    },
}

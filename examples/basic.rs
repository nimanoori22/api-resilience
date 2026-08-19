//! Minimal end-to-end use of `api-resilience`.

use api_resilience::{ResilienceConfig, resilient};
use tower::ServiceExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = tower::service_fn(|request: String| async move {
        Ok::<_, std::io::Error>(format!("completed: {request}"))
    });

    let response = resilient(transport, ResilienceConfig::default())
        .oneshot("GET /health".to_owned())
        .await?;
    println!("{response}");
    Ok(())
}

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use api_resilience::{ResilienceConfig, ResilienceError, resilient};
use tower::{Service, ServiceExt};

fn config() -> ResilienceConfig {
    ResilienceConfig {
        requests_per_second: 100,
        max_retries: 0,
        initial_backoff: Duration::ZERO,
        max_backoff: Duration::ZERO,
        rate_limit_timeout: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn passes_through_a_successful_request() {
    let service = tower::service_fn(|request: String| async move {
        Ok::<_, std::io::Error>(format!("ok: {request}"))
    });

    assert_eq!(
        resilient(service, config())
            .oneshot("hello".into())
            .await
            .unwrap(),
        "ok: hello"
    );
}

struct RequiresReadiness {
    was_readied: Arc<AtomicBool>,
}

impl Clone for RequiresReadiness {
    fn clone(&self) -> Self {
        Self {
            was_readied: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Service<()> for RequiresReadiness {
    type Response = ();
    type Error = std::io::Error;
    type Future = std::future::Ready<Result<(), std::io::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.was_readied.store(true, Ordering::SeqCst);
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        if self.was_readied.swap(false, Ordering::SeqCst) {
            std::future::ready(Ok(()))
        } else {
            std::future::ready(Err(std::io::Error::other("call without poll_ready")))
        }
    }
}

#[tokio::test]
async fn readies_the_per_call_retry_service_before_calling_it() {
    let service = RequiresReadiness {
        was_readied: Arc::new(AtomicBool::new(false)),
    };

    resilient(service, config()).oneshot(()).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn rate_limit_delays_another_request_until_the_next_window() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let service = tower::service_fn(move |_: ()| {
        let counter = Arc::clone(&counter);
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok::<_, std::io::Error>(())
        }
    });
    let cfg = ResilienceConfig {
        requests_per_second: 1,
        ..config()
    };
    let mut service = resilient(service, cfg);

    service.ready().await.unwrap().call(()).await.unwrap();
    let pending = service.call(());
    tokio::pin!(pending);
    assert!(futures_poll_once(pending.as_mut()).is_none());
    tokio::time::advance(Duration::from_secs(1)).await;
    pending.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn rate_limit_allows_bursts_up_to_the_configured_limit() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let service = tower::service_fn(move |_: ()| {
        let counter = Arc::clone(&counter);
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok::<_, std::io::Error>(())
        }
    });
    let cfg = ResilienceConfig {
        requests_per_second: 3,
        ..config()
    };
    let mut service = resilient(service, cfg);

    for _ in 0..3 {
        service.ready().await.unwrap().call(()).await.unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    let pending = service.call(());
    tokio::pin!(pending);
    assert!(
        futures_poll_once(pending.as_mut()).is_none(),
        "the fourth request should be throttled until the next window"
    );

    tokio::time::advance(Duration::from_secs(1)).await;
    pending.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn retries_then_succeeds() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let service = tower::service_fn(move |_: ()| {
        let counter = Arc::clone(&counter);
        async move {
            if counter.fetch_add(1, Ordering::SeqCst) < 2 {
                Err(std::io::Error::other("temporary"))
            } else {
                Ok(())
            }
        }
    });
    let cfg = ResilienceConfig {
        max_retries: 2,
        ..config()
    };

    resilient(service, cfg).oneshot(()).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn backoff_increases_between_retries() {
    let call_times = Arc::new(std::sync::Mutex::new(Vec::new()));
    let times = Arc::clone(&call_times);
    let service = tower::service_fn(move |_: ()| {
        times.lock().unwrap().push(tokio::time::Instant::now());
        async { Err::<(), _>(std::io::Error::other("always fails")) }
    });
    let cfg = ResilienceConfig {
        max_retries: 3,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(10),
        ..config()
    };

    let _ = resilient(service, cfg).oneshot(()).await;

    let times = call_times.lock().unwrap();
    assert_eq!(times.len(), 4, "۱ تلاش اولیه + ۳ retry = ۴ فراخوانی");

    let gap1 = times[1] - times[0];
    let gap2 = times[2] - times[1];
    let gap3 = times[3] - times[2];

    assert!(gap2 >= gap1, "backoff نباید کوچیک‌تر بشه بین تلاش‌ها");
    assert!(gap3 >= gap2, "backoff نباید کوچیک‌تر بشه بین تلاش‌ها");
}

#[tokio::test]
async fn preserves_the_final_error_after_exhausting_retries() {
    let service =
        tower::service_fn(|_: ()| async { Err::<(), _>(std::io::Error::other("temporary")) });
    let cfg = ResilienceConfig {
        max_retries: 2,
        ..config()
    };

    match resilient(service, cfg).oneshot(()).await {
        Err(ResilienceError::Failed { error, attempts }) => {
            assert_eq!(attempts, 3);
            assert_eq!(error.kind(), std::io::ErrorKind::Other);
            assert_eq!(error.to_string(), "temporary");
        }
        _ => panic!("expected the final inner error after retries"),
    }
}

fn futures_poll_once<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> Option<F::Output> {
    use std::task::{Context, Poll, Waker};
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.poll(&mut context) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    }
}
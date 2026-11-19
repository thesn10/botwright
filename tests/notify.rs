use std::time::{Duration, Instant};

use botwright::BoxError;
use botwright::notify::{Notification, NotificationSink, Notifier, QueuedNotifier};

struct HangingNetwork;

impl NotificationSink for HangingNetwork {
    async fn send(&self, _: Vec<Notification>) -> Result<(), BoxError> {
        std::future::pending::<()>().await;
        Ok(())
    }
}

#[tokio::test]
async fn notify_returns_immediately_and_counts_overflow() {
    let notifier = QueuedNotifier::spawn(HangingNetwork, 10);
    let started = Instant::now();
    (0..100).for_each(|i| notifier.notify(Notification::warning("lag", format!("event {i}"))));
    assert!(started.elapsed() < Duration::from_millis(50));
    assert_eq!(notifier.dropped(), 90);

    tokio::task::yield_now().await;
    (0..20).for_each(|i| notifier.notify(Notification::warning("lag", format!("again {i}"))));
    assert_eq!(
        notifier.dropped(),
        100,
        "the worker took one batch of 10, the queue holds 10 more"
    );
}

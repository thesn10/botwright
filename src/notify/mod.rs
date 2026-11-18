//! Non-blocking notifications for operators.

#[cfg(feature = "telegram")]
mod telegram;

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;

#[cfg(feature = "telegram")]
pub use telegram::TelegramNotifier;

use crate::{BoxError, obs};

/// Severity of a notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Level {
    /// Informational.
    Info,
    /// Needs attention.
    Warning,
    /// Something failed.
    Error,
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Info => "INFO",
            Self::Warning => "WARNING",
            Self::Error => "ERROR",
        })
    }
}

/// A message for operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    /// Severity.
    pub level: Level,
    /// Short summary; notifications with equal level and title may be bundled.
    pub title: String,
    /// Details.
    pub body: String,
}

impl Notification {
    /// Creates a notification.
    pub fn new(level: Level, title: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            level,
            title: title.into(),
            body: body.into(),
        }
    }

    /// An [`Level::Info`] notification.
    pub fn info(title: impl Into<String>, body: impl Into<String>) -> Self {
        Self::new(Level::Info, title, body)
    }

    /// A [`Level::Warning`] notification.
    pub fn warning(title: impl Into<String>, body: impl Into<String>) -> Self {
        Self::new(Level::Warning, title, body)
    }

    /// A [`Level::Error`] notification.
    pub fn error(title: impl Into<String>, body: impl Into<String>) -> Self {
        Self::new(Level::Error, title, body)
    }
}

/// Receives notifications. `notify` must never block.
pub trait Notifier: Send + Sync + 'static {
    /// Hands over a notification without waiting.
    fn notify(&self, notification: Notification);
}

/// Shared, cloneable notifier used by the engine and strategies.
#[derive(Clone)]
pub struct NotifierHandle(Arc<dyn Notifier>);

impl NotifierHandle {
    /// Wraps a notifier.
    pub fn new(notifier: impl Notifier) -> Self {
        Self(Arc::new(notifier))
    }
}

impl Notifier for NotifierHandle {
    fn notify(&self, notification: Notification) {
        self.0.notify(notification);
    }
}

impl Default for NotifierHandle {
    /// A [`LogNotifier`].
    fn default() -> Self {
        Self::new(LogNotifier)
    }
}

impl fmt::Debug for NotifierHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NotifierHandle")
    }
}

impl From<LogNotifier> for NotifierHandle {
    fn from(notifier: LogNotifier) -> Self {
        Self::new(notifier)
    }
}

impl From<QueuedNotifier> for NotifierHandle {
    fn from(notifier: QueuedNotifier) -> Self {
        Self::new(notifier)
    }
}

/// Writes notifications through `tracing`.
#[derive(Debug, Clone, Copy, Default)]
pub struct LogNotifier;

impl Notifier for LogNotifier {
    fn notify(&self, n: Notification) {
        match n.level {
            Level::Info => {
                tracing::info!(target: "botwright::notify", title = %n.title, "{}", n.body)
            }
            Level::Warning => {
                tracing::warn!(target: "botwright::notify", title = %n.title, "{}", n.body)
            }
            Level::Error => {
                tracing::error!(target: "botwright::notify", title = %n.title, "{}", n.body)
            }
        }
    }
}

/// Delivers batches of notifications, e.g. over HTTP. Driven by a [`QueuedNotifier`] worker.
pub trait NotificationSink: Send + Sync + 'static {
    /// Sends all notifications that queued up since the last call, oldest first.
    fn send(&self, batch: Vec<Notification>) -> impl Future<Output = Result<(), BoxError>> + Send;
}

/// Bounded queue in front of a [`NotificationSink`]; `notify` never waits, overflow is dropped and counted.
#[derive(Clone)]
pub struct QueuedNotifier {
    tx: mpsc::Sender<Notification>,
    dropped: Arc<AtomicU64>,
    dropped_metric: obs::Counter,
}

impl QueuedNotifier {
    /// Spawns the worker on the current tokio runtime.
    ///
    /// # Panics
    /// Outside of a tokio runtime.
    pub fn spawn<S: NotificationSink>(sink: S, capacity: usize) -> Self {
        let (tx, mut rx) = mpsc::channel::<Notification>(capacity.max(1));
        tokio::spawn(async move {
            while let Some(first) = rx.recv().await {
                let mut batch = vec![first];
                while let Ok(next) = rx.try_recv() {
                    batch.push(next);
                }
                if let Err(err) = sink.send(batch).await {
                    tracing::warn!(error = %err, "sending notifications failed");
                }
            }
        });
        Self {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
            dropped_metric: obs::counter(obs::NOTIFICATIONS_DROPPED, &[]),
        }
    }

    /// Notifications dropped because the queue was full.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Acquire)
    }
}

impl Notifier for QueuedNotifier {
    fn notify(&self, notification: Notification) {
        if self.tx.try_send(notification).is_err() {
            self.dropped.fetch_add(1, Ordering::AcqRel);
            self.dropped_metric.increment(1);
        }
    }
}

impl fmt::Debug for QueuedNotifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueuedNotifier")
            .field("dropped", &self.dropped())
            .finish()
    }
}

/// Groups notifications with equal level and title, keeping first-seen order.
pub fn bundle(batch: Vec<Notification>) -> Vec<(Notification, usize)> {
    let mut groups: Vec<(Notification, usize)> = Vec::new();
    for n in batch {
        match groups
            .iter_mut()
            .find(|(g, _)| g.level == n.level && g.title == n.title)
        {
            Some((group, count)) => {
                *count += 1;
                group.body.push('\n');
                group.body.push_str(&n.body);
            }
            None => groups.push((n, 1)),
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundles_equal_titles() {
        let groups = bundle(vec![
            Notification::warning("lag", "a"),
            Notification::info("start", "x"),
            Notification::warning("lag", "b"),
        ]);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].1, 2);
        assert_eq!(groups[0].0.body, "a\nb");
        assert_eq!(groups[1].0.title, "start");
    }
}

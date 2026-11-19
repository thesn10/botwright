use std::time::Duration;

use futures::{StreamExt, stream};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time::Instant;

use super::{Level, Notification, NotificationSink, bundle};
use crate::BoxError;

const MAX_TEXT: usize = 4096;
const MAX_ATTEMPTS: u32 = 3;

/// Sends notifications to a Telegram chat with the Bot API `sendMessage`. Equal notifications
/// in a batch are bundled into one message, messages to the chat are spaced by a minimum
/// interval, and HTTP 429 is retried after `retry_after`.
///
/// Wrap it in a [`QueuedNotifier`](super::QueuedNotifier) so `notify` never blocks.
pub struct TelegramNotifier {
    client: reqwest::Client,
    url: String,
    chat_id: String,
    thread_id: Option<i64>,
    min_interval: Duration,
    last_sent: Mutex<Option<Instant>>,
}

impl std::fmt::Debug for TelegramNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelegramNotifier")
            .field("chat_id", &self.chat_id)
            .field("thread_id", &self.thread_id)
            .field("min_interval", &self.min_interval)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct ApiResponse {
    ok: bool,
    description: Option<String>,
    parameters: Option<ResponseParameters>,
}

#[derive(Deserialize)]
struct ResponseParameters {
    retry_after: Option<u64>,
}

impl TelegramNotifier {
    /// `chat_id` is a numeric id or `@channelname`. Messages are at least 1 s apart.
    pub fn new(token: impl AsRef<str>, chat_id: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: format!("https://api.telegram.org/bot{}/sendMessage", token.as_ref()),
            chat_id: chat_id.into(),
            thread_id: None,
            min_interval: Duration::from_secs(1),
            last_sent: Mutex::new(None),
        }
    }

    /// Posts into a forum topic.
    pub fn thread_id(mut self, thread_id: i64) -> Self {
        self.thread_id = Some(thread_id);
        self
    }

    /// Minimum time between two messages; group chats allow about 20 per minute.
    pub fn min_interval(mut self, interval: Duration) -> Self {
        self.min_interval = interval;
        self
    }

    /// Replaces the full `sendMessage` URL, e.g. for a local Bot API server.
    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    async fn send_text(&self, text: String) -> Result<(), BoxError> {
        let mut body = serde_json::json!({ "chat_id": self.chat_id, "text": text });
        if let Some(thread_id) = self.thread_id {
            body["message_thread_id"] = thread_id.into();
        }
        let mut last_sent = self.last_sent.lock().await;
        for attempt in 1..=MAX_ATTEMPTS {
            if let Some(sent) = self
                .attempt(&body, attempt, &mut last_sent)
                .await
                .transpose()
            {
                return sent;
            }
        }
        Err("telegram sendMessage failed: rate limited".into())
    }

    /// `Ok(None)` means rate limited, retry now.
    async fn attempt(
        &self,
        body: &serde_json::Value,
        attempt: u32,
        last_sent: &mut Option<Instant>,
    ) -> Result<Option<()>, BoxError> {
        if let Some(last) = *last_sent {
            tokio::time::sleep_until(last + self.min_interval).await;
        }
        let response = self.client.post(&self.url).json(body).send().await;
        *last_sent = Some(Instant::now());
        let response = response?;
        let status = response.status();
        let api: ApiResponse = response.json().await?;
        if api.ok {
            return Ok(Some(()));
        }
        match api.parameters.and_then(|p| p.retry_after) {
            Some(secs) if status.as_u16() == 429 && attempt < MAX_ATTEMPTS => {
                tracing::debug!(secs, "telegram rate limit");
                tokio::time::sleep(Duration::from_secs(secs)).await;
                Ok(None)
            }
            _ => {
                let description = api.description.unwrap_or_default();
                Err(format!("telegram sendMessage failed ({status}): {description}").into())
            }
        }
    }
}

fn format(notification: &Notification, count: usize) -> String {
    let marker = match notification.level {
        Level::Info => "ℹ️",
        Level::Warning => "⚠️",
        Level::Error => "🛑",
    };
    let mut text = if count > 1 {
        format!(
            "{marker} {} (×{count})\n{}",
            notification.title, notification.body
        )
    } else {
        format!("{marker} {}\n{}", notification.title, notification.body)
    };
    if text.chars().count() > MAX_TEXT {
        text = text.chars().take(MAX_TEXT - 1).chain(['…']).collect();
    }
    text
}

impl NotificationSink for TelegramNotifier {
    async fn send(&self, batch: Vec<Notification>) -> Result<(), BoxError> {
        let errors: Vec<String> = stream::iter(bundle(batch))
            .filter_map(|(notification, count)| async move {
                let sent = self.send_text(format(&notification, count)).await;
                sent.err()
                    .map(|err| format!("`{}`: {err}", notification.title))
            })
            .collect()
            .await;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; ").into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_bundles_and_truncates() {
        let n = Notification::warning("lag", "a\nb");
        assert_eq!(format(&n, 2), "⚠️ lag (×2)\na\nb");
        let long = Notification::info("t", "x".repeat(5000));
        let text = format(&long, 1);
        assert_eq!(text.chars().count(), MAX_TEXT);
        assert!(text.ends_with('…'));
    }
}

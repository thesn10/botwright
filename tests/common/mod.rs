#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use botwright::{BoxError, Envelope, Strategy, StrategyCtx, SystemEvent};

#[derive(Debug, Clone, PartialEq)]
pub enum Seen {
    Event {
        source: String,
        seq: u64,
        value: u64,
    },
    System(SystemEvent),
}

#[derive(Clone, Default)]
pub struct Log(Arc<Mutex<Vec<Seen>>>);

impl Log {
    pub fn push(&self, seen: Seen) {
        self.0.lock().unwrap().push(seen);
    }

    pub fn all(&self) -> Vec<Seen> {
        self.0.lock().unwrap().clone()
    }

    pub fn values(&self, source: &str) -> Vec<u64> {
        self.all()
            .into_iter()
            .filter_map(|s| match s {
                Seen::Event {
                    source: src, value, ..
                } if src == source => Some(value),
                _ => None,
            })
            .collect()
    }

    pub fn seqs(&self, source: &str) -> Vec<u64> {
        self.all()
            .into_iter()
            .filter_map(|s| match s {
                Seen::Event {
                    source: src, seq, ..
                } if src == source => Some(seq),
                _ => None,
            })
            .collect()
    }

    pub fn systems(&self) -> Vec<SystemEvent> {
        self.all()
            .into_iter()
            .filter_map(|s| match s {
                Seen::System(e) => Some(e),
                _ => None,
            })
            .collect()
    }

    pub fn dropped_total(&self) -> u64 {
        self.systems()
            .iter()
            .map(|e| match e {
                SystemEvent::Dropped { count, .. } => *count,
                _ => 0,
            })
            .sum()
    }
}

pub struct Recorder {
    pub name: String,
    pub log: Log,
    pub delay: Duration,
    pub submit: bool,
}

impl Recorder {
    pub fn new(name: &str, log: &Log) -> Self {
        Self {
            name: name.to_owned(),
            log: log.clone(),
            delay: Duration::ZERO,
            submit: false,
        }
    }

    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    pub fn submitting(mut self) -> Self {
        self.submit = true;
        self
    }
}

impl Strategy<u64, u64> for Recorder {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_event(
        &mut self,
        event: &Envelope<u64>,
        ctx: &mut StrategyCtx<u64>,
    ) -> Result<(), BoxError> {
        self.log.push(Seen::Event {
            source: event.source.to_string(),
            seq: event.seq,
            value: *event.event,
        });
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        if self.submit {
            ctx.submit(*event.event).await?;
        }
        Ok(())
    }

    async fn on_system(
        &mut self,
        event: &SystemEvent,
        _ctx: &mut StrategyCtx<u64>,
    ) -> Result<(), BoxError> {
        self.log.push(Seen::System(event.clone()));
        Ok(())
    }
}

pub fn is_sorted_strictly(values: &[u64]) -> bool {
    values.windows(2).all(|w| w[0] < w[1])
}

//! Deterministic testing: replay recorded events, record live events, capture actions and run
//! the engine on a paused clock.

use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{TryStream, TryStreamExt, stream};
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::{
    BoxError, Collector, CollectorError, EngineBuilder, Executor, ExecutorOpts, ExitReason, Item,
};

/// One line of a JSONL recording.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record<E> {
    /// Milliseconds since the recording started.
    pub at_ms: u64,
    /// The event.
    pub event: E,
}

/// Replay speed of a [`ReplayCollector`].
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Pace {
    /// No waiting between events.
    #[default]
    AsFastAsPossible,
    /// Original spacing.
    Original,
    /// Original spacing divided by the factor.
    Speedup(f64),
}

/// Emits a fixed sequence of events. The cursor is the index of the next event, so a reconnect
/// continues where the stream broke off. Exhausted after the last event.
pub struct ReplayCollector<E> {
    name: String,
    events: Arc<Vec<Record<E>>>,
    pace: Pace,
    position: Arc<AtomicUsize>,
}

impl<E> ReplayCollector<E> {
    /// Events without timing.
    pub fn from_events(name: impl Into<String>, events: impl IntoIterator<Item = E>) -> Self {
        Self::from_records(
            name,
            events.into_iter().map(|event| Record { at_ms: 0, event }),
        )
    }

    /// Events with offsets from the start.
    pub fn from_timed(
        name: impl Into<String>,
        events: impl IntoIterator<Item = (Duration, E)>,
    ) -> Self {
        Self::from_records(
            name,
            events.into_iter().map(|(at, event)| Record {
                at_ms: at.as_millis() as u64,
                event,
            }),
        )
    }

    /// Records as written by [`RecordingCollector`].
    pub fn from_records(
        name: impl Into<String>,
        records: impl IntoIterator<Item = Record<E>>,
    ) -> Self {
        Self {
            name: name.into(),
            events: Arc::new(records.into_iter().collect()),
            pace: Pace::default(),
            position: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Replay speed, default [`Pace::AsFastAsPossible`].
    pub fn pace(mut self, pace: Pace) -> Self {
        self.pace = pace;
        self
    }

    /// Number of events.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether there are no events.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

impl<E: DeserializeOwned> ReplayCollector<E> {
    /// Reads a JSONL file of [`Record`]s.
    pub fn from_jsonl(name: impl Into<String>, path: impl AsRef<Path>) -> io::Result<Self> {
        let records = BufReader::new(File::open(path)?)
            .lines()
            .filter_ok(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(&line?).map_err(io::Error::other))
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self::from_records(name, records))
    }
}

impl<E: Clone + Send + Sync + 'static> Collector<E> for ReplayCollector<E> {
    type Cursor = usize;

    fn name(&self) -> &str {
        &self.name
    }

    async fn connect(
        &self,
        resume: Option<usize>,
    ) -> Result<
        impl TryStream<Ok = Item<E, usize>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        let start = resume.unwrap_or(0);
        let events = Arc::clone(&self.events);
        let position = Arc::clone(&self.position);
        let pace = self.pace;
        let base = Instant::now();
        let origin = events.get(start).map_or(0, |r| r.at_ms);
        Ok(stream::unfold(start, move |index| {
            let events = Arc::clone(&events);
            let position = Arc::clone(&position);
            async move {
                let record = events.get(index)?;
                let offset = Duration::from_millis(record.at_ms.saturating_sub(origin));
                let wait = match pace {
                    Pace::AsFastAsPossible => None,
                    Pace::Original => Some(offset),
                    Pace::Speedup(factor) => Some(offset.div_f64(factor.max(f64::MIN_POSITIVE))),
                };
                if let Some(wait) = wait {
                    tokio::time::sleep_until(base + wait).await;
                }
                position.store(index + 1, Ordering::Release);
                Some((Ok(Item::new(record.event.clone(), index + 1)), index + 1))
            }
        }))
    }

    fn is_exhausted(&self) -> bool {
        self.position.load(Ordering::Acquire) >= self.events.len()
    }
}

/// Wraps a collector and appends every event as a JSONL [`Record`].
pub struct RecordingCollector<C> {
    inner: C,
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    started: Instant,
}

impl<C> RecordingCollector<C> {
    /// Records into a new file at `path`.
    pub fn new(inner: C, path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self::with_writer(
            inner,
            BufWriter::new(File::create(path)?),
        ))
    }

    /// Records into any writer; each line is flushed.
    pub fn with_writer(inner: C, writer: impl Write + Send + 'static) -> Self {
        Self {
            inner,
            writer: Arc::new(Mutex::new(Box::new(writer))),
            started: Instant::now(),
        }
    }
}

impl<E, C> Collector<E> for RecordingCollector<C>
where
    E: Serialize + Send + Sync + 'static,
    C: Collector<E>,
{
    type Cursor = C::Cursor;

    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn connect(
        &self,
        resume: Option<C::Cursor>,
    ) -> Result<
        impl TryStream<Ok = Item<E, C::Cursor>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        let stream = self.inner.connect(resume).await?;
        let writer = Arc::clone(&self.writer);
        let started = self.started;
        Ok(stream.inspect_ok(move |item| {
            if let Item::Event { event, .. } = item {
                let record = Record {
                    at_ms: started.elapsed().as_millis() as u64,
                    event,
                };
                if let Err(err) = write_record(&writer, &record) {
                    tracing::warn!(error = %err, "recording event failed");
                }
            }
        }))
    }

    fn is_exhausted(&self) -> bool {
        self.inner.is_exhausted()
    }
}

fn write_record<E: Serialize>(
    writer: &Mutex<Box<dyn Write + Send>>,
    record: &Record<&E>,
) -> io::Result<()> {
    let mut line = serde_json::to_vec(record).map_err(io::Error::other)?;
    line.push(b'\n');
    let mut writer = writer
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    writer.write_all(&line)?;
    writer.flush()
}

/// Collects every action it receives.
pub struct CapturingExecutor<A> {
    name: String,
    actions: Arc<Mutex<Vec<Arc<A>>>>,
}

impl<A> Clone for CapturingExecutor<A> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            actions: Arc::clone(&self.actions),
        }
    }
}

impl<A> CapturingExecutor<A> {
    /// Creates an empty capture; clones share it.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            actions: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// All actions so far, in execution order.
    pub fn actions(&self) -> Vec<Arc<A>> {
        self.actions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Number of actions so far.
    pub fn len(&self) -> usize {
        self.actions.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// Whether no action arrived yet.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<A: Send + Sync + 'static> Executor<A> for CapturingExecutor<A> {
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(&self, action: Arc<A>) -> Result<(), BoxError> {
        self.actions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(action);
        Ok(())
    }
}

/// Outcome of a [`Harness`] run.
#[derive(Debug)]
pub struct HarnessReport<A> {
    /// Every submitted action, in order.
    pub actions: Vec<Arc<A>>,
    /// How the engine ended.
    pub exit: ExitReason,
    /// `true` if the engine became idle, `false` if `max_duration` or a fatal error ended the run.
    pub completed: bool,
    /// Virtual time the run took.
    pub elapsed: Duration,
}

/// Runs an engine on a current-thread runtime with paused clock until every collector is
/// exhausted, all channels are empty and all tasks and actions are done.
pub struct Harness<E, A> {
    builder: EngineBuilder<E, A>,
    max_duration: Duration,
    tick: Duration,
}

impl<E, A> Harness<E, A>
where
    E: Send + Sync + 'static,
    A: Send + Sync + 'static,
{
    /// Adds a capturing executor to `builder`; all other executors run as configured.
    pub fn new(builder: EngineBuilder<E, A>) -> Self {
        Self {
            builder,
            max_duration: Duration::from_secs(3600),
            tick: Duration::from_millis(1),
        }
    }

    /// Upper bound of virtual time, default one hour.
    pub fn max_duration(mut self, max: Duration) -> Self {
        self.max_duration = max;
        self
    }

    /// Runs on a new thread with its own paused runtime and blocks until done.
    ///
    /// # Panics
    /// If the configuration is invalid or the engine task panics.
    pub fn run(self) -> HarnessReport<A> {
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .start_paused(true)
                .build()
                .expect("runtime")
                .block_on(self.run_in_current())
        })
        .join()
        .expect("harness thread panicked")
    }

    /// Runs on the current runtime, which must be a current-thread runtime with paused clock,
    /// e.g. `#[tokio::test(start_paused = true)]`.
    ///
    /// # Panics
    /// If the configuration is invalid or the engine task panics.
    pub async fn run_in_current(self) -> HarnessReport<A> {
        let capture = CapturingExecutor::<A>::new("botwright-harness-capture");
        let engine = self
            .builder
            .disable_signals()
            .executor(
                capture.clone(),
                ExecutorOpts::default().queue_capacity(65_536),
            )
            .build()
            .expect("invalid engine configuration");
        let handle = engine.handle();
        let started = Instant::now();
        let task = tokio::spawn(engine.run());
        let mut idle_ticks = 0;
        let completed = loop {
            tokio::time::sleep(self.tick).await;
            if task.is_finished() {
                break false;
            }
            idle_ticks = if handle.is_idle() { idle_ticks + 1 } else { 0 };
            if idle_ticks >= 2 {
                break true;
            }
            if started.elapsed() >= self.max_duration {
                break false;
            }
        };
        let elapsed = started.elapsed();
        handle.shutdown();
        let exit = task.await.expect("engine task panicked");
        HarnessReport {
            actions: capture.actions(),
            exit,
            completed,
            elapsed,
        }
    }
}

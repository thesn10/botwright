use std::borrow::Borrow;
use std::fmt;
use std::sync::Arc;

use tokio::time::Instant;

/// Name of an event source, equal to the name of the collector that produces it.
///
/// Cloning is cheap: the name is shared.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceId(Arc<str>);

impl SourceId {
    /// Creates a source id from a collector name.
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self(name.into())
    }

    /// The collector name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for SourceId {
    fn from(name: &str) -> Self {
        Self(name.into())
    }
}

impl From<String> for SourceId {
    fn from(name: String) -> Self {
        Self(name.into())
    }
}

impl From<&SourceId> for SourceId {
    fn from(id: &SourceId) -> Self {
        id.clone()
    }
}

impl Borrow<str> for SourceId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for SourceId {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}

impl PartialEq<&str> for SourceId {
    fn eq(&self, other: &&str) -> bool {
        &*self.0 == *other
    }
}

/// An event together with its delivery metadata.
pub struct Envelope<E> {
    /// Collector that produced the event.
    pub source: SourceId,
    /// Per-source sequence number, starting at 1 and counting every event the collector produced,
    /// including events filtered out or dropped for this subscriber.
    pub seq: u64,
    /// Time the collector task received the event.
    pub received_at: Instant,
    /// The event, shared between all subscribers.
    pub event: Arc<E>,
}

impl<E> Clone for Envelope<E> {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            seq: self.seq,
            received_at: self.received_at,
            event: Arc::clone(&self.event),
        }
    }
}

impl<E: fmt::Debug> fmt::Debug for Envelope<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Envelope")
            .field("source", &self.source)
            .field("seq", &self.seq)
            .field("received_at", &self.received_at)
            .field("event", &self.event)
            .finish()
    }
}

impl<E> std::ops::Deref for Envelope<E> {
    type Target = E;

    fn deref(&self) -> &E {
        &self.event
    }
}

/// One element of a collector stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item<E, C> {
    /// A domain event. `cursor` is the position to resume from after this event;
    /// `None` leaves the last known cursor unchanged.
    Event {
        /// The event.
        event: E,
        /// Position after this event.
        cursor: Option<C>,
    },
    /// The gap between two positions after a reconnect was backfilled completely.
    GapFilled {
        /// First position of the gap.
        from: u64,
        /// Last position of the gap.
        to: u64,
    },
    /// The gap between two positions could not be backfilled.
    GapUnfilled {
        /// First position of the gap.
        from: u64,
        /// Last position of the gap.
        to: u64,
    },
}

impl<E, C> Item<E, C> {
    /// An event with a resume cursor.
    pub fn new(event: E, cursor: C) -> Self {
        Self::Event {
            event,
            cursor: Some(cursor),
        }
    }

    /// An event without a cursor.
    pub fn event(event: E) -> Self {
        Self::Event {
            event,
            cursor: None,
        }
    }

    /// Converts the event, keeping cursor and gap notices.
    pub fn map_event<E2>(self, f: impl FnOnce(E) -> E2) -> Item<E2, C> {
        match self {
            Self::Event { event, cursor } => Item::Event {
                event: f(event),
                cursor,
            },
            Self::GapFilled { from, to } => Item::GapFilled { from, to },
            Self::GapUnfilled { from, to } => Item::GapUnfilled { from, to },
        }
    }
}

/// Engine notifications delivered to [`Strategy::on_system`](crate::Strategy::on_system).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SystemEvent {
    /// The collector stream is (again) established.
    CollectorConnected {
        /// Affected collector.
        source: SourceId,
    },
    /// The collector stream broke; a reconnect with backoff is running.
    CollectorDisconnected {
        /// Affected collector.
        source: SourceId,
        /// Why the stream ended.
        error: String,
    },
    /// A gap after a reconnect was backfilled completely.
    GapFilled {
        /// Affected collector.
        source: SourceId,
        /// First position of the gap.
        from: u64,
        /// Last position of the gap.
        to: u64,
    },
    /// A gap could not be backfilled; the strategy has to rebuild its state.
    /// `None` bounds mean the extent is unknown, e.g. after a strategy restart.
    GapUnfilled {
        /// Affected collector.
        source: SourceId,
        /// First position of the gap, if known.
        from: Option<u64>,
        /// Last position of the gap, if known.
        to: Option<u64>,
    },
    /// A channel to this strategy is filled above the lagging threshold.
    Lagging {
        /// Affected collector.
        source: SourceId,
        /// Number of queued events.
        depth: usize,
    },
    /// Events of this source were dropped for this strategy (`DropOldest` or `DropNewest`).
    Dropped {
        /// Affected collector.
        source: SourceId,
        /// Number of dropped events since the last notice.
        count: u64,
    },
    /// The strategy was rebuilt by its factory after a panic.
    Restarted {
        /// Panic message.
        reason: String,
    },
    /// The engine is shutting down; this is the last event the strategy receives.
    ShutdownRequested,
}

impl SystemEvent {
    /// The source this event refers to, if any.
    pub fn source(&self) -> Option<&SourceId> {
        match self {
            Self::CollectorConnected { source }
            | Self::CollectorDisconnected { source, .. }
            | Self::GapFilled { source, .. }
            | Self::GapUnfilled { source, .. }
            | Self::Lagging { source, .. }
            | Self::Dropped { source, .. } => Some(source),
            Self::Restarted { .. } | Self::ShutdownRequested => None,
        }
    }
}

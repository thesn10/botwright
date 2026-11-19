use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use tokio::sync::mpsc;

use crate::{SourceId, SystemEvent};

/// Out-of-band notices from collectors to one strategy; `wake` is a coalescing doorbell.
pub(crate) struct Signals {
    wake: mpsc::Sender<()>,
    pub(crate) slots: Vec<Slot>,
}

pub(crate) struct Slot {
    pub(crate) source: SourceId,
    pub(crate) dropped: AtomicU64,
    pub(crate) lagging: AtomicUsize,
}

impl Signals {
    pub(crate) fn new(sources: &[SourceId]) -> (Self, mpsc::Receiver<()>) {
        let (wake, rx) = mpsc::channel(1);
        let slots = sources
            .iter()
            .map(|source| Slot {
                source: source.clone(),
                dropped: AtomicU64::new(0),
                lagging: AtomicUsize::new(0),
            })
            .collect();
        (Self { wake, slots }, rx)
    }

    pub(crate) fn wake(&self) {
        let _ = self.wake.try_send(());
    }

    pub(crate) fn slot_of(&self, source: &SourceId) -> usize {
        self.slots
            .iter()
            .position(|s| &s.source == source)
            .expect("slot exists for every subscribed source")
    }

    pub(crate) fn take_dropped(&self, slot: usize) -> Option<SystemEvent> {
        let slot = &self.slots[slot];
        let count = slot.dropped.swap(0, Ordering::AcqRel);
        (count > 0).then(|| SystemEvent::Dropped {
            source: slot.source.clone(),
            count,
        })
    }

    pub(crate) fn take_lagging(&self, slot: usize) -> Option<SystemEvent> {
        let slot = &self.slots[slot];
        let depth = slot.lagging.swap(0, Ordering::AcqRel);
        (depth > 0).then(|| SystemEvent::Lagging {
            source: slot.source.clone(),
            depth,
        })
    }

    pub(crate) fn pending(&self) -> bool {
        self.slots
            .iter()
            .any(|s| s.dropped.load(Ordering::Acquire) > 0 || s.lagging.load(Ordering::Acquire) > 0)
    }
}

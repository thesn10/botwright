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

    pub(crate) fn take(&self) -> impl Iterator<Item = SystemEvent> + '_ {
        self.slots.iter().flat_map(|slot| {
            let dropped = slot.dropped.swap(0, Ordering::AcqRel);
            let lagging = slot.lagging.swap(0, Ordering::AcqRel);
            let source = &slot.source;
            [
                (dropped > 0).then(|| SystemEvent::Dropped {
                    source: source.clone(),
                    count: dropped,
                }),
                (lagging > 0).then(|| SystemEvent::Lagging {
                    source: source.clone(),
                    depth: lagging,
                }),
            ]
            .into_iter()
            .flatten()
        })
    }

    pub(crate) fn pending(&self) -> bool {
        self.slots
            .iter()
            .any(|s| s.dropped.load(Ordering::Acquire) > 0 || s.lagging.load(Ordering::Acquire) > 0)
    }
}

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

use super::Shared;
use crate::SubmitError;

pub(crate) type HandlesFn<A> = Box<dyn Fn(&A) -> bool + Send + Sync>;

pub(crate) struct Route<A> {
    pub(crate) name: Arc<str>,
    pub(crate) index: usize,
    pub(crate) handles: HandlesFn<A>,
    pub(crate) tx: mpsc::Sender<Arc<A>>,
}

pub(crate) struct Router<A> {
    pub(crate) routes: Vec<Route<A>>,
    pub(crate) shared: Arc<Shared>,
}

impl<A: Send + Sync + 'static> Router<A> {
    pub(crate) async fn submit(&self, action: Arc<A>) -> Result<(), SubmitError> {
        let mut routed = false;
        for route in &self.routes {
            if !(route.handles)(&action) {
                continue;
            }
            routed = true;
            let pending = &self.shared.executors[route.index].pending;
            pending.fetch_add(1, Ordering::AcqRel);
            if route.tx.send(Arc::clone(&action)).await.is_err() {
                pending.fetch_sub(1, Ordering::AcqRel);
                return Err(SubmitError::Closed(route.name.to_string()));
            }
        }
        if routed {
            Ok(())
        } else {
            Err(SubmitError::Unrouted)
        }
    }

    pub(crate) fn try_submit(&self, action: Arc<A>) -> Result<(), SubmitError> {
        let mut permits = Vec::new();
        for route in &self.routes {
            if !(route.handles)(&action) {
                continue;
            }
            match route.tx.try_reserve() {
                Ok(permit) => permits.push((route.index, permit)),
                Err(mpsc::error::TrySendError::Full(())) => {
                    return Err(SubmitError::Full(route.name.to_string()));
                }
                Err(mpsc::error::TrySendError::Closed(())) => {
                    return Err(SubmitError::Closed(route.name.to_string()));
                }
            }
        }
        if permits.is_empty() {
            return Err(SubmitError::Unrouted);
        }
        for (index, permit) in permits {
            self.shared.executors[index]
                .pending
                .fetch_add(1, Ordering::AcqRel);
            permit.send(Arc::clone(&action));
        }
        Ok(())
    }
}

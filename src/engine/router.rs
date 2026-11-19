use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::{StreamExt, TryStreamExt, stream};
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
    fn routes_for<'a>(&'a self, action: &'a A) -> impl Iterator<Item = &'a Route<A>> {
        self.routes
            .iter()
            .filter(move |route| (route.handles)(action))
    }

    pub(crate) async fn submit(&self, action: Arc<A>) -> Result<(), SubmitError> {
        let routed = stream::iter(self.routes_for(&action))
            .map(Ok)
            .try_fold(false, |_, route| async {
                let pending = &self.shared.executors[route.index].pending;
                pending.fetch_add(1, Ordering::AcqRel);
                route.tx.send(Arc::clone(&action)).await.map_err(|_| {
                    pending.fetch_sub(1, Ordering::AcqRel);
                    SubmitError::Closed(route.name.to_string())
                })?;
                Ok(true)
            })
            .await?;
        routed.then_some(()).ok_or(SubmitError::Unrouted)
    }

    pub(crate) fn try_submit(&self, action: Arc<A>) -> Result<(), SubmitError> {
        let permits = self
            .routes_for(&action)
            .map(|route| match route.tx.try_reserve() {
                Ok(permit) => Ok((route.index, permit)),
                Err(mpsc::error::TrySendError::Full(())) => {
                    Err(SubmitError::Full(route.name.to_string()))
                }
                Err(mpsc::error::TrySendError::Closed(())) => {
                    Err(SubmitError::Closed(route.name.to_string()))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        if permits.is_empty() {
            return Err(SubmitError::Unrouted);
        }
        permits.into_iter().for_each(|(index, permit)| {
            self.shared.executors[index]
                .pending
                .fetch_add(1, Ordering::AcqRel);
            permit.send(Arc::clone(&action));
        });
        Ok(())
    }
}

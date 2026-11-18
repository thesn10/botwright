use std::marker::PhantomData;
use std::sync::Arc;

use futures::{TryStream, TryStreamExt};

use crate::{Collector, CollectorError, Item};

/// Converts the events of another collector, keeping cursors and gap notices.
pub struct MapCollector<C, F, E1> {
    inner: C,
    map: Arc<F>,
    _event: PhantomData<fn(E1)>,
}

impl<C, F, E1> MapCollector<C, F, E1> {
    /// Wraps `inner`; the name stays the same.
    pub fn new(inner: C, map: F) -> Self {
        Self {
            inner,
            map: Arc::new(map),
            _event: PhantomData,
        }
    }
}

impl<C, F, E1, E2> Collector<E2> for MapCollector<C, F, E1>
where
    C: Collector<E1>,
    F: Fn(E1) -> E2 + Send + Sync + 'static,
    E1: Send + 'static,
    E2: Send + Sync + 'static,
{
    type Cursor = C::Cursor;

    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn connect(
        &self,
        resume: Option<C::Cursor>,
    ) -> Result<
        impl TryStream<Ok = Item<E2, C::Cursor>, Error = CollectorError> + Send + 'static,
        CollectorError,
    > {
        let stream = self.inner.connect(resume).await?;
        let map = Arc::clone(&self.map);
        Ok(stream.map_ok(move |item| item.map_event(|e| map(e))))
    }

    fn is_exhausted(&self) -> bool {
        self.inner.is_exhausted()
    }
}

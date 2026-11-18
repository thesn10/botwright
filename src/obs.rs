#[cfg(feature = "metrics")]
pub(crate) use metrics::{Counter, Gauge, Histogram};

#[cfg(not(feature = "metrics"))]
pub(crate) use noop::{Counter, Gauge, Histogram};

#[cfg(not(feature = "metrics"))]
mod noop {
    #[derive(Clone, Default)]
    pub(crate) struct Counter;

    impl Counter {
        pub(crate) fn increment(&self, _: u64) {}
    }

    #[derive(Clone, Default)]
    pub(crate) struct Gauge;

    impl Gauge {
        pub(crate) fn set(&self, _: f64) {}
        pub(crate) fn increment(&self, _: f64) {}
        pub(crate) fn decrement(&self, _: f64) {}
    }

    #[derive(Clone, Default)]
    pub(crate) struct Histogram;

    impl Histogram {
        pub(crate) fn record(&self, _: f64) {}
    }
}

#[cfg(feature = "metrics")]
fn labels(labels: &[(&'static str, &str)]) -> Vec<metrics::Label> {
    labels
        .iter()
        .map(|(k, v)| metrics::Label::new(*k, (*v).to_owned()))
        .collect()
}

#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
pub(crate) fn counter(name: &'static str, l: &[(&'static str, &str)]) -> Counter {
    #[cfg(feature = "metrics")]
    return metrics::counter!(name, labels(l));
    #[cfg(not(feature = "metrics"))]
    Counter
}

#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
pub(crate) fn gauge(name: &'static str, l: &[(&'static str, &str)]) -> Gauge {
    #[cfg(feature = "metrics")]
    return metrics::gauge!(name, labels(l));
    #[cfg(not(feature = "metrics"))]
    Gauge
}

#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
pub(crate) fn histogram(name: &'static str, l: &[(&'static str, &str)]) -> Histogram {
    #[cfg(feature = "metrics")]
    return metrics::histogram!(name, labels(l));
    #[cfg(not(feature = "metrics"))]
    Histogram
}

pub(crate) const EVENTS: &str = "botwright_events_total";
pub(crate) const CHANNEL_DEPTH: &str = "botwright_channel_depth";
pub(crate) const DISPATCH_WAIT: &str = "botwright_dispatch_wait_seconds";
pub(crate) const EVENT_HANDLE: &str = "botwright_event_handle_seconds";
pub(crate) const TASKS_ACTIVE: &str = "botwright_tasks_active";
pub(crate) const ACTIONS: &str = "botwright_actions_total";
pub(crate) const EXECUTE: &str = "botwright_execute_seconds";
pub(crate) const DROPPED: &str = "botwright_dropped_total";
pub(crate) const RESTARTS: &str = "botwright_strategy_restarts_total";
pub(crate) const RECONNECTS: &str = "botwright_reconnects_total";
pub(crate) const GAPS: &str = "botwright_gaps_total";
#[cfg_attr(
    not(any(feature = "evm-alloy1", feature = "evm-alloy2")),
    allow(dead_code)
)]
pub(crate) const REORGS: &str = "botwright_reorgs_total";
pub(crate) const NOTIFICATIONS_DROPPED: &str = "botwright_notifications_dropped_total";

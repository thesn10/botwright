#![cfg(feature = "metrics")]

mod common;

use std::time::Duration;

use botwright::test_util::{Harness, ReplayCollector};
use botwright::{CollectorOpts, Delivery, Engine};
use common::{Log, Recorder};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};

type Entry = (String, Vec<(String, String)>, DebugValue);

fn find<'a>(snapshot: &'a [Entry], name: &str, labels: &[(&str, &str)]) -> Vec<&'a DebugValue> {
    snapshot
        .iter()
        .filter(|(n, l, _)| {
            n == name
                && labels
                    .iter()
                    .all(|(k, v)| l.iter().any(|(lk, lv)| lk == k && lv == v))
        })
        .map(|(_, _, v)| v)
        .collect()
}

#[test]
fn backpressure_and_drops_show_up_in_metrics() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().unwrap();

    let slow = Log::default();
    let dropping = Log::default();
    let report = Harness::new(
        Engine::<u64, u64>::builder()
            .collector(
                ReplayCollector::from_events("src", 1..=100u64),
                CollectorOpts::default(),
            )
            .strategy(
                Recorder::new("slow", &slow).delay(Duration::from_millis(10)),
                |s| s.from("src", Delivery::Lossless { capacity: 10 }),
            )
            .strategy(
                Recorder::new("dropping", &dropping).delay(Duration::from_millis(10)),
                |s| s.from("src", Delivery::DropNewest { capacity: 5 }),
            ),
    )
    .run();
    assert!(report.completed);

    let snapshot: Vec<_> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(key, _, _, value)| {
            let key = key.key();
            let labels = key
                .labels()
                .map(|l| (l.key().to_owned(), l.value().to_owned()))
                .collect();
            (key.name().to_owned(), labels, value)
        })
        .collect();

    assert_eq!(
        find(&snapshot, "botwright_events_total", &[("source", "src")]),
        [&DebugValue::Counter(100)]
    );
    let waits = find(
        &snapshot,
        "botwright_dispatch_wait_seconds",
        &[("source", "src"), ("strategy", "slow")],
    );
    assert!(matches!(&waits[..], [DebugValue::Histogram(samples)] if !samples.is_empty()));
    let depth = find(
        &snapshot,
        "botwright_channel_depth",
        &[("source", "src"), ("strategy", "slow")],
    );
    assert_eq!(depth.len(), 1);
    let dropped = find(
        &snapshot,
        "botwright_dropped_total",
        &[
            ("source", "src"),
            ("strategy", "dropping"),
            ("delivery", "drop_newest"),
        ],
    );
    assert_eq!(dropped, [&DebugValue::Counter(dropping.dropped_total())]);
    assert!(dropping.dropped_total() > 0);
    let handled = find(
        &snapshot,
        "botwright_event_handle_seconds",
        &[("strategy", "slow")],
    );
    assert!(matches!(&handled[..], [DebugValue::Histogram(samples)] if samples.len() == 100));
}

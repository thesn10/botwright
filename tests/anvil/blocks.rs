use botwright::eth::{BlockCollector, ChainEvent, Provider};
use botwright::{Delivery, Engine, SystemEvent};

use crate::harness::{Ev, Proxy, Record, Recorder, anvil, opts, spawn, stop, until};

#[tokio::test(flavor = "multi_thread")]
async fn block_gap_is_backfilled_after_reconnect() {
    let Some(anvil) = anvil().await else { return };
    let provider = anvil.provider().await;
    let proxy = Proxy::start(anvil.port).await;
    let mine = |n: u64| {
        let provider = provider.clone();
        async move {
            let _: serde_json::Value = provider
                .raw_request("anvil_mine".into(), (n,))
                .await
                .unwrap();
        }
    };

    let record = Record::default();
    let engine = Engine::<Ev, ()>::builder()
        .collector(BlockCollector::ws("blocks", proxy.ws()), opts())
        .strategy(Recorder(record.clone()), |s| {
            s.from("blocks", Delivery::Lossless { capacity: 64 })
        })
        .build()
        .unwrap();
    let (handle, run) = spawn(engine);
    let numbers = || -> Vec<u64> {
        record
            .chain()
            .into_iter()
            .filter_map(|c| match c {
                ChainEvent::Block(header) => Some(header.number),
                ChainEvent::Reorg { .. } => None,
            })
            .collect()
    };

    until("connect", || record.connects("blocks") == 1).await;
    mine(2).await;
    until("live blocks", || numbers().len() == 2).await;
    proxy.cut();
    mine(5).await;
    proxy.restore();
    until("reconnect", || record.connects("blocks") == 2).await;
    mine(1).await;
    until("all blocks", || numbers().len() >= 8).await;

    assert_eq!(numbers(), (1..=8).collect::<Vec<_>>());
    assert!(
        record
            .systems()
            .iter()
            .any(|e| matches!(e, SystemEvent::GapFilled { from: 3, to: 7, .. }))
    );
    assert!(record.reorgs().is_empty());
    stop(handle, run).await;
}

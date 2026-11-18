use std::time::Duration;

use botwright::eth::{BlockCollector, BlockCursor, Filter, LogCollector, Provider};
use botwright::{Delivery, Engine, SystemEvent};

use crate::harness::{
    Ev, Proxy, Record, Recorder, anvil, deploy_emitter, emit, opts, spawn, stop, until,
};

#[tokio::test(flavor = "multi_thread")]
async fn log_gap_is_backfilled_exactly_once() {
    let Some(anvil) = anvil().await else { return };
    let provider = anvil.provider().await;
    let (from, emitter) = deploy_emitter(&provider).await;
    let proxy = Proxy::start(anvil.port).await;

    let record = Record::default();
    let logs = LogCollector::ws("logs", proxy.ws(), Filter::new().address(emitter));
    let engine = Engine::<Ev, ()>::builder()
        .collector(logs, opts())
        .strategy(Recorder(record.clone()), |s| {
            s.from("logs", Delivery::Lossless { capacity: 64 })
        })
        .build()
        .unwrap();
    let (handle, run) = spawn(engine);

    until("connect", || record.connects("logs") == 1).await;
    for v in 1..=3 {
        emit(&provider, from, emitter, v).await;
    }
    until("live logs", || record.added().len() == 3).await;

    proxy.cut();
    until("disconnect", || {
        record
            .systems()
            .iter()
            .any(|e| matches!(e, SystemEvent::CollectorDisconnected { .. }))
    })
    .await;
    for v in 4..=6 {
        emit(&provider, from, emitter, v).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    proxy.restore();
    until("reconnect", || record.connects("logs") == 2).await;
    for v in 7..=9 {
        emit(&provider, from, emitter, v).await;
    }
    until("all logs", || record.added().len() >= 9).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(record.added(), (1..=9).collect::<Vec<_>>());
    assert!(
        record
            .systems()
            .iter()
            .any(|e| matches!(e, SystemEvent::GapFilled { .. })),
        "{:?}",
        record.systems()
    );
    stop(handle, run).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reorg_reports_common_ancestor_and_removed_logs() {
    let Some(anvil) = anvil().await else { return };
    let provider = anvil.provider().await;
    let (from, emitter) = deploy_emitter(&provider).await;
    let ws = format!("ws://127.0.0.1:{}", anvil.port);

    let record = Record::default();
    let engine = Engine::<Ev, ()>::builder()
        .collector(BlockCollector::ws("blocks", ws.clone()), opts())
        .collector(
            LogCollector::ws("logs", ws, Filter::new().address(emitter)),
            opts(),
        )
        .strategy(Recorder(record.clone()), |s| {
            s.from("blocks", Delivery::Lossless { capacity: 64 })
                .from("logs", Delivery::Lossless { capacity: 64 })
        })
        .build()
        .unwrap();
    let (handle, run) = spawn(engine);
    until("connect", || {
        record.connects("blocks") == 1 && record.connects("logs") == 1
    })
    .await;

    for v in 1..=4 {
        emit(&provider, from, emitter, v).await;
    }
    until("logs", || record.added().len() == 4).await;
    let head = provider.get_block_number().await.unwrap();
    let ancestor = provider
        .get_block_by_number((head - 2).into())
        .await
        .unwrap()
        .unwrap()
        .header
        .hash;

    let _: serde_json::Value = provider
        .raw_request(
            "anvil_reorg".into(),
            (2u64, Vec::<serde_json::Value>::new()),
        )
        .await
        .unwrap();
    let other = provider.get_accounts().await.unwrap()[1];
    emit(&provider, other, emitter, 5).await;

    until("reorg", || !record.reorgs().is_empty()).await;
    let (common, dropped) = record.reorgs().remove(0);
    assert_eq!(
        common,
        BlockCursor {
            number: head - 2,
            hash: ancestor
        }
    );
    assert_eq!(
        dropped.iter().map(|c| c.number).collect::<Vec<_>>(),
        vec![head - 1, head]
    );

    until("removed logs", || {
        record.logs().iter().filter(|(added, _)| !added).count() == 2
    })
    .await;
    let removed: Vec<u64> = record
        .logs()
        .into_iter()
        .filter(|(added, _)| !added)
        .map(|(_, v)| v)
        .collect();
    assert_eq!(removed, vec![4, 3]);
    until("log after reorg", || record.added().last() == Some(&5)).await;
    stop(handle, run).await;
}

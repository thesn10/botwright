#![cfg(any(feature = "evm-alloy1", feature = "evm-alloy2"))]

use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use botwright::eth::{
    Address, B256, BlockCollector, BlockCursor, Bytes, ChainEvent, DynProvider, Filter, Header,
    LogCollector, LogEvent, Provider, ProviderBuilder, TransactionRequest, U256,
};
use botwright::{
    Backoff, BoxError, CollectorOpts, Delivery, Engine, EngineHandle, Envelope, ExitReason,
    Strategy, StrategyCtx, SystemEvent,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::watch;
use tokio::task::JoinHandle;

const EMITTER_INIT: &str = "600e600c600039600e6000f3600035600052600160206000a100";

struct Anvil {
    _child: Child,
    port: u16,
}

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn anvil() -> Option<Anvil> {
    if std::env::var_os("RUST_LOG").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
    }
    let bin = std::env::var("ANVIL").unwrap_or_else(|_| "anvil".into());
    let port = free_port().await;
    let child = Command::new(&bin)
        .args(["--port", &port.to_string(), "--silent"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn();
    let child = match child {
        Ok(child) => child,
        Err(err) if std::env::var_os("BOTWRIGHT_REQUIRE_ANVIL").is_some() => {
            panic!("cannot start `{bin}`: {err}")
        }
        Err(err) => {
            eprintln!("skipping: cannot start `{bin}`: {err}");
            return None;
        }
    };
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return Some(Anvil {
                _child: child,
                port,
            });
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("anvil did not start");
}

impl Anvil {
    fn http(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    async fn provider(&self) -> DynProvider {
        let provider = ProviderBuilder::new().connect_http(self.http().parse().unwrap());
        DynProvider::new(provider)
    }
}

/// TCP proxy whose connections can be cut to simulate an outage.
struct Proxy {
    addr: SocketAddr,
    up: Arc<AtomicBool>,
    generation: watch::Sender<u64>,
    _task: JoinHandle<()>,
}

impl Proxy {
    async fn start(target: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let up = Arc::new(AtomicBool::new(true));
        let (generation, _) = watch::channel(0u64);
        let task = {
            let up = Arc::clone(&up);
            let generation = generation.clone();
            tokio::spawn(async move {
                loop {
                    let (mut client, _) = listener.accept().await.unwrap();
                    if !up.load(Ordering::SeqCst) {
                        continue;
                    }
                    let mut cut = generation.subscribe();
                    tokio::spawn(async move {
                        let Ok(mut server) = TcpStream::connect(("127.0.0.1", target)).await else {
                            return;
                        };
                        tokio::select! {
                            _ = tokio::io::copy_bidirectional(&mut client, &mut server) => {}
                            _ = cut.changed() => {}
                        }
                    });
                }
            })
        };
        Self {
            addr,
            up,
            generation,
            _task: task,
        }
    }

    fn ws(&self) -> String {
        format!("ws://{}", self.addr)
    }

    fn cut(&self) {
        self.up.store(false, Ordering::SeqCst);
        self.generation.send_modify(|g| *g += 1);
    }

    fn restore(&self) {
        self.up.store(true, Ordering::SeqCst);
    }
}

async fn deploy_emitter(provider: &DynProvider) -> (Address, Address) {
    let from = provider.get_accounts().await.unwrap()[0];
    let tx = TransactionRequest::default()
        .from(from)
        .input(Bytes::from(hex(EMITTER_INIT)).into());
    let receipt = provider
        .send_transaction(tx)
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();
    (from, receipt.contract_address.expect("deployed"))
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

async fn emit(provider: &DynProvider, from: Address, emitter: Address, value: u64) {
    let data = B256::from(U256::from(value));
    let tx = TransactionRequest::default()
        .from(from)
        .to(emitter)
        .input(Bytes::from(data.0.to_vec()).into());
    provider
        .send_transaction(tx)
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();
}

#[derive(Debug, Clone)]
enum Ev {
    Log(Box<LogEvent>),
    Chain(Box<ChainEvent<Header>>),
}

impl From<LogEvent> for Ev {
    fn from(e: LogEvent) -> Self {
        Self::Log(Box::new(e))
    }
}

impl From<ChainEvent<Header>> for Ev {
    fn from(e: ChainEvent<Header>) -> Self {
        Self::Chain(Box::new(e))
    }
}

#[derive(Debug, Clone)]
enum Seen {
    Event(Ev),
    System(SystemEvent),
}

#[derive(Clone, Default)]
struct Record(Arc<Mutex<Vec<Seen>>>);

impl Record {
    fn all(&self) -> Vec<Seen> {
        self.0.lock().unwrap().clone()
    }

    fn events(&self) -> Vec<Ev> {
        self.all()
            .into_iter()
            .filter_map(|s| match s {
                Seen::Event(e) => Some(e),
                Seen::System(_) => None,
            })
            .collect()
    }

    fn chain(&self) -> Vec<ChainEvent<Header>> {
        self.events()
            .into_iter()
            .filter_map(|e| match e {
                Ev::Chain(c) => Some(*c),
                Ev::Log(_) => None,
            })
            .collect()
    }

    fn logs(&self) -> Vec<(bool, u64)> {
        self.events()
            .into_iter()
            .filter_map(|e| match e {
                Ev::Log(log) => Some(match *log {
                    LogEvent::Added(log) => (true, value_of(&log.inner.data.data)),
                    LogEvent::Removed(log) => (false, value_of(&log.inner.data.data)),
                }),
                Ev::Chain(_) => None,
            })
            .collect()
    }

    fn added(&self) -> Vec<u64> {
        self.logs()
            .into_iter()
            .filter(|(added, _)| *added)
            .map(|(_, v)| v)
            .collect()
    }

    fn reorgs(&self) -> Vec<(BlockCursor, Vec<BlockCursor>)> {
        self.chain()
            .into_iter()
            .filter_map(|c| match c {
                ChainEvent::Reorg {
                    common_ancestor,
                    dropped,
                } => Some((common_ancestor, dropped)),
                ChainEvent::Block(_) => None,
            })
            .collect()
    }

    fn connects(&self, source: &str) -> usize {
        self.all()
            .iter()
            .filter(|s| matches!(s, Seen::System(SystemEvent::CollectorConnected { source: s }) if s == source))
            .count()
    }

    fn systems(&self) -> Vec<SystemEvent> {
        self.all()
            .into_iter()
            .filter_map(|s| match s {
                Seen::System(e) => Some(e),
                _ => None,
            })
            .collect()
    }
}

fn value_of(data: &[u8]) -> u64 {
    U256::from_be_slice(data).to::<u64>()
}

struct Recorder(Record);

impl Strategy<Ev, ()> for Recorder {
    fn name(&self) -> &str {
        "recorder"
    }

    async fn on_event(
        &mut self,
        event: &Envelope<Ev>,
        _ctx: &mut StrategyCtx<()>,
    ) -> Result<(), BoxError> {
        self.0
            .0
            .lock()
            .unwrap()
            .push(Seen::Event((*event.event).clone()));
        Ok(())
    }

    async fn on_system(
        &mut self,
        event: &SystemEvent,
        _ctx: &mut StrategyCtx<()>,
    ) -> Result<(), BoxError> {
        self.0.0.lock().unwrap().push(Seen::System(event.clone()));
        Ok(())
    }
}

async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..400 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

fn opts() -> CollectorOpts {
    CollectorOpts::default().backoff(Backoff::constant(Duration::from_millis(100)))
}

fn spawn(engine: Engine<Ev, ()>) -> (EngineHandle, JoinHandle<ExitReason>) {
    let handle = engine.handle();
    (handle, tokio::spawn(engine.run()))
}

async fn stop(handle: EngineHandle, run: JoinHandle<ExitReason>) {
    handle.shutdown();
    let exit = run.await.unwrap();
    assert!(exit.is_shutdown(), "{exit}");
}

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

mod tx {
    use botwright::eth::{PrivateKeySigner, TransactionAction, TransactionExecutor, TxFeedback};
    use botwright::{ChannelCollector, ExecutorOpts};

    use super::*;

    const DEV_KEY: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    #[derive(Debug, Clone)]
    enum Event {
        Go(u64),
        Feedback(TxFeedback),
    }

    impl From<TxFeedback> for Event {
        fn from(f: TxFeedback) -> Self {
            Self::Feedback(f)
        }
    }

    struct Pay(Address);

    impl TransactionAction for Pay {
        fn transaction(&self) -> Option<TransactionRequest> {
            Some(
                TransactionRequest::default()
                    .to(self.0)
                    .value(U256::from(1)),
            )
        }
    }

    struct Payer {
        to: Address,
        seen: Arc<Mutex<Vec<TxFeedback>>>,
    }

    impl Strategy<Event, Pay> for Payer {
        fn name(&self) -> &str {
            "payer"
        }

        async fn on_event(
            &mut self,
            event: &Envelope<Event>,
            ctx: &mut StrategyCtx<Pay>,
        ) -> Result<(), BoxError> {
            match &*event.event {
                Event::Go(n) => {
                    for _ in 0..*n {
                        ctx.submit(Pay(self.to)).await?;
                    }
                }
                Event::Feedback(f) => self.seen.lock().unwrap().push(f.clone()),
            }
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transactions_are_sent_with_sequential_nonces_and_reported() {
        let Some(anvil) = anvil().await else { return };
        let provider = anvil.provider().await;
        let to = Address::repeat_byte(0x42);
        let signer: PrivateKeySigner = DEV_KEY.parse().unwrap();

        let (feed, sender) = ChannelCollector::<Event>::new("feed", 64);
        let executor = TransactionExecutor::new("tx", signer, &anvil.http())
            .unwrap()
            .feedback(sender.clone())
            .receipt_timeout(Duration::from_secs(10), Duration::from_millis(50));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let engine = Engine::<Event, Pay>::builder()
            .collector(feed, CollectorOpts::default())
            .strategy(
                Payer {
                    to,
                    seen: Arc::clone(&seen),
                },
                |s| s.from("feed", Delivery::default()),
            )
            .executor(executor, ExecutorOpts::default())
            .build()
            .unwrap();
        let handle = engine.handle();
        let run = tokio::spawn(engine.run());

        sender.send(Event::Go(3)).await.unwrap();
        let included = || {
            seen.lock()
                .unwrap()
                .iter()
                .filter_map(|f| match f {
                    TxFeedback::Included { receipt, .. } => Some(receipt.status()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        until("receipts", || included().len() == 3).await;

        assert_eq!(included(), vec![true; 3]);
        let sent = seen
            .lock()
            .unwrap()
            .iter()
            .filter(|f| matches!(f, TxFeedback::Sent { .. }))
            .count();
        assert_eq!(sent, 3);
        assert_eq!(provider.get_balance(to).await.unwrap(), U256::from(3));
        handle.shutdown();
        assert!(run.await.unwrap().is_shutdown());
    }
}

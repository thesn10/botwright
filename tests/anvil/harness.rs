use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{StreamExt, stream};

use botwright::eth::{
    Address, B256, BlockCursor, Bytes, ChainEvent, DynProvider, Header, LogEvent, Provider,
    ProviderBuilder, TransactionRequest, U256,
};
use botwright::{
    Backoff, BoxError, CollectorOpts, Engine, EngineHandle, Envelope, ExitReason, Strategy,
    StrategyCtx, SystemEvent,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::watch;
use tokio::task::JoinHandle;

const EMITTER_INIT: &str = "600e600c600039600e6000f3600035600052600160206000a100";

pub struct Anvil {
    _child: Child,
    pub port: u16,
}

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub async fn anvil() -> Option<Anvil> {
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
    let up = stream::iter(0..100)
        .any(|_| async move {
            let up = TcpStream::connect(("127.0.0.1", port)).await.is_ok();
            if !up {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            up
        })
        .await;
    assert!(up, "anvil did not start");
    Some(Anvil {
        _child: child,
        port,
    })
}

impl Anvil {
    pub fn http(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub async fn provider(&self) -> DynProvider {
        let provider = ProviderBuilder::new().connect_http(self.http().parse().unwrap());
        DynProvider::new(provider)
    }
}

/// TCP proxy whose connections can be cut to simulate an outage.
pub struct Proxy {
    addr: SocketAddr,
    up: Arc<AtomicBool>,
    generation: watch::Sender<u64>,
    _task: JoinHandle<()>,
}

impl Proxy {
    pub async fn start(target: u16) -> Self {
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

    pub fn ws(&self) -> String {
        format!("ws://{}", self.addr)
    }

    pub fn cut(&self) {
        self.up.store(false, Ordering::SeqCst);
        self.generation.send_modify(|g| *g += 1);
    }

    pub fn restore(&self) {
        self.up.store(true, Ordering::SeqCst);
    }
}

pub async fn deploy_emitter(provider: &DynProvider) -> (Address, Address) {
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

pub async fn emit(provider: &DynProvider, from: Address, emitter: Address, value: u64) {
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
pub enum Ev {
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
pub struct Record(Arc<Mutex<Vec<Seen>>>);

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

    pub fn chain(&self) -> Vec<ChainEvent<Header>> {
        self.events()
            .into_iter()
            .filter_map(|e| match e {
                Ev::Chain(c) => Some(*c),
                Ev::Log(_) => None,
            })
            .collect()
    }

    pub fn logs(&self) -> Vec<(bool, u64)> {
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

    pub fn added(&self) -> Vec<u64> {
        self.logs()
            .into_iter()
            .filter(|(added, _)| *added)
            .map(|(_, v)| v)
            .collect()
    }

    pub fn reorgs(&self) -> Vec<(BlockCursor, Vec<BlockCursor>)> {
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

    pub fn connects(&self, source: &str) -> usize {
        self.all()
            .iter()
            .filter(|s| matches!(s, Seen::System(SystemEvent::CollectorConnected { source: s }) if s == source))
            .count()
    }

    pub fn systems(&self) -> Vec<SystemEvent> {
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

pub struct Recorder(pub Record);

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

pub async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    let met = stream::iter(0..400)
        .any(|_| {
            let met = condition();
            async move {
                if !met {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                met
            }
        })
        .await;
    assert!(met, "timed out waiting for {what}");
}

pub fn opts() -> CollectorOpts {
    CollectorOpts::default().backoff(Backoff::constant(Duration::from_millis(100)))
}

pub fn spawn(engine: Engine<Ev, ()>) -> (EngineHandle, JoinHandle<ExitReason>) {
    let handle = engine.handle();
    (handle, tokio::spawn(engine.run()))
}

pub async fn stop(handle: EngineHandle, run: JoinHandle<ExitReason>) {
    handle.shutdown();
    let exit = run.await.unwrap();
    assert!(exit.is_shutdown(), "{exit}");
}

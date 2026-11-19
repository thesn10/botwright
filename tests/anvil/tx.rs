use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{StreamExt, TryStreamExt, stream};

use botwright::eth::{
    Address, PrivateKeySigner, Provider, TransactionAction, TransactionExecutor,
    TransactionRequest, TxFeedback, U256,
};
use botwright::{
    BoxError, ChannelCollector, CollectorOpts, Delivery, Engine, Envelope, ExecutorOpts, Strategy,
    StrategyCtx,
};

use crate::harness::{anvil, until};

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
                let (ctx, to) = (&*ctx, self.to);
                stream::iter(0..*n)
                    .map(Ok)
                    .try_for_each(|_| ctx.submit(Pay(to)))
                    .await?;
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

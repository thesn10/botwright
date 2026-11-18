//! `/healthz` and `/metrics` over HTTP.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::EngineHandle;

/// Configuration of the HTTP endpoints, passed to [`EngineBuilder::http`](crate::EngineBuilder::http).
#[derive(Clone)]
pub struct HttpOpts {
    addr: SocketAddr,
    max_event_age: Duration,
    exempt: HashSet<String>,
    prometheus: Option<PrometheusHandle>,
}

impl std::fmt::Debug for HttpOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpOpts")
            .field("addr", &self.addr)
            .field("max_event_age", &self.max_event_age)
            .field("exempt", &self.exempt)
            .finish_non_exhaustive()
    }
}

impl HttpOpts {
    /// Listens on `addr`; events may be at most 60 s old.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            max_event_age: Duration::from_secs(60),
            exempt: HashSet::new(),
            prometheus: None,
        }
    }

    /// `/healthz` fails when a collector's last event is older than `age`.
    pub fn max_event_age(mut self, age: Duration) -> Self {
        self.max_event_age = age;
        self
    }

    /// Skips the event age check for a collector with rare events; it must still be connected.
    pub fn exempt(mut self, collector: impl Into<String>) -> Self {
        self.exempt.insert(collector.into());
        self
    }

    /// Renders `/metrics` from this handle. Without one, a Prometheus recorder is installed as the
    /// global `metrics` recorder when the server starts.
    pub fn prometheus(mut self, handle: PrometheusHandle) -> Self {
        self.prometheus = Some(handle);
        self
    }
}

struct AppState {
    engine: EngineHandle,
    max_event_age: Duration,
    exempt: HashSet<String>,
    prometheus: Option<PrometheusHandle>,
}

pub(crate) async fn serve(opts: HttpOpts, engine: EngineHandle) {
    let prometheus = opts.prometheus.or_else(|| match PrometheusBuilder::new().install_recorder() {
        Ok(handle) => {
            let upkeep = handle.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));
                loop {
                    interval.tick().await;
                    upkeep.run_upkeep();
                }
            });
            Some(handle)
        }
        Err(err) => {
            tracing::warn!(error = %err, "could not install the Prometheus recorder, /metrics disabled");
            None
        }
    });
    let state = Arc::new(AppState {
        engine,
        max_event_age: opts.max_event_age,
        exempt: opts.exempt,
        prometheus,
    });
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .with_state(state);
    let listener = match tokio::net::TcpListener::bind(opts.addr).await {
        Ok(listener) => listener,
        Err(err) => {
            tracing::error!(error = %err, addr = %opts.addr, "binding the HTTP server failed");
            return;
        }
    };
    tracing::info!(addr = %opts.addr, "HTTP server listening");
    if let Err(err) = axum::serve(listener, app).await {
        tracing::error!(error = %err, "HTTP server failed");
    }
}

async fn healthz(State(state): State<Arc<AppState>>) -> (StatusCode, String) {
    let uptime = state.engine.uptime();
    let mut healthy = !state.engine.is_shutting_down();
    let mut body = String::new();
    for c in state.engine.health() {
        let fresh = state.exempt.contains(&c.name)
            || match c.last_event_age {
                Some(age) => age <= state.max_event_age,
                None => uptime <= state.max_event_age,
            };
        let ok = c.finished || (c.connected && fresh);
        healthy &= ok;
        let age = c
            .last_event_age
            .map_or_else(|| "none".to_owned(), |a| format!("{}ms", a.as_millis()));
        let _ = writeln!(
            body,
            "{} {} connected={} finished={} last_event_age={age}",
            if ok { "ok" } else { "FAIL" },
            c.name,
            c.connected,
            c.finished,
        );
    }
    let status = if healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, body)
}

async fn metrics(State(state): State<Arc<AppState>>) -> (StatusCode, String) {
    match &state.prometheus {
        Some(handle) => (StatusCode::OK, handle.render()),
        None => (StatusCode::NOT_FOUND, String::new()),
    }
}

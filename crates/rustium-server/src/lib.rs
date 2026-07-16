//! HTTP health, status, lifecycle, and Prometheus endpoints.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use prometheus::{Encoder, Gauge, IntGauge, Registry, TextEncoder};
use rustium_core::{ConnectorState, Error, Result, RuntimeStatus, SignalRecord, SignalSender};
use serde_json::json;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::{catch_panic::CatchPanicLayer, trace::TraceLayer};

#[derive(Clone)]
struct AppState {
    status: RuntimeStatus,
    cancellation: CancellationToken,
    enable_mutations: bool,
    signal_sender: Option<SignalSender>,
    metrics: Arc<Metrics>,
}

struct Metrics {
    registry: Registry,
    delivered_events: IntGauge,
    failed_events: IntGauge,
    sink_retry_attempts: IntGauge,
    queue_depth: IntGauge,
    connector_state: IntGauge,
    source_lag_seconds: Gauge,
    checkpoint_age_seconds: Gauge,
    last_event_age_seconds: Gauge,
    state_age_seconds: Gauge,
}

impl Metrics {
    fn new() -> Result<Self> {
        let registry = Registry::new();
        let delivered_events = IntGauge::new(
            "rustium_delivered_events",
            "Number of events durably acknowledged by the configured sink",
        )
        .map_err(metrics_error)?;
        let failed_events = IntGauge::new(
            "rustium_failed_events",
            "Number of events that failed processing",
        )
        .map_err(metrics_error)?;
        let queue_depth = IntGauge::new(
            "rustium_pipeline_queue_depth",
            "Current source-to-runtime queue depth",
        )
        .map_err(metrics_error)?;
        let sink_retry_attempts = IntGauge::new(
            "rustium_sink_retry_attempts",
            "Number of retry attempts for retryable Sink operations",
        )
        .map_err(metrics_error)?;
        let connector_state = IntGauge::new(
            "rustium_connector_state",
            "Connector state as a numeric lifecycle value",
        )
        .map_err(metrics_error)?;
        let source_lag_seconds = Gauge::new(
            "rustium_source_lag_seconds",
            "Current wall-clock lag from the last durably acknowledged source timestamp, or NaN when unavailable",
        )
        .map_err(metrics_error)?;
        let checkpoint_age_seconds = Gauge::new(
            "rustium_checkpoint_age_seconds",
            "Seconds since the last durably persisted checkpoint, or NaN when unavailable",
        )
        .map_err(metrics_error)?;
        let last_event_age_seconds = Gauge::new(
            "rustium_last_event_age_seconds",
            "Seconds since the last observed source event, or NaN when unavailable",
        )
        .map_err(metrics_error)?;
        let state_age_seconds = Gauge::new(
            "rustium_connector_state_age_seconds",
            "Seconds since the connector entered its current lifecycle state",
        )
        .map_err(metrics_error)?;
        for collector in [
            Box::new(delivered_events.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(failed_events.clone()),
            Box::new(sink_retry_attempts.clone()),
            Box::new(queue_depth.clone()),
            Box::new(connector_state.clone()),
            Box::new(source_lag_seconds.clone()),
            Box::new(checkpoint_age_seconds.clone()),
            Box::new(last_event_age_seconds.clone()),
            Box::new(state_age_seconds.clone()),
        ] {
            registry.register(collector).map_err(metrics_error)?;
        }
        Ok(Self {
            registry,
            delivered_events,
            failed_events,
            sink_retry_attempts,
            queue_depth,
            connector_state,
            source_lag_seconds,
            checkpoint_age_seconds,
            last_event_age_seconds,
            state_age_seconds,
        })
    }
}

pub async fn serve(
    bind: SocketAddr,
    status: RuntimeStatus,
    cancellation: CancellationToken,
    enable_mutations: bool,
    signal_sender: Option<SignalSender>,
) -> Result<()> {
    let state = AppState {
        status,
        cancellation: cancellation.clone(),
        enable_mutations,
        signal_sender,
        metrics: Arc::new(Metrics::new()?),
    };
    let app = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/v1/connector/status", get(connector_status))
        .route("/v1/connector/stop", post(stop_connector))
        .route("/v1/connector/signals", post(submit_signal))
        .route("/metrics", get(metrics))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = TcpListener::bind(bind).await?;
    tracing::info!(%bind, "management server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(cancellation.cancelled_owned())
        .await
        .map_err(|error| Error::Source(format!("management server failed: {error}")))
}

async fn live() -> impl IntoResponse {
    Json(json!({"status": "live"}))
}

async fn ready(State(state): State<AppState>) -> Response {
    let status = state.status.snapshot().await;
    let ready = matches!(
        status.state,
        ConnectorState::Snapshotting | ConnectorState::Streaming | ConnectorState::Paused
    );
    let code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(json!({"ready": ready, "state": status.state}))).into_response()
}

async fn connector_status(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.status.snapshot().await)
}

async fn stop_connector(State(state): State<AppState>) -> Response {
    if !state.enable_mutations {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "mutating management endpoints are disabled"})),
        )
            .into_response();
    }
    state.cancellation.cancel();
    (StatusCode::ACCEPTED, Json(json!({"status": "stopping"}))).into_response()
}

async fn submit_signal(
    State(state): State<AppState>,
    Json(signal): Json<SignalRecord>,
) -> Response {
    if !state.enable_mutations {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "mutating management endpoints are disabled"})),
        )
            .into_response();
    }
    let Some(sender) = &state.signal_sender else {
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": "in-process signal channel is not enabled"})),
        )
            .into_response();
    };
    match sender.send(signal).await {
        Ok(()) => (StatusCode::ACCEPTED, Json(json!({"status": "accepted"}))).into_response(),
        Err(Error::Configuration(message)) => {
            (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
        }
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": error.to_string()})),
        )
            .into_response(),
    }
}

async fn metrics(State(state): State<AppState>) -> Response {
    let status = state.status.snapshot().await;
    state
        .metrics
        .delivered_events
        .set(i64::try_from(status.delivered_events).unwrap_or(i64::MAX));
    state
        .metrics
        .failed_events
        .set(i64::try_from(status.failed_events).unwrap_or(i64::MAX));
    state
        .metrics
        .sink_retry_attempts
        .set(i64::try_from(status.sink_retry_attempts).unwrap_or(i64::MAX));
    state
        .metrics
        .queue_depth
        .set(i64::try_from(status.queue_depth).unwrap_or(i64::MAX));
    state
        .metrics
        .connector_state
        .set(state_number(status.state));
    state.metrics.source_lag_seconds.set(
        status
            .source_lag_millis
            .map_or(f64::NAN, |lag| lag as f64 / 1_000.0),
    );
    let now = Utc::now();
    state
        .metrics
        .checkpoint_age_seconds
        .set(age_seconds(status.last_checkpoint_at, now));
    state
        .metrics
        .last_event_age_seconds
        .set(age_seconds(status.last_event_observed_at, now));
    state
        .metrics
        .state_age_seconds
        .set(age_seconds(Some(status.state_since), now));

    let encoder = TextEncoder::new();
    let mut body = Vec::new();
    if let Err(error) = encoder.encode(&state.metrics.registry.gather(), &mut body) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("metric encoding failed: {error}"),
        )
            .into_response();
    }
    let mut response = body.into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
}

const fn state_number(state: ConnectorState) -> i64 {
    match state {
        ConnectorState::Created => 0,
        ConnectorState::Starting => 1,
        ConnectorState::Snapshotting => 2,
        ConnectorState::Streaming => 3,
        ConnectorState::Paused => 4,
        ConnectorState::Failed => 5,
        ConnectorState::Stopping => 6,
        ConnectorState::Stopped => 7,
    }
}

fn metrics_error(error: prometheus::Error) -> Error {
    Error::Source(format!("metrics initialization failed: {error}"))
}

fn age_seconds(timestamp: Option<DateTime<Utc>>, now: DateTime<Utc>) -> f64 {
    timestamp.map_or(f64::NAN, |timestamp| {
        now.signed_duration_since(timestamp)
            .num_milliseconds()
            .max(0) as f64
            / 1_000.0
    })
}

#[cfg(test)]
mod tests {
    use rustium_core::signal_channel;

    use super::*;

    fn app_state(enable_mutations: bool, signal_sender: Option<SignalSender>) -> AppState {
        AppState {
            status: RuntimeStatus::new("test"),
            cancellation: CancellationToken::new(),
            enable_mutations,
            signal_sender,
            metrics: Arc::new(Metrics::new().unwrap()),
        }
    }

    #[tokio::test]
    async fn gates_and_delivers_management_signals() {
        let (sender, mut receiver) = signal_channel(1);
        let signal = SignalRecord::new(
            "snapshot-1",
            "execute-snapshot",
            json!({"type": "incremental"}),
        );
        let response =
            submit_signal(State(app_state(true, Some(sender))), Json(signal.clone())).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(receiver.recv().await.unwrap().record(), &signal);

        let response = submit_signal(State(app_state(false, None)), Json(signal.clone())).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let response = submit_signal(State(app_state(true, None)), Json(signal)).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn exposes_unknown_source_lag_as_nan() {
        let response = metrics(State(app_state(false, None))).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("rustium_source_lag_seconds NaN"));
        assert!(body.contains("rustium_checkpoint_age_seconds NaN"));
        assert!(body.contains("rustium_last_event_age_seconds NaN"));
        assert!(body.contains("rustium_sink_retry_attempts 0"));
    }
}

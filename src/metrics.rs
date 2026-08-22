use hbb_common::log;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

pub type Collector = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = String> + Send>> + Send + Sync>;

pub fn encode_metrics(gauges: &[(prometheus::Gauge, f64)]) -> String {
    use prometheus::Encoder;
    for (gauge, value) in gauges {
        gauge.set(*value);
    }
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        log::error!("Failed to encode metrics: {}", e);
        return String::new();
    }
    String::from_utf8(buffer).unwrap_or_default()
}

pub async fn start_metrics_server(
    bind_addr: Option<IpAddr>,
    port: u16,
    collector: Collector,
) {
    let addr = SocketAddr::from((
        bind_addr.unwrap_or(IpAddr::from([0, 0, 0, 0])),
        port,
    ));

    let app = axum::Router::new().route(
        "/metrics",
        axum::routing::get(move || {
            let collector = collector.clone();
            async move {
                let body = collector().await;
                (
                    [(
                        axum::http::header::CONTENT_TYPE,
                        "text/plain; version=0.0.4; charset=utf-8",
                    )],
                    body,
                )
            }
        }),
    );

    log::info!("Metrics server listening on http://{}/metrics", addr);
    if let Err(e) = axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
    {
        log::error!("Metrics server error: {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_metrics_produces_valid_output() {
        let gauge = prometheus::Gauge::with_opts(
            prometheus::Opts::new("test_metric_output", "A test metric"),
        )
        .unwrap();
        prometheus::register(Box::new(gauge.clone())).unwrap();

        let result = encode_metrics(&[(gauge, 42.0)]);
        assert!(result.contains("# HELP test_metric_output A test metric"));
        assert!(result.contains("# TYPE test_metric_output gauge"));
        assert!(result.contains("test_metric_output 42"));
    }

    #[test]
    fn encode_metrics_sets_correct_value() {
        let gauge = prometheus::Gauge::with_opts(
            prometheus::Opts::new("test_metric_value", "A test metric for value"),
        )
        .unwrap();
        prometheus::register(Box::new(gauge.clone())).unwrap();

        let result = encode_metrics(&[(gauge, 123.0)]);
        assert!(result.contains("test_metric_value 123"));
    }
}

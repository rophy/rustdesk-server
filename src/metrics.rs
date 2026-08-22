use hbb_common::log;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

type Collector = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = String> + Send>> + Send + Sync>;

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

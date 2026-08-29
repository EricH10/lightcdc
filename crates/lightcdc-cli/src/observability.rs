//! Owns production metric sampling and the optional Prometheus listener.

use std::{net::SocketAddr, time::Duration};

use anyhow::Context;
use lightcdc_core::Config;
use lightcdc_runtime::{
    ProductionMetrics, ShutdownReceiver, StorageMetricsSampler, bind_metrics_listener,
    serve_metrics,
};
use lightcdc_storage::RedbEventStore;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::{failure, store::storage_options};

/// Keeps every observability worker alive until coordinated process shutdown.
pub(crate) struct ObservabilityRuntime {
    metrics: ProductionMetrics,
    _sampler: Option<StorageMetricsSampler>,
    server: Option<JoinHandle<anyhow::Result<()>>>,
}

impl ObservabilityRuntime {
    /// Samples storage synchronously and binds the optional metrics listener.
    pub(crate) async fn start(
        config: &Config,
        store: RedbEventStore,
        shutdown: ShutdownReceiver,
        grpc_addr: Option<SocketAddr>,
    ) -> anyhow::Result<Self> {
        let metrics = ProductionMetrics::new(
            config.runtime.max_storage_bytes,
            config.runtime.min_free_disk_bytes,
            config.runtime.max_active_subscriptions,
            config.runtime.max_durable_consumers,
            config.runtime.max_api_connections,
            config.observability.metrics_max_connections,
        );
        let storage = storage_options(config);
        let sampler = if config.observability.metrics_enabled {
            Some(StorageMetricsSampler::start(
                metrics.clone(),
                store,
                storage.data_dir,
                config.source.name.clone(),
                Duration::from_secs(config.observability.metrics_sample_interval_seconds),
            )?)
        } else {
            None
        };

        let server = if config.observability.metrics_enabled {
            let metrics_addr = config.metrics_addr().map_err(anyhow::Error::msg)?;
            if grpc_addr == Some(metrics_addr) {
                return Err(failure::configuration(anyhow::anyhow!(
                    "gRPC and production metrics cannot bind the same address"
                )));
            }
            if !metrics_addr.ip().is_loopback() {
                warn!(
                    %metrics_addr,
                    "production metrics are plaintext; bind only on a trusted monitoring network"
                );
            }
            let listener = bind_metrics_listener(metrics_addr).await?;
            let server_metrics = metrics.clone();
            let max_connections = config.observability.metrics_max_connections;
            Some(tokio::spawn(async move {
                serve_metrics(listener, server_metrics, max_connections, shutdown).await
            }))
        } else {
            None
        };

        Ok(Self {
            metrics,
            _sampler: sampler,
            server,
        })
    }

    /// Clones the cheap atomic metric handle used by capture and serving paths.
    pub(crate) fn metrics(&self) -> ProductionMetrics {
        self.metrics.clone()
    }

    /// Waits forever when disabled or reports an unexpected metrics-server exit.
    pub(crate) async fn stopped(&mut self) -> anyhow::Error {
        let result = match self.server.as_mut() {
            Some(server) => match server.await {
                Ok(Ok(())) => anyhow::anyhow!("production metrics server stopped unexpectedly"),
                Ok(Err(error)) => error.context("production metrics server failed"),
                Err(error) => anyhow::Error::new(error).context("production metrics task failed"),
            },
            None => std::future::pending().await,
        };
        self.server.take();
        result
    }

    /// Joins the HTTP task within the process shutdown deadline.
    pub(crate) async fn shutdown(mut self, timeout: Duration) -> anyhow::Result<()> {
        let Some(mut server) = self.server.take() else {
            return Ok(());
        };
        match tokio::time::timeout(timeout, &mut server).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => {
                Err(error).context("production metrics server failed while draining")
            }
            Ok(Err(error)) => Err(error).context("production metrics task failed while draining"),
            Err(_) => {
                server.abort();
                let _ = server.await;
                anyhow::bail!("production metrics server did not stop within {timeout:?}");
            }
        }
    }
}

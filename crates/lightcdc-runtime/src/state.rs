//! Shares process lifecycle and shutdown state across capture and serving tasks.

use std::sync::Arc;

use tokio::sync::watch;

use crate::ProductionMetrics;

/// Stable lifecycle states exposed through readiness and structured logs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeState {
    Starting,
    Capturing,
    Retrying,
    Degraded,
    Draining,
    Failed,
}

/// Current lifecycle state with an operator-facing reason when useful.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeStatus {
    pub state: RuntimeState,
    pub detail: Option<Arc<str>>,
}

/// Publishes runtime transitions to health and observability consumers.
#[derive(Clone, Debug)]
pub struct RuntimeStateHandle {
    sender: watch::Sender<RuntimeStatus>,
    metrics: Option<ProductionMetrics>,
}

/// Observes lifecycle transitions without owning process state.
#[derive(Clone, Debug)]
pub struct RuntimeStateReceiver {
    receiver: watch::Receiver<RuntimeStatus>,
}

/// Triggers coordinated process shutdown exactly once.
#[derive(Clone, Debug)]
pub struct ShutdownHandle {
    sender: watch::Sender<bool>,
}

/// Allows long-lived tasks to stop when shutdown begins.
#[derive(Clone, Debug)]
pub struct ShutdownReceiver {
    receiver: watch::Receiver<bool>,
}

/// Creates one lifecycle publisher and its initial observer.
pub fn runtime_state_channel() -> (RuntimeStateHandle, RuntimeStateReceiver) {
    runtime_state_channel_inner(None)
}

/// Creates lifecycle state that also updates production metrics atomically.
pub fn runtime_state_channel_with_metrics(
    metrics: ProductionMetrics,
) -> (RuntimeStateHandle, RuntimeStateReceiver) {
    runtime_state_channel_inner(Some(metrics))
}

fn runtime_state_channel_inner(
    metrics: Option<ProductionMetrics>,
) -> (RuntimeStateHandle, RuntimeStateReceiver) {
    let (sender, receiver) = watch::channel(RuntimeStatus {
        state: RuntimeState::Starting,
        detail: None,
    });
    (
        RuntimeStateHandle { sender, metrics },
        RuntimeStateReceiver { receiver },
    )
}

/// Creates one shutdown trigger and its initial observer.
pub fn shutdown_channel() -> (ShutdownHandle, ShutdownReceiver) {
    let (sender, receiver) = watch::channel(false);
    (ShutdownHandle { sender }, ShutdownReceiver { receiver })
}

impl RuntimeStateHandle {
    /// Records a state transition and optional operator-facing context.
    pub fn transition(&self, state: RuntimeState, detail: Option<String>) {
        if let Some(metrics) = &self.metrics {
            metrics.record_runtime_state(state);
        }
        let detail = detail.map(Arc::<str>::from);
        self.sender.send_replace(RuntimeStatus { state, detail });
    }

    /// Creates another independent observer of current and future state.
    pub fn subscribe(&self) -> RuntimeStateReceiver {
        RuntimeStateReceiver {
            receiver: self.sender.subscribe(),
        }
    }
}

impl RuntimeStateReceiver {
    /// Returns a snapshot of the current lifecycle state.
    pub fn current(&self) -> RuntimeStatus {
        self.receiver.borrow().clone()
    }

    /// Waits for another lifecycle transition.
    pub async fn changed(&mut self) -> Result<RuntimeStatus, watch::error::RecvError> {
        self.receiver.changed().await?;
        Ok(self.current())
    }
}

impl ShutdownHandle {
    /// Announces that every long-lived task should begin draining.
    pub fn trigger(&self) {
        self.sender.send_replace(true);
    }

    /// Creates another independent shutdown observer.
    pub fn subscribe(&self) -> ShutdownReceiver {
        ShutdownReceiver {
            receiver: self.sender.subscribe(),
        }
    }
}

impl ShutdownReceiver {
    /// Returns true after coordinated shutdown has started.
    pub fn is_triggered(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Waits until coordinated shutdown starts or every trigger is dropped.
    pub async fn cancelled(&mut self) {
        while !self.is_triggered() {
            if self.receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lifecycle_and_shutdown_updates_reach_independent_observers() {
        let (state, mut state_rx) = runtime_state_channel();
        let second_state_rx = state.subscribe();
        state.transition(
            RuntimeState::Retrying,
            Some("PostgreSQL unavailable".to_owned()),
        );
        assert_eq!(
            state_rx.changed().await.expect("state update").state,
            RuntimeState::Retrying
        );
        assert_eq!(second_state_rx.current().state, RuntimeState::Retrying);

        let (shutdown, mut shutdown_rx) = shutdown_channel();
        let mut second_shutdown_rx = shutdown.subscribe();
        shutdown.trigger();
        shutdown_rx.cancelled().await;
        second_shutdown_rx.cancelled().await;
        assert!(shutdown_rx.is_triggered());
    }
}

//! Eclipse iceoryx2 zero-copy shared-memory IPC sink built on the pure-Rust `iceoryx2` crate.
//!
//! Delivery profile:
//! - initializes a real `iceoryx2` node (`ipc_threadsafe::Service`) with signal handling disabled;
//! - opens or creates a `publish_subscribe::<[u8]>` service for the configured `ServiceName`;
//! - loans uninitialized shared-memory slices (`loan_slice_uninit(payload.len())`), copies payload
//!   bytes directly into shared memory (`write_from_slice`), and publishes (`send()`);
//! - latches the first initialization, loan, or delivery failure so subsequent calls fail fast.

use super::{Sink, SinkRecord};
use crate::error::ReplayError;
use async_trait::async_trait;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::port::update_connections::UpdateConnections;
use iceoryx2::prelude::*;
use iceoryx2::service::builder::publish_subscribe::{
    PublishSubscribeOpenError, PublishSubscribeOpenOrCreateError,
};
use iceoryx2::service::port_factory::publish_subscribe::PortFactory;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use tracing::{debug, error, info, warn};

pub use iceoryx2::prelude::{AllocationStrategy, BackpressureStrategy};

/// Type alias for backpressure handling when a subscriber buffer is full.
pub type UnableToDeliverStrategy = BackpressureStrategy;

/// Tunables for the Eclipse `iceoryx2` zero-copy shared-memory IPC sink.
#[derive(Debug, Clone)]
pub struct Iceoryx2Options {
    /// Initial maximum slice length in bytes (`[u8]`) preallocated per sample.
    pub initial_max_slice_len: usize,
    /// Strategy used when a payload exceeds `initial_max_slice_len`.
    pub allocation_strategy: AllocationStrategy,
    /// Maximum number of samples this publisher can loan in parallel.
    pub max_loaned_samples: usize,
    /// Strategy when a subscriber's buffer is full in a non-overflowing service.
    pub unable_to_deliver_strategy: BackpressureStrategy,
    /// Maximum buffer size configured on the publish-subscribe service for subscribers.
    pub subscriber_max_buffer_size: usize,
    /// Whether the service safely overwrites the oldest sample when a subscriber buffer is full.
    pub enable_safe_overflow: bool,
}

impl Default for Iceoryx2Options {
    fn default() -> Self {
        Self {
            initial_max_slice_len: 4096,
            allocation_strategy: AllocationStrategy::PowerOfTwo,
            max_loaned_samples: 2,
            unable_to_deliver_strategy: BackpressureStrategy::DiscardData,
            subscriber_max_buffer_size: 256,
            enable_safe_overflow: true,
        }
    }
}

/// Latches the first delivery or loan failure of a run so subsequent sends fail immediately.
#[derive(Default)]
struct Failure {
    failed: AtomicBool,
    first: Mutex<Option<String>>,
}

impl Failure {
    fn record(&self, message: String) -> String {
        let mut first = self.first.lock().unwrap_or_else(|e| e.into_inner());
        let latched = match first.as_ref() {
            Some(existing) => existing.clone(),
            None => {
                error!("iceoryx2 IPC delivery failed: {message}");
                *first = Some(message.clone());
                message
            }
        };
        self.failed.store(true, Ordering::SeqCst);
        latched
    }

    fn is_failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }

    fn message(&self) -> Option<String> {
        self.first.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

pub struct Iceoryx2Sink {
    _node: Mutex<Node<ipc_threadsafe::Service>>,
    _service: PortFactory<ipc_threadsafe::Service, [u8], ()>,
    publisher: Mutex<Publisher<ipc_threadsafe::Service, [u8], ()>>,
    service_name: String,
    options: Iceoryx2Options,
    failure: Failure,
    sent: AtomicU64,
}

impl Iceoryx2Sink {
    pub fn new(service_name: impl Into<String>) -> Result<Self, ReplayError> {
        let name = service_name.into();
        connect(&name, Iceoryx2Options::default())
    }

    pub fn with_options(
        service_name: impl Into<String>,
        options: Iceoryx2Options,
    ) -> Result<Self, ReplayError> {
        let name = service_name.into();
        connect(&name, options)
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn options(&self) -> &Iceoryx2Options {
        &self.options
    }

    /// Number of records loaned into shared memory and sent so far.
    pub fn sent_records(&self) -> u64 {
        self.sent.load(Ordering::SeqCst)
    }

    fn fail_if_failed(&self) -> Result<(), ReplayError> {
        if !self.failure.is_failed() {
            return Ok(());
        }
        match self.failure.message() {
            Some(message) => Err(ReplayError::Sink(message)),
            None => Err(ReplayError::Sink("iceoryx2 sink failed".to_string())),
        }
    }

    fn record_failure(&self, message: String) -> ReplayError {
        ReplayError::Sink(self.failure.record(message))
    }

    fn send_payload(&self, topic: &str, payload: &[u8]) -> Result<(), ReplayError> {
        self.fail_if_failed()?;

        let trimmed = topic.trim();
        let target = if trimmed.is_empty() {
            self.service_name.as_str()
        } else {
            trimmed
        };
        if target != self.service_name {
            return Err(self.record_failure(format!(
                "this iceoryx2 sink publishes to '{}', not '{topic}'",
                self.service_name
            )));
        }

        let publisher = self.publisher.lock().unwrap_or_else(|e| e.into_inner());
        let sample = publisher.loan_slice_uninit(payload.len()).map_err(|e| {
            self.record_failure(format!(
                "failed to loan shared-memory slice ({} bytes) on iceoryx2 service '{}': {e}",
                payload.len(),
                self.service_name
            ))
        })?;

        let sample = sample.write_from_slice(payload);
        let recipients = sample.send().map_err(|e| {
            self.record_failure(format!(
                "failed to send shared-memory sample on iceoryx2 service '{}': {e}",
                self.service_name
            ))
        })?;

        let seq = self.sent.fetch_add(1, Ordering::SeqCst) + 1;
        debug!(
            service_name = %self.service_name,
            bytes = payload.len(),
            recipients,
            sent = seq,
            "iceoryx2 shared-memory sample delivered"
        );
        Ok(())
    }
}

/// Initialize an Eclipse `iceoryx2` node, open or create the publish-subscribe `[u8]` service,
/// and create a zero-copy shared-memory publisher.
pub fn connect(service_name: &str, options: Iceoryx2Options) -> Result<Iceoryx2Sink, ReplayError> {
    let trimmed_name = service_name.trim();
    if trimmed_name.is_empty() {
        return Err(ReplayError::Sink(
            "iceoryx2 service_name must not be empty".to_string(),
        ));
    }

    let iox_service_name = ServiceName::new(trimmed_name).map_err(|e| {
        ReplayError::Sink(format!(
            "invalid iceoryx2 service_name '{trimmed_name}': {e}"
        ))
    })?;

    if options.initial_max_slice_len == 0 {
        return Err(ReplayError::Sink(
            "iceoryx2 initial_max_slice_len must be at least 1".to_string(),
        ));
    }
    if options.max_loaned_samples == 0 {
        return Err(ReplayError::Sink(
            "iceoryx2 max_loaned_samples must be at least 1".to_string(),
        ));
    }
    if options.subscriber_max_buffer_size == 0 {
        return Err(ReplayError::Sink(
            "iceoryx2 subscriber_max_buffer_size must be at least 1".to_string(),
        ));
    }

    info!(
        service_name = %trimmed_name,
        initial_max_slice_len = options.initial_max_slice_len,
        allocation_strategy = ?options.allocation_strategy,
        "Initializing Eclipse iceoryx2 zero-copy IPC publisher"
    );

    let node = NodeBuilder::new()
        .signal_handling_mode(SignalHandlingMode::Disabled)
        .create::<ipc_threadsafe::Service>()
        .map_err(|e| ReplayError::Sink(format!("failed to create iceoryx2 IPC node: {e}")))?;

    let service = match node
        .service_builder(&iox_service_name)
        .publish_subscribe::<[u8]>()
        .subscriber_max_buffer_size(options.subscriber_max_buffer_size)
        .enable_safe_overflow(options.enable_safe_overflow)
        .open_or_create()
    {
        Ok(svc) => svc,
        Err(PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(
            reason @ (PublishSubscribeOpenError::DoesNotSupportRequestedMinBufferSize
            | PublishSubscribeOpenError::IncompatibleOverflowBehavior),
        )) => {
            warn!(
                service_name = %trimmed_name,
                ?reason,
                "Attaching to existing iceoryx2 service with pre-existing buffer/overflow settings"
            );
            node.service_builder(&iox_service_name)
                .publish_subscribe::<[u8]>()
                .open_or_create()
                .map_err(|e| {
                    ReplayError::Sink(format!(
                        "failed to open or create iceoryx2 service '{trimmed_name}': {e}"
                    ))
                })?
        }
        Err(e) => {
            return Err(ReplayError::Sink(format!(
                "failed to open or create iceoryx2 service '{trimmed_name}': {e}"
            )));
        }
    };

    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(options.initial_max_slice_len)
        .allocation_strategy(options.allocation_strategy)
        .max_loaned_samples(options.max_loaned_samples)
        .backpressure_strategy(options.unable_to_deliver_strategy)
        .create()
        .map_err(|e| {
            ReplayError::Sink(format!(
                "failed to create iceoryx2 publisher for service '{trimmed_name}': {e}"
            ))
        })?;

    Ok(Iceoryx2Sink {
        _node: Mutex::new(node),
        _service: service,
        publisher: Mutex::new(publisher),
        service_name: trimmed_name.to_string(),
        options,
        failure: Failure::default(),
        sent: AtomicU64::new(0),
    })
}

#[async_trait]
impl Sink for Iceoryx2Sink {
    async fn send(
        &self,
        topic: &str,
        _key: Option<&str>,
        payload: &[u8],
    ) -> Result<(), ReplayError> {
        self.send_payload(topic, payload)
    }

    async fn send_record(&self, topic: &str, record: &SinkRecord<'_>) -> Result<(), ReplayError> {
        self.send_payload(topic, record.payload)
    }

    async fn flush(&self) -> Result<(), ReplayError> {
        self.fail_if_failed()?;
        let publisher = self.publisher.lock().unwrap_or_else(|e| e.into_inner());
        publisher.update_connections().map_err(|e| {
            self.record_failure(format!(
                "iceoryx2 connection update during flush for service '{}' failed: {e}",
                self.service_name
            ))
        })?;
        info!(
            service_name = %self.service_name,
            sent = self.sent_records(),
            "Flushed all records to iceoryx2 IPC"
        );
        Ok(())
    }
}

pub async fn create_iceoryx2_sink(service_name: &str) -> Result<Box<dyn Sink>, ReplayError> {
    Ok(Box::new(connect(service_name, Iceoryx2Options::default())?))
}

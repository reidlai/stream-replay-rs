use super::Sink;
use crate::error::ReplayError;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

pub type RecordedMessage = (String, Option<String>, Vec<u8>);

#[derive(Clone, Default)]
pub struct StdoutSink {
    pub topic: String,
    recorded_messages: Arc<Mutex<Vec<RecordedMessage>>>,
    silent: bool,
}

impl StdoutSink {
    pub fn new(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            recorded_messages: Arc::new(Mutex::new(Vec::new())),
            silent: false,
        }
    }

    pub fn new_silent(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            recorded_messages: Arc::new(Mutex::new(Vec::new())),
            silent: true,
        }
    }

    pub fn get_recorded(&self) -> Vec<RecordedMessage> {
        self.recorded_messages.lock().unwrap().clone()
    }
}

#[async_trait]
impl Sink for StdoutSink {
    async fn send(&self, topic: &str, key: Option<&str>, payload: &[u8]) -> Result<(), ReplayError> {
        let key_str = key.unwrap_or("none");
        let payload_str = String::from_utf8_lossy(payload);

        if !self.silent {
            println!("[{topic}] key={key_str} payload={payload_str}");
        }

        let mut rec = self.recorded_messages.lock().unwrap();
        if rec.len() < 5000 {
            rec.push((topic.to_string(), key.map(|s| s.to_string()), payload.to_vec()));
        }

        Ok(())
    }

    async fn flush(&self) -> Result<(), ReplayError> {
        Ok(())
    }
}


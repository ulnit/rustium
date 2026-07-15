use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalRecord {
    pub id: String,
    #[serde(rename = "type")]
    pub signal_type: String,
    #[serde(default = "empty_data")]
    pub data: serde_json::Value,
}

impl SignalRecord {
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        signal_type: impl Into<String>,
        data: serde_json::Value,
    ) -> Self {
        Self {
            id: id.into(),
            signal_type: signal_type.into(),
            data,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() || self.signal_type.trim().is_empty() {
            return Err(Error::Configuration(
                "signal requires non-empty id and type".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SignalSender(mpsc::Sender<SignalRecord>);

impl SignalSender {
    pub async fn send(&self, signal: SignalRecord) -> Result<()> {
        signal.validate()?;
        self.0.send(signal).await.map_err(|_| Error::Cancelled)
    }
}

#[must_use]
pub fn signal_channel(capacity: usize) -> (SignalSender, mpsc::Receiver<SignalRecord>) {
    let (sender, receiver) = mpsc::channel(capacity.max(1));
    (SignalSender(sender), receiver)
}

fn empty_data() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn validates_and_delivers_typed_signals() {
        let (sender, mut receiver) = signal_channel(1);
        let signal = SignalRecord::new(
            "snapshot-1",
            "execute-snapshot",
            serde_json::json!({"type": "incremental"}),
        );
        sender.send(signal.clone()).await.unwrap();
        assert_eq!(receiver.recv().await, Some(signal));
        assert!(
            sender
                .send(SignalRecord::new(
                    "",
                    "pause-snapshot",
                    serde_json::json!({})
                ))
                .await
                .unwrap_err()
                .to_string()
                .contains("non-empty id and type")
        );
    }
}

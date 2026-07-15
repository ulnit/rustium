use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

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

    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() || self.signal_type.trim().is_empty() {
            return Err(Error::Configuration(
                "signal requires non-empty id and type".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SignalAcknowledgement(Arc<Mutex<Option<oneshot::Sender<Result<()>>>>>);

impl SignalAcknowledgement {
    pub fn acknowledge(&self) {
        if let Some(sender) = self.0.lock().unwrap().take() {
            let _ = sender.send(Ok(()));
        }
    }
}

impl std::fmt::Debug for SignalAcknowledgement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("SignalAcknowledgement").finish()
    }
}

impl PartialEq for SignalAcknowledgement {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Debug)]
pub struct SignalDelivery {
    record: SignalRecord,
    acknowledgement: Option<SignalAcknowledgement>,
}

impl SignalDelivery {
    #[must_use]
    pub fn record(&self) -> &SignalRecord {
        &self.record
    }

    #[must_use]
    pub fn into_acknowledgement(self) -> Option<SignalAcknowledgement> {
        self.acknowledgement
    }

    pub fn acknowledge(self) {
        if let Some(acknowledgement) = self.acknowledgement {
            acknowledgement.acknowledge();
        }
    }
}

#[derive(Debug, Clone)]
pub struct SignalSender(mpsc::Sender<SignalDelivery>);

impl SignalSender {
    pub async fn send(&self, signal: SignalRecord) -> Result<()> {
        signal.validate()?;
        self.0
            .send(SignalDelivery {
                record: signal,
                acknowledgement: None,
            })
            .await
            .map_err(|_| Error::Cancelled)
    }

    pub async fn send_and_wait(&self, signal: SignalRecord) -> Result<()> {
        signal.validate()?;
        let (sender, receiver) = oneshot::channel();
        self.0
            .send(SignalDelivery {
                record: signal,
                acknowledgement: Some(SignalAcknowledgement(Arc::new(Mutex::new(Some(sender))))),
            })
            .await
            .map_err(|_| Error::Cancelled)?;
        receiver.await.map_err(|_| Error::Cancelled)?
    }
}

#[must_use]
pub fn signal_channel(capacity: usize) -> (SignalSender, mpsc::Receiver<SignalDelivery>) {
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
        let delivery = receiver.recv().await.unwrap();
        assert_eq!(delivery.record(), &signal);
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

    #[tokio::test]
    async fn waits_for_durable_signal_acknowledgement() {
        let (sender, mut receiver) = signal_channel(1);
        let task = tokio::spawn(async move {
            sender
                .send_and_wait(SignalRecord::new(
                    "snapshot-2",
                    "execute-snapshot",
                    serde_json::json!({}),
                ))
                .await
        });
        let delivery = receiver.recv().await.unwrap();
        assert!(!task.is_finished());
        delivery.acknowledge();
        task.await.unwrap().unwrap();
    }
}

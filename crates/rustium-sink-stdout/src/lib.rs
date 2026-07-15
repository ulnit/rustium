//! Development sink that writes newline-delimited JSON to stdout.

use async_trait::async_trait;
use rustium_core::{DeliveryBatch, Durability, Result, Sink};
use tokio::io::{AsyncWriteExt, BufWriter, Stdout};

pub struct StdoutSink {
    writer: BufWriter<Stdout>,
}

impl Default for StdoutSink {
    fn default() -> Self {
        Self {
            writer: BufWriter::new(tokio::io::stdout()),
        }
    }
}

#[async_trait]
impl Sink for StdoutSink {
    fn name(&self) -> &'static str {
        "stdout"
    }

    fn durability(&self) -> Durability {
        Durability::BestEffort
    }

    async fn validate(&mut self) -> Result<()> {
        Ok(())
    }

    async fn write(&mut self, batch: &DeliveryBatch) -> Result<()> {
        for event in &batch.events {
            if let Some(payload) = &event.payload {
                self.writer.write_all(payload).await?;
            } else {
                self.writer.write_all(b"null").await?;
            }
            self.writer.write_all(b"\n").await?;
        }
        self.writer.flush().await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await?;
        Ok(())
    }
}

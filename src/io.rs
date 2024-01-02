use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::sync::Synchronizer;

pub struct Consumed {
    id: String,
    sync: Arc<Synchronizer>,
}

impl Drop for Consumed {
    fn drop(&mut self) {
        self.sync.ended(&self.id)
    }
}

/// Defines an end to a pipe that allows data to be received from.
#[derive(Debug)]
pub struct PipeReader<T> {
    pipe_id: String,
    synchronizer: Arc<Synchronizer>,
    rx: Receiver<T>,
}

impl<T> PipeReader<T> {
    pub fn new(
        pipe_id: impl Into<String>,
        synchronizer: Arc<Synchronizer>,
        input_rx: Receiver<T>,
    ) -> Self {
        Self {
            pipe_id: pipe_id.into(),
            synchronizer,
            rx: input_rx,
        }
    }

    /// Provides a callback function to be called once the task has handled a provided
    /// input from this reader.
    ///
    /// This callback function decrements the task count for this pipe.
    ///
    /// We use a callback function here so it can be passed and called in the task definition
    /// without having to give ownership of this reader to the task definition.

    /// Receive the next value from the inner receiver.
    pub async fn read(&mut self) -> Option<(T, Consumed)> {
        self.rx.recv().await.map(|v| (v, self.consumed()))
    }

    fn consumed(&self) -> Consumed {
        Consumed {
            id: self.pipe_id.clone(),
            sync: self.synchronizer.clone(),
        }
    }
}

/// Defines an end to a pipe that allows data to be sent through.
#[derive(Debug)]
pub struct PipeWriter<T> {
    pipe_id: String,
    synchronizer: Arc<Synchronizer>,
    tx: Sender<T>,
}

/// Manually implement [Clone] for [PipeWriter] over `T`, as deriving [Clone]
/// does not implement it over generic parameter.
impl<T> Clone for PipeWriter<T> {
    fn clone(&self) -> Self {
        Self {
            pipe_id: self.pipe_id.clone(),
            synchronizer: self.synchronizer.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl<T> PipeWriter<T> {
    pub fn new(
        pipe_id: impl Into<String>,
        synchronizer: Arc<Synchronizer>,
        output_tx: Sender<T>,
    ) -> Self {
        Self {
            pipe_id: pipe_id.into(),
            synchronizer,
            tx: output_tx,
        }
    }

    /// Increment the task count for this pipe and then send the value through the channel.
    pub async fn write(&self, value: T) {
        self.synchronizer.started(&self.pipe_id);
        self.tx
            .send(value)
            .await
            .expect("failed to send input over channel");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::io::PipeReader;
    use tokio::sync::mpsc::channel;

    use crate::sync::Synchronizer;

    #[tokio::test]
    async fn test_read_consumed_updates_sync_on_drop() {
        let id = "pipe-id";
        let mut sync = Synchronizer::default();
        sync.register(id);
        sync.started_many(id, 4);

        let sync = Arc::new(sync);
        let (tx, rx) = channel::<()>(1);

        let mut input = PipeReader::new(id, sync.clone(), rx);

        tx.send(()).await.unwrap();

        {
            let (_, _c) = input.read().await.unwrap();
            assert_eq!(sync.get(id), 4);
        }
        assert_eq!(sync.get(id), 3);
    }
}

use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::sync;

/// Defines an end to a pipe that allows data to be received from.
///
/// Provide this to [Pipeline::register_worker] as the input for that stage.
#[derive(Debug)]
pub struct PipeReader<T> {
    pipe_id: String,
    synchronizer: Arc<sync::Synchronizer>,
    rx: Receiver<T>,
}

impl<T> PipeReader<T> {
    pub(crate) fn new(
        pipe_id: impl Into<String>,
        synchronizer: Arc<sync::Synchronizer>,
        input_rx: Receiver<T>,
    ) -> Self {
        Self {
            pipe_id: pipe_id.into(),
            synchronizer,
            rx: input_rx,
        }
    }

    pub(crate) fn consumed_callback(&self) -> impl FnOnce() {
        let id = self.pipe_id.clone();
        let sync = self.synchronizer.clone();
        move || {
            sync.ended(id);
        }
    }

    pub(crate) async fn next(&mut self) -> Option<T> {
        self.rx.recv().await
    }
}

/// Defines an end to a pipe that allows data to be sent through.
///
/// Provide this to [Pipeline::register_worker] as an output for a stage.
#[derive(Debug)]
pub struct PipeWriter<T> {
    pipe_id: String,
    synchronizer: Arc<sync::Synchronizer>,
    tx: Sender<T>,
}

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
    pub(crate) fn new(
        pipe_id: impl Into<String>,
        synchronizer: Arc<sync::Synchronizer>,
        output_tx: Sender<T>,
    ) -> Self {
        Self {
            pipe_id: pipe_id.into(),
            synchronizer,
            tx: output_tx,
        }
    }

    pub(crate) async fn submit(&self, value: T) {
        self.synchronizer.started(&self.pipe_id);
        self.send(value).await;
    }

    async fn send(&self, value: T) {
        self.tx
            .send(value)
            .await
            .expect("failed to send input over channel");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::mpsc::channel;

    use crate::sync::Synchronizer;
    use crate::PipeReader;

    #[test]
    fn test_input_consumed_callback_updates_sync() {
        let id = "pipe-id";
        let mut sync = Synchronizer::default();
        sync.register(id);
        sync.started_many(id, 4);

        let sync = Arc::new(sync);
        let (_, rx) = channel::<()>(1);
        let input = PipeReader::new(id, sync.clone(), rx);

        let callback = input.consumed_callback();

        assert_eq!(sync.get(id), 4);
        callback();
        assert_eq!(sync.get(id), 3);
    }
}

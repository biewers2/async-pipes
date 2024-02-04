use std::sync::Arc;

use crate::pipeline::sync::Synchronizer;

macro_rules! variable_channels {
    (<$t:ident> $($var:ident($tx:ty, $rx:ty)),+ $(,)?) => {
        #[derive(Debug)]
        pub enum VarSender<$t> {
            $( $var($tx), )*
        }

        #[derive(Debug)]
        pub enum VarReceiver<$t> {
            $( $var($rx), )*
        }

        impl<$t> Clone for VarSender<$t> {
            fn clone(&self) -> Self {
                match self {
                    $( Self::$var(tx) => Self::$var(tx.clone()), )*
                }
            }
        }

        impl<$t> VarReceiver<$t> {
            async fn recv(&mut self) -> Option<$t> {
                match self {
                    $( Self::$var(rx) => rx.recv().await, )*
                }
            }
        }

        $(
            impl<$t> From<$tx> for VarSender<$t> {
                fn from(value: $tx) -> Self {
                    Self::$var(value)
                }
            }

            impl<$t> From<$rx> for VarReceiver<$t> {
                fn from(value: $rx) -> Self {
                    Self::$var(value)
                }
            }
        )*
    };
}

variable_channels! {
    <T>
    MpscBounded(tokio::sync::mpsc::Sender<T>, tokio::sync::mpsc::Receiver<T>),
    MpscUnbounded(tokio::sync::mpsc::UnboundedSender<T>, tokio::sync::mpsc::UnboundedReceiver<T>),
}

impl<T> VarSender<T> {
    /// Implement send outside of macro because of variations in sender interfaces.
    async fn send(&self, t: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        match self {
            Self::MpscBounded(tx) => tx.send(t).await,
            Self::MpscUnbounded(tx) => tx.send(t),
        }
    }
}

pub struct ConsumeOnDrop {
    id: String,
    sync: Arc<Synchronizer>,
}

impl Drop for ConsumeOnDrop {
    fn drop(&mut self) {
        self.sync.ended(&self.id)
    }
}

/// Defines an end to a pipe that allows data to be received from.
#[derive(Debug)]
pub struct PipeReader<T> {
    pipe_id: String,
    synchronizer: Arc<Synchronizer>,
    rx: VarReceiver<T>,
}

impl<T> PipeReader<T> {
    pub fn new(
        pipe_id: String,
        synchronizer: Arc<Synchronizer>,
        rx: impl Into<VarReceiver<T>>,
    ) -> Self {
        Self {
            pipe_id,
            synchronizer,
            rx: rx.into(),
        }
    }

    #[allow(dead_code)]
    pub fn get_pipe_id(&self) -> &str {
        &self.pipe_id
    }

    /// Receive the next value from the inner receiver.
    pub async fn read(&mut self) -> Option<(T, ConsumeOnDrop)> {
        self.rx.recv().await.map(|v| {
            let cod = ConsumeOnDrop {
                id: self.pipe_id.clone(),
                sync: self.synchronizer.clone(),
            };

            (v, cod)
        })
    }
}

/// Defines an end to a pipe that allows data to be sent through.
#[derive(Debug)]
pub struct PipeWriter<T> {
    pipe_id: String,
    synchronizer: Arc<Synchronizer>,
    tx: VarSender<T>,
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
        pipe_id: String,
        synchronizer: Arc<Synchronizer>,
        tx: impl Into<VarSender<T>>,
    ) -> Self {
        Self {
            pipe_id,
            synchronizer,
            tx: tx.into(),
        }
    }

    #[allow(dead_code)]
    pub fn get_pipe_id(&self) -> &str {
        &self.pipe_id
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

    use tokio::sync::mpsc::channel;

    use super::*;

    #[tokio::test]
    async fn test_read_consumed_updates_sync_on_drop() {
        let id = "pipe-id";
        let mut sync = Synchronizer::default();
        sync.register(id);
        sync.started_many(id, 4);

        let sync = Arc::new(sync);
        let (tx, rx) = channel::<()>(1);

        let mut input = PipeReader::new(id.to_string(), sync.clone(), rx);

        tx.send(()).await.unwrap();

        {
            let (_, _c) = input.read().await.unwrap();
            assert_eq!(sync.get(id), 4);
        }
        assert_eq!(sync.get(id), 3);
    }
}

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinSet;

pub use io::*;

use crate::sync::Synchronizer;

mod io;
pub(crate) mod sync;

/// Signals sent to workers.
///
/// Useful for interrupting the natural worker workflow to tell it something.
enum WorkerSignal {
    Ping,
    Terminate,
}

struct StageWorker<
    I: Send + 'static,
    O: Send + 'static,
    F: FnOnce(I) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Vec<Option<O>>> + Send + 'static,
> {
    name: String,
    signal_rx: Receiver<WorkerSignal>,
    inputs: PipeReader<I>,
    outputs: Vec<PipeWriter<O>>,
    task_definition: F,
}

/// Provided by [Pipeline::from_pipes], used to create the I/O objects for each end of a pipe.
///
/// # Examples
///
/// Create three pipes and demonstrate how to acquire the I/O objects of a pipe by name.
///
/// ```
/// use plumber::{Pipeline, PipeReader, Pipes, PipeWriter};
///
/// // [pipes] is mutable so it can give ownership of values to the I/O objects produced in [Pipes::create_io].
/// let (pipeline, mut pipes): (Pipeline, Pipes) = Pipeline::from_pipes(vec!["a", "b", "c"]);
///
/// let pipe_io = pipes.create_io::<String>("a");
/// assert!(pipe_io.is_some());
///
/// // A tuple is returned containing the I/O objects.
/// let (w, r): (PipeWriter<String>, PipeReader<String>) = pipe_io.unwrap();
///
/// // [None] is returned if the pipe name doesn't exist
/// assert!(pipes.create_io::<String>("d").is_none());
/// ```
#[derive(Debug)]
pub struct Pipes {
    synchronizer: Arc<Synchronizer>,
    names_to_ids: HashMap<String, String>,
}

impl Pipes {
    /// Creates the I/O objects associated with a pipe identified by the provided name.
    ///
    /// If a pipe with the name exists, a tuple with the I/O objects is returned; otherwise,
    /// [None] is returned.
    ///
    /// If the pipe with that name exists, ownership is transferred to the created I/O objects.
    /// This means that a later call with the same name will result in [None], preventing multiple
    /// I/O objects from being created. This is necessary to properly track all data in the pipes.
    ///
    /// # Examples
    ///
    /// See [Pipes] for examples.
    ///
    /// ```
    pub fn create_io<T>(
        &mut self,
        name: impl AsRef<str>,
    ) -> Option<(PipeWriter<T>, PipeReader<T>)> {
        self.names_to_ids.remove(name.as_ref()).map(|id| {
            let (tx, rx) = channel(10);
            (
                PipeWriter::new(&id, self.synchronizer.clone(), tx),
                PipeReader::new(&id, self.synchronizer.clone(), rx),
            )
        })
    }
}

/// A pipeline defines the infrastructure for managing stage workers and transferring data between them
/// using pipes defined by the workers.
///
/// To create a pipeline, use [Pipeline::from_pipes].
///
/// # Examples
///
/// TODO
///
#[derive(Debug)]
pub struct Pipeline {
    synchronizer: Arc<Synchronizer>,
    workers: JoinSet<()>,
    signal_txs: HashMap<String, Sender<WorkerSignal>>,
}

impl Pipeline {
    /// Create a new pipeline using the list of pipe names.
    ///
    /// Using the names, an instance of [Pipes] will also be created which can be used to create
    /// the I/O objects for each pipe. These objects can be given to [Pipeline::register_worker] calls
    /// which tell the pipeline to connect that pipe to the stage worker being registered.
    ///
    /// Providing no names will result in a pipeline that can't transfer any data.
    ///
    /// # Examples
    ///
    /// TODO
    ///
    pub fn from_pipes<S: Into<String>>(names: Vec<S>) -> (Self, Pipes) {
        let mut sync = Synchronizer::default();

        // Construct this before creating the pipe I/O objects
        //
        // Reasoning: [Arc] doesn't allow the inner data (sync) to be mutable, so any mutable operations
        // must be done before storing it into an [Arc]. This is done to avoid having to wrap the inner data
        // in a [Mutex] significantly slowing down read operations on the data (i.e. Arc<Mutex<Synchronizer>>).
        //
        // See the docs of [Synchronizer] for more info on this.
        //
        let mut names_to_ids = HashMap::new();
        for name in names {
            let name = name.into();
            let id = format!("{}-{}", &name, uuid::Uuid::new_v4());

            sync.register(&id);
            names_to_ids.insert(name, id);
        }

        let sync = Arc::new(sync);
        (
            Self {
                synchronizer: sync.clone(),
                workers: JoinSet::default(),
                signal_txs: HashMap::new(),
            },
            Pipes {
                synchronizer: sync,
                names_to_ids,
            },
        )
    }

    /// Wait for all stage workers in the pipeline to finish.
    ///
    /// Once all workers are done running, send the `TERMINATE` signal to all the workers.
    pub async fn wait(mut self) {
        let workers = &mut self.workers;

        let check_workers_completed = async {
            // todo - log join results if failures
            while workers.join_next().await.is_some() {}
        };

        let check_sync_completed = async {
            while !self.synchronizer.completed() {}

            for tx in self.signal_txs.values() {
                tx.send(WorkerSignal::Terminate)
                    .await
                    .expect("failed to send done signal")
            }
        };

        // If either the synchronizer determines we're done, or all workers completed, we're done
        select! {
            _ = check_workers_completed => {},
            _ = check_sync_completed => {},
        }
    }

    /// Register a worker to handle data between pipes.
    ///
    /// This is considered defining a "stage".
    ///
    pub fn register_worker<I, O, F, Fut>(
        &mut self,
        name: impl Into<String>,
        inputs: PipeReader<I>,
        outputs: Vec<PipeWriter<O>>,
        worker_def: F,
    ) where
        I: Send + 'static,
        O: Send + 'static,
        F: FnOnce(I) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Vec<Option<O>>> + Send + 'static,
    {
        let name = name.into();

        let (signal_tx, signal_rx) = channel(1);
        self.signal_txs.insert(name.clone(), signal_tx);

        self.workers.spawn(Self::new_worker(StageWorker {
            name,
            signal_rx,
            inputs,
            outputs,
            task_definition: worker_def,
        }));
    }

    async fn new_worker<I, O, F, Fut>(mut worker: StageWorker<I, O, F, Fut>)
    where
        I: Send + 'static,
        O: Send + 'static,
        F: FnOnce(I) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Vec<Option<O>>> + Send + 'static,
    {
        // Individual tasks asynchronously operating on received input
        let mut tasks = JoinSet::new();

        loop {
            // Reference to avoid moving into closures below
            let tasks_ref = &mut tasks;
            let StageWorker {
                name,
                signal_rx,
                inputs,
                outputs,
                task_definition,
            } = &mut worker;

            // Primary stage worker loop
            let stage_worker_loop = async move {
                while let Some(input) = inputs.read().await {
                    let consumed = inputs.consumed_callback();
                    let outputs = outputs.clone();
                    let task = task_definition.clone();

                    tasks_ref.spawn(async move {
                        let results = task(input).await;
                        for (i, value) in results.into_iter().enumerate() {
                            if let Some(result) = value {
                                outputs
                                    .get(i)
                                    .expect(
                                        "produced output count does not equal output pipe count",
                                    )
                                    .write(result)
                                    .await;
                            }
                        }

                        // Mark input as consumed *after* writing outputs
                        // (avoids false positive of completed pipeline)
                        consumed();
                    });
                }
            };

            select! {
                _ = stage_worker_loop => {
                    break
                }

                // Receive external signals
                Some(signal) = signal_rx.recv() => {
                    match signal {
                        WorkerSignal::Terminate => break,
                        WorkerSignal::Ping => println!("Number of tasks for {}: {}", name, tasks.len())
                    }
                },
            }
        }

        while tasks.join_next().await.is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::sync::Arc;

    use crate::Pipeline;

    #[test]
    fn test_pipeline_from_no_pipes_succeeds() {
        let (_, pipes) = Pipeline::from_pipes(Vec::<String>::new());

        assert_eq!(pipes.names_to_ids.len(), 0);
    }

    #[test]
    fn test_pipeline_from_pipes_succeeds() {
        let mut expected_names = vec!["start", "middle", "end"];
        expected_names.sort();

        let (pipeline, pipes) = Pipeline::from_pipes(expected_names.clone());
        let mut actual_names = pipes
            .names_to_ids
            .keys()
            .map(|s| s.deref())
            .collect::<Vec<&str>>();
        actual_names.sort();

        assert_eq!(pipes.names_to_ids.len(), 3);
        assert_eq!(actual_names, expected_names);

        assert!(Arc::ptr_eq(&pipeline.synchronizer, &pipes.synchronizer));
        for name in expected_names {
            let id = pipes.names_to_ids.get(name).unwrap();
            assert_eq!(pipes.synchronizer.get(id), 0);
        }
    }

    #[test]
    fn test_pipes_create_io_returns_correctly() {
        let names = vec!["start", "middle", "end"];

        let (_, mut pipes) = Pipeline::from_pipes(names);

        assert!(pipes.create_io::<()>("undefined").is_none());
        assert!(pipes.create_io::<()>("start").is_some());
    }
}

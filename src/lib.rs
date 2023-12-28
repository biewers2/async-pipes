//!
//! Plumber provides a simple way to create high-throughput, in-memory pipelines for data processing.
//!
//! ```
//! use std::sync::Arc;
//! use tokio::sync::Mutex;
//! use plumber::Pipeline;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create the pipeline from a list of pipe names.
//!     let (mut pipeline, mut pipes) =
//!         Pipeline::from_pipes(vec!["MapInput", "MapToReduce", "ReduceToLog"]);
//!
//!     // We create "writers" (*_w) and "readers" (*_r) to transfer data
//!     let (map_input_w, map_input_r) = pipes.create_io("MapInput").unwrap();
//!     let (map_to_reduce_w, map_to_reduce_r) = pipes.create_io("MapToReduce").unwrap();
//!
//!     // After creating the pipes, stage workers are registered in the pipeline.
//!     pipeline.register_stage(
//!         "MapStage",
//!         map_input_r,
//!         vec![map_to_reduce_w],
//!         |value: String| async move {
//!             let new_value = format!("{}!", value);
//!             vec![Some(new_value)]
//!         },
//!     );
//!
//!     // Variables can be updated by a task by wrapping it in a `Mutex` (to make it mutable)
//!     // and then in an `Arc` (for data ownership between more than one thread).
//!     let total_count = Arc::new(Mutex::new(0));
//!     let reduce_total_count = total_count.clone();
//!
//!     pipeline.register_stage(
//!         "ReduceStage",
//!         map_to_reduce_r,
//!         vec![],
//!         |value: String| async move {
//!             *reduce_total_count.lock().await += value.len();
//!             Vec::<Option<()>>::new()
//!         },
//!     );
//!
//!     // Provide the pipeline with initial data, and then wait on the pipeline to complete.
//!     for value in ["a", "bb", "ccc"] {
//!         map_input_w.write(value.to_string()).await;
//!     }
//!     pipeline.wait().await;
//!
//!     // We see that after the data goes through our map and reduce stages,
//!     // we effectively get this: `len("a!") + len("bb!") + len("ccc!") = 9`
//!     assert_eq!(*total_count.lock().await, 9);
//! }
//! ```
//!

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::panic;
use std::sync::Arc;

use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::{JoinError, JoinSet};

pub use io::*;

use crate::sync::Synchronizer;

mod io;
pub(crate) mod sync;

/// Signals sent to stage workers.
///
/// Useful for interrupting the natural workflow to tell it something.
enum StageWorkerSignal {
    /// For now, this is a placeholder signal and marked as dead code.
    ///
    /// This is to avoid a compiler error in [Pipeline::new_stage_worker] where the loop
    /// complains about never having another iteration due to all other branches breaking
    /// within the `select!`.
    #[allow(dead_code)]
    Ping,

    /// Used to tell stage workers to finish immediately without waiting for remaining tasks to end.
    Terminate,
}

/// Simple data structure to hold information about a stage worker.
struct StageWorker<
    I: Send + 'static,
    O: Send + 'static,
    F: FnOnce(I) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Vec<Option<O>>> + Send + 'static,
> {
    name: String,
    signal_rx: Receiver<StageWorkerSignal>,
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
/// // `pipes` is mutable so it can give ownership of values to the I/O objects produced in `Pipes::create_io`
/// let (_pipeline, mut pipes): (Pipeline, Pipes) = Pipeline::from_pipes(vec!["a", "b", "c"]);
///
/// let pipe_io = pipes.create_io::<String>("a");
/// assert!(pipe_io.is_some());
///
/// // A tuple is returned containing the I/O objects
/// let (w, r): (PipeWriter<String>, PipeReader<String>) = pipe_io.unwrap();
///
/// // `None` is returned if the pipe name doesn't exist
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
    stage_workers: JoinSet<()>,
    signal_txs: HashMap<String, Sender<StageWorkerSignal>>,
}

impl Pipeline {
    /// Create a new pipeline using the list of pipe names.
    ///
    /// Using the names, an instance of [Pipes] will also be created which can be used to create
    /// the I/O objects for each pipe. These objects can be given to [Pipeline::register_stage] calls
    /// which tell the pipeline to connect that pipe to the stage worker being registered.
    ///
    /// Providing no names will result in a pipeline that can't transfer any data.
    ///
    /// # Examples
    ///
    /// TODO
    ///
    pub fn from_pipes<S: AsRef<str>>(names: Vec<S>) -> (Self, Pipes) {
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
            let name = name.as_ref().to_string();
            let id = format!("{}-{}", &name, uuid::Uuid::new_v4());

            sync.register(&id);
            names_to_ids.insert(name, id);
        }

        let sync = Arc::new(sync);
        (
            Self {
                synchronizer: sync.clone(),
                stage_workers: JoinSet::default(),
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
    /// Once all workers are done running, send the [StageWorkerSignal::TERMINATE] signal to all the workers.
    pub async fn wait(mut self) {
        let workers = &mut self.stage_workers;

        let check_workers_completed = async {
            while let Some(result) = workers.join_next().await {
                Self::check_join_result(&result);
            }
        };
        let check_sync_completed = async {
            while !self.synchronizer.completed().await {}

            for tx in self.signal_txs.values() {
                tx.send(StageWorkerSignal::Terminate)
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

    /// Register a new stage in the pipeline.
    ///
    /// This will create a "stage worker" that will create asynchronous tasks for each input received from
    /// the connected pipe reader(s).
    pub fn register_stage<I, O, F, Fut>(
        &mut self,
        name: impl Into<String>,
        inputs: PipeReader<I>,
        outputs: Vec<PipeWriter<O>>,
        task_definition: F,
    ) where
        I: Send + 'static,
        O: Send + 'static,
        F: FnOnce(I) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Vec<Option<O>>> + Send + 'static,
    {
        let name = name.into();

        let (signal_tx, signal_rx) = channel(1);
        self.signal_txs.insert(name.clone(), signal_tx);

        self.stage_workers
            .spawn(Self::new_stage_worker(StageWorker {
                name,
                signal_rx,
                inputs,
                outputs,
                task_definition,
            }));
    }

    async fn new_stage_worker<I, O, F, Fut>(mut worker: StageWorker<I, O, F, Fut>)
    where
        I: Send + 'static,
        O: Send + 'static,
        F: FnOnce(I) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Vec<Option<O>>> + Send + 'static,
    {
        // Individual tasks asynchronously operating on received input
        let StageWorker {
            name,
            signal_rx,
            inputs,
            outputs,
            task_definition,
        } = &mut worker;

        let mut tasks = JoinSet::new();
        loop {
            // Reference to avoid moving into closures below
            select! {
                // Start SWL
                Some(input) = inputs.read() => {
                    let consumed = inputs.consumed_callback();
                    let outputs = outputs.clone();
                    let task = task_definition.clone();

                    tasks.spawn(async move {
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
                        consumed().await;
                    });
                }

                // Join tasks to check for errors
                Some(result) = tasks.join_next() => {
                    Self::check_join_result(&result);
                }

                // Receive external signals
                Some(signal) = signal_rx.recv() => {
                    match signal {
                        StageWorkerSignal::Terminate => break,
                        StageWorkerSignal::Ping => println!("Responding from {}", name),
                    }
                },
            }
        }

        tasks.shutdown().await;
    }

    fn check_join_result<T>(result: &Result<T, JoinError>) {
        if let Err(e) = result {
            if e.is_panic() {
                // TODO - figure out to get `select!` to NOT provide a borrowed result
                // panic::resume_unwind(e.into_panic())
                panic!("task panicked!")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::join;
    use tokio::sync::Mutex;

    use crate::{PipeWriter, Pipeline, StageWorkerSignal};

    #[test]
    fn test_pipeline_from_no_pipes_succeeds() {
        let (_, pipes) = Pipeline::from_pipes(Vec::<String>::new());

        assert_eq!(pipes.names_to_ids.len(), 0);
    }

    #[tokio::test]
    async fn test_pipeline_from_pipes_succeeds() {
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
            assert_eq!(pipes.synchronizer.get(id).await, 0);
        }
    }

    #[test]
    fn test_pipes_create_io_returns_correctly() {
        let names = vec!["start", "middle", "end"];

        let (_, mut pipes) = Pipeline::from_pipes(names);

        assert!(pipes.create_io::<()>("undefined").is_none());
        assert!(pipes.create_io::<()>("start").is_some());
    }

    #[tokio::test]
    #[should_panic]
    async fn test_stage_worker_propagates_task_panic() {
        let (mut pipeline, mut pipes) = Pipeline::from_pipes(vec!["pipe"]);
        let (w, r) = pipes.create_io("pipe").unwrap();

        pipeline.register_stage(
            "test-stage",
            r,
            Vec::<PipeWriter<Option<()>>>::new(),
            |_: ()| async move { panic!("AHH!") },
        );
        w.write(()).await;
        pipeline.wait().await;
    }

    #[tokio::test]
    async fn test_stage_worker_signal_terminate() {
        let (mut pipeline, mut pipes) = Pipeline::from_pipes(vec!["pipe"]);
        let (w, r) = pipes.create_io("pipe").unwrap();

        pipeline.register_stage(
            "test-stage",
            r,
            Vec::<PipeWriter<Option<()>>>::new(),
            |_: ()| async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                panic!("worker did not terminate!");
            },
        );
        w.write(()).await;

        let signaller = pipeline.signal_txs["test-stage"].clone();
        let _ = join!(
            pipeline.wait(),
            signaller.send(StageWorkerSignal::Terminate),
        );
    }
}

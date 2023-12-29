//!
//! Async Pipes provides a simple way to create high-throughput, in-memory pipelines for data processing.
//!
//! ```
//! use std::sync::Arc;
//! use tokio::sync::Mutex;
//! use async_pipes::Pipeline;
//! use async_pipes::atomic_mut;
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
//!     pipeline.register_inputs("Producer", map_input_w, vec!["a", "bb", "ccc"]);
//!
//!     // We return an option to tell the stage whether to write `new_value` to the pipe or ignore it.
//!     pipeline.register("MapStage", map_input_r, map_to_reduce_w, |value: &'static str| async move {
//!         let new_value = format!("{}!", value);
//!         Some(new_value)
//!     });
//!
//!     // Variables can be updated in a task by wrapping it in a `Mutex` (to make it mutable)
//!     // and then in an `Arc` (for data ownership across task executions).
//!     let total_count = atomic_mut(0);
//!     let reduce_total_count = total_count.clone();
//!     pipeline.register_consumer("ReduceStage", map_to_reduce_r, |value: String| async move {
//!         *reduce_total_count.lock().await += value.len();
//!     });
//!
//!     pipeline.wait().await;
//!
//!     // We see that after the data goes through our map and reduce stages,
//!     // we effectively get this: `len("a!") + len("bb!") + len("ccc!") = 9`
//!     assert_eq!(*total_count.lock().await, 9);
//! }
//! ```
//!
#![warn(missing_docs)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::panic;
use std::sync::Arc;

use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};

pub use io::*;
pub use util::*;

use crate::sync::Synchronizer;

mod io;
pub(crate) mod sync;
mod util;

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
    reader: PipeReader<I>,
    writers: Vec<PipeWriter<O>>,
    task_definition: F,
}

/// Provided by [Pipeline::from_pipes], used to create the I/O objects for each end of a pipe.
///
/// # Examples
///
/// Create three pipes and demonstrate how to acquire the I/O objects of a pipe by name.
///
/// ```
/// use async_pipes::{Pipeline, PipeReader, Pipes, PipeWriter};
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
/// # Examples
///
/// Creating a single producer and a single consumer.
/// ```
/// use async_pipes::atomic_mut;
///
/// tokio_test::block_on(async {
///     use async_pipes::{atomic_mut_cloned, Pipeline, PipeReader, Pipes, PipeWriter};
///
///     let (mut pipeline, mut pipes): (Pipeline, Pipes) = Pipeline::from_pipes(vec!["pipe"]);
///     let (writer, reader) = pipes.create_io::<usize>("pipe").unwrap();
///
///     // Produce values 1 through 10
///     let count = atomic_mut(0);
///     pipeline.register_producer("producer", writer, || async move {
///         let mut count = count.lock().await;
///         if *count < 10 {
///             *count += 1;
///             Some(*count)
///         } else {
///             None
///         }
///     });
///
///     let (sum, wk_sum) = atomic_mut_cloned(0);
///     pipeline.register_consumer("consumer", reader, |n: usize| async move {
///         *wk_sum.lock().await += n;
///     });
///
///     pipeline.wait().await;
///
///     assert_eq!(*sum.lock().await, 55);
/// });
/// ```
///
/// Creating a branching producer and two consumers for each branch.
/// ```
/// use async_pipes::atomic_mut;
///
/// tokio_test::block_on(async {
///     use async_pipes::{atomic_mut_cloned, Pipeline , Pipes };
///
///     let (mut pipeline, mut pipes): (Pipeline, Pipes) = Pipeline::from_pipes(vec!["evens", "odds"]);
///     let (evens_w, evens_r) = pipes.create_io::<usize>("evens").unwrap();
///     let (odds_w, odds_r) = pipes.create_io::<usize>("odds").unwrap();
///
///     let count = atomic_mut(0);
///     pipeline.register_branching_producer("producer", vec![evens_w, odds_w], || async move {
///         let mut count = count.lock().await;
///         if *count >= 10 {
///             return None;
///         }
///         *count += 1;
///
///         let output = if *count % 2 == 0 {
///             vec![Some(*count), None]
///         } else {
///             vec![None, Some(*count)]
///         };
///
///         Some(output)
///     });
///
///     let (odds_sum, wk_odds_sum) = atomic_mut_cloned(0);
///     pipeline.register_consumer("odds-consumer", odds_r, |n: usize| async move {
///         *wk_odds_sum.lock().await += n;
///     });
///     let (evens_sum, wk_evens_sum) = atomic_mut_cloned(0);
///     pipeline.register_consumer("evens-consumer", evens_r, |n: usize| async move {
///         *wk_evens_sum.lock().await += n;
///     });
///
///     pipeline.wait().await;
///
///     assert_eq!(*odds_sum.lock().await, 25);
///     assert_eq!(*evens_sum.lock().await, 30);
/// });
/// ```
#[derive(Debug)]
pub struct Pipeline {
    synchronizer: Arc<Synchronizer>,
    producers: JoinSet<()>,
    stage_workers: JoinSet<()>,
    signal_txs: HashMap<String, Sender<StageWorkerSignal>>,
}

impl Pipeline {
    /// Create a new pipeline using the list of pipe names.
    ///
    /// Using the names, an instance of [Pipes] will also be created which can be used to create
    /// the I/O objects for each pipe. These objects can be given to [Pipeline::register_branching] calls
    /// which tell the pipeline to connect that pipe to the stage worker being registered.
    ///
    /// Providing no names will result in a pipeline that can't transfer any data.
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
                producers: JoinSet::default(),
                signal_txs: HashMap::new(),
            },
            Pipes {
                synchronizer: sync,
                names_to_ids,
            },
        )
    }

    /// Wait for the pipeline to complete.
    ///
    /// Once the pipeline is complete, a [StageWorkerSignal::TERMINATE] is sent to to all the workers.
    ///
    /// A pipeline progresses to completion by doing the following:
    ///   1. Wait for all "producers" to complete while also progressing stage workers
    ///   2. Wait for either all the stage workers to complete, or wait for the internal synchronizer to notify
    ///      of completion (i.e. there's no more data flowing through the pipeline)
    ///
    /// Step 1 implies that if the producers never finish, the pipeline will run forever. See
    /// [Pipeline::register_producer] for more info.
    pub async fn wait(mut self) {
        let (workers_to_progress, workers_to_finish) = atomic_mut_cloned(self.stage_workers);

        let wait_for_producers = async {
            while let Some(result) = self.producers.join_next().await {
                Self::check_join_result(&result);
            }
        };
        let wait_for_workers = |workers: Arc<Mutex<JoinSet<()>>>| async move {
            while let Some(result) = workers.lock().await.join_next().await {
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

        // Effectively make progress until all producers are done.
        // We do this to prevent the synchronizer from causing the pipeline to shutdown too early.
        select! {
            _ = wait_for_producers => {},
            _ = wait_for_workers(workers_to_progress), if !workers_to_progress.lock().await.is_empty() => {},
        }

        // If either the synchronizer determines we're done, or all workers completed, we're done
        select! {
            _ = wait_for_workers(workers_to_finish) => {},
            _ = check_sync_completed => {},
        }
    }

    /// Register a set of inputs to be written to a provided pipe.
    ///
    /// This effectively creates a producer stage internally, looping over each input and writing it to
    /// the pipe.
    ///
    /// # Arguments
    ///
    /// * `name` - For debugging purposes; provide an identifying string for this stage.
    /// * `writer` - Created by [Pipes::create_io], where each input is written to.
    /// * `inputs` - A list of inputs to write to the pipe.
    ///
    pub fn register_inputs<O: Send + 'static>(
        &mut self,
        name: impl Into<String>,
        writer: PipeWriter<O>,
        inputs: Vec<O>,
    ) {
        let iter = atomic_mut(inputs.into_iter());
        self.register_branching_producer(name, vec![writer], || async move {
            iter.lock().await.next().map(|input| vec![Some(input)])
        });
    }

    /// Register a set of inputs to be written to a provided pipe.
    ///
    /// This effectively creates a producer stage internally, looping over each input and writing it to
    /// the pipe.
    ///
    /// # Arguments
    ///
    /// * `name` - For debugging purposes; provide an identifying string for this stage.
    /// * `writer` - Created by [Pipes::create_io], where each input is written to.
    /// * `inputs` - A list of inputs to write to the pipe.
    ///
    pub fn register_branching_inputs<O: Send + 'static>(
        &mut self,
        name: impl Into<String>,
        writers: Vec<PipeWriter<O>>,
        inputs: Vec<Vec<O>>,
    ) {
        let iter = atomic_mut(inputs.into_iter());
        self.register_branching_producer(name, writers, || async move {
            iter.lock()
                .await
                .next()
                .map(|is| is.into_iter().map(Some).collect())
        });
    }

    /// Register a new stage in the pipeline that produces values and writes them into a pipe.
    ///
    /// The producer will continue producing values until the user-provided task function returns `None`.
    /// This means that it is possible to create an infinite stream of values by simply never returning `None`.
    ///
    /// # Arguments
    ///
    /// * `name` - For debugging purposes; provide an identifying string for this stage.
    /// * `writer` - Created by [Pipes::create_io], where produced output is written to.
    /// * `task_definition` - An async function that when executed produces a single output value.
    ///
    /// If the output value returned by the task definition is `Some(...)`, it is written to
    /// the provided pipe. Otherwise, if the output value is `None`, the producer terminates.
    pub fn register_producer<O, F, Fut>(
        &mut self,
        name: impl Into<String>,
        writer: PipeWriter<O>,
        task_definition: F,
    ) where
        O: Send + 'static,
        F: FnOnce() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<O>> + Send + 'static,
    {
        self.register_branching_producer(name, vec![writer], || async move {
            task_definition().await.map(|t| vec![Some(t)])
        });
    }

    /// Register a new stage in the pipeline that produces multiple values and writes them into their
    /// respective pipe.
    ///
    /// The producer will continue producing values until the user-provided task function returns `None`.
    /// This means that it is possible to create an infinite stream of values by simply never returning `None`.
    ///
    /// # Arguments
    ///
    /// * `name` - for debugging purposes; provide an identifying string for this stage.
    /// * `writers` - created by [Pipes::create_io], where produced outputs are written to.
    ///   The position of each output in the returned vector maps directly to the position of the writer
    ///   in the `writers` vector provided.
    /// * `task_definition` - an async function that when executed produces a list of output values.
    ///
    /// If the output returned by the task definition is `Some(vec![...])` each value in the vector is
    /// written to the respective pipe. Otherwise, if the output value is `None`, the producer terminates.
    pub fn register_branching_producer<O, F, Fut>(
        &mut self,
        _name: impl Into<String>,
        writers: Vec<PipeWriter<O>>,
        task_definition: F,
    ) where
        O: Send + 'static,
        F: FnOnce() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<Vec<Option<O>>>> + Send + 'static,
    {
        self.producers.spawn(async move {
            loop {
                let task = task_definition.clone();
                let writers = writers.clone();

                if let Some(results) = task().await {
                    Self::write_results(writers, results).await;
                } else {
                    break;
                }
            }
        });
    }

    /// Register a new stage in the pipeline that operates on incoming data and writes the result to a single output
    /// pipe.
    ///
    /// This effectively provides a means to do a "mapping" transformation on incoming data, with an additional
    /// capability to filter it by returning `None` in the task definition.
    ///
    /// # Arguments
    ///
    /// * `name` - for debugging purposes; provide an identifying string for this stage.
    /// * `reader` - Created by [Pipes::create_io], where incoming data is read from.
    /// * `writer` - Created by [Pipes::create_io], where the task's output is written to.
    /// * `task_definition` - An async function that operates on input received from the `reader` and returns
    ///   an output that is written to the `writer`.
    ///
    /// If the output returned by the task definition is `Some(...)`, that value will be written.
    /// Otherwise, if the output value is `None`, that value will not be written.
    pub fn register<I, O, F, Fut>(
        &mut self,
        name: impl Into<String>,
        reader: PipeReader<I>,
        writer: PipeWriter<O>,
        task_definition: F,
    ) where
        I: Send + 'static,
        O: Send + 'static,
        F: FnOnce(I) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Option<O>> + Send + 'static,
    {
        self.register_branching(name, reader, vec![writer], |value: I| async move {
            vec![task_definition(value).await]
        });
    }

    /// Register a new stage in the pipeline that operates on reads incoming data and writes the results to its
    /// respective output pipe.
    ///
    /// This effectively provides a means to do a "mapping" transformation on incoming data, with an additional
    /// capability to filter it by returning `None` in the task definition.
    ///
    /// # Arguments
    ///
    /// * `name` - for debugging purposes; provide an identifying string for this stage.
    /// * `reader` - Created by [Pipes::create_io], where incoming data is read from.
    /// * `writer` - Created by [Pipes::create_io], where the task's output is written to.
    ///   The position of each output in the returned vector maps directly to the position of the writer
    ///   in the `writers` vector provided.
    /// * `task_definition` - An async function that operates on input received from the `reader` and returns
    ///   a list of outputs where each is written to its respective `writer`.
    ///
    /// For each output, if the task definition returns `Some(...)`, that value will be written.
    /// Otherwise, if it is `None`, that value will not be written.
    pub fn register_branching<I, O, F, Fut>(
        &mut self,
        name: impl Into<String>,
        reader: PipeReader<I>,
        writers: Vec<PipeWriter<O>>,
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
                reader,
                writers,
                task_definition,
            }));
    }

    /// Register a new stage in the pipeline that consumes incoming data from a pipe.
    ///
    /// This acts as a "leaf" stage where the data flowing through the pipeline stops flowing at.
    ///
    /// # Arguments
    ///
    /// * `name` - for debugging purposes; provide an identifying string for this stage.
    /// * `reader` - Created by [Pipes::create_io], where incoming data is read from.
    /// * `writer` - Created by [Pipes::create_io], where the task's output is written to.
    /// * `task_definition` - An async function that operates on input received from the `reader` and returns
    ///   an output that is written to the `writer`.
    ///
    /// If the output returned by the task definition is `Some(...)`, that value will be written.
    /// Otherwise, if the output value is `None`, that value will not be written.
    pub fn register_consumer<I, F, Fut>(
        &mut self,
        name: impl Into<String>,
        reader: PipeReader<I>,
        task_definition: F,
    ) where
        I: Send + 'static,
        F: FnOnce(I) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.register_branching(name, reader, vec![], |value: I| async move {
            task_definition(value).await;
            Vec::<Option<()>>::new()
        });
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
            reader,
            writers,
            task_definition,
        } = &mut worker;

        let mut tasks = JoinSet::new();
        loop {
            // Reference to avoid moving into closures below
            select! {
                // Start SWL
                Some(value) = reader.read() => {
                    let consumed = reader.consumed_callback();
                    let writers = writers.clone();
                    let task = task_definition.clone();

                    tasks.spawn(async move {
                        Self::write_results(writers, task(value).await).await;

                        // Mark input as consumed *after* writing outputs
                        // (avoids false positive of completed pipeline)
                        consumed().await;
                    });
                }

                // Join tasks to check for errors
                Some(result) = tasks.join_next(), if !tasks.is_empty() => {
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

    async fn write_results<O>(writers: Vec<PipeWriter<O>>, results: Vec<Option<O>>) {
        for (i, value) in results.into_iter().enumerate() {
            if let Some(result) = value {
                writers
                    .get(i)
                    .expect("len(results) != len(writers)")
                    .write(result)
                    .await;
            }
        }
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

    use crate::{atomic_mut_cloned, Pipeline, StageWorkerSignal};

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
    async fn test_stage_worker_producer() {
        let value = Some("hello!".to_string());
        let wk_value = value.clone();

        let (mut pipeline, mut pipes) = Pipeline::from_pipes(vec!["pipe"]);
        let (w, mut r) = pipes.create_io("pipe").unwrap();

        let (written, wk_written) = atomic_mut_cloned(false);
        pipeline.register_producer("test-stage", w, || async move {
            let mut wk_written = wk_written.lock().await;
            if !*wk_written {
                *wk_written = true;
                wk_value
            } else {
                None
            }
        });

        pipeline.wait().await;

        assert!(*written.lock().await, "value was not handled by worker!");
        assert_eq!(r.read().await, value);
    }

    #[tokio::test]
    async fn test_stage_worker_consumer() {
        let (mut pipeline, mut pipes) = Pipeline::from_pipes(vec!["pipe"]);
        let (w, r) = pipes.create_io("pipe").unwrap();

        let (written, wk_written) = atomic_mut_cloned(false);
        pipeline.register_consumer("test-stage", r, |n: usize| async move {
            assert_eq!(n, 3);
            *wk_written.lock().await = true;
        });

        w.write(3).await;
        pipeline.wait().await;

        assert!(*written.lock().await, "value was not handled by worker!");
    }

    #[tokio::test]
    async fn test_stage_worker_single_output() {
        let (mut pipeline, mut pipes) = Pipeline::from_pipes(vec!["first", "second"]);
        let (first_w, first_r) = pipes.create_io("first").unwrap();
        let (second_w, second_r) = pipes.create_io("second").unwrap();

        let (written, wk_written) = atomic_mut_cloned(false);
        pipeline.register("test-stage", first_r, second_w, |n: usize| async move {
            Some(n + 1)
        });
        pipeline.register_consumer("test-stage", second_r, |n: usize| async move {
            assert_eq!(n, 2);
            *wk_written.lock().await = true;
        });

        first_w.write(1).await;
        pipeline.wait().await;

        assert!(*written.lock().await, "value was not handled by worker!");
    }

    #[tokio::test]
    #[should_panic]
    async fn test_stage_worker_propagates_task_panic() {
        let (mut pipeline, mut pipes) = Pipeline::from_pipes(vec!["pipe"]);
        let (w, r) = pipes.create_io("pipe").unwrap();

        pipeline.register_consumer("test-stage", r, |_: ()| async move { panic!("AHH!") });
        w.write(()).await;
        pipeline.wait().await;
    }

    #[tokio::test]
    async fn test_stage_worker_signal_terminate() {
        let (mut pipeline, mut pipes) = Pipeline::from_pipes(vec!["pipe"]);
        let (w, r) = pipes.create_io("pipe").unwrap();

        pipeline.register_consumer("test-stage", r, |_: ()| async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            panic!("worker did not terminate!");
        });
        w.write(()).await;

        let signaller = pipeline.signal_txs["test-stage"].clone();
        let _ = join!(
            pipeline.wait(),
            signaller.send(StageWorkerSignal::Terminate),
        );
    }
}

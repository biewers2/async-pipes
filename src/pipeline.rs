use std::any::{type_name, Any};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;

use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::{yield_now, JoinError, JoinSet};

use crate::io::{PipeReader, PipeWriter};
use crate::sync::Synchronizer;
use crate::{new_id, StageWorkerSignal};

/// A Box that can hold any value ([Any]) that is also [Send].
///
/// Values sent through pipes are trait objects of this type.
///
/// This type is publicly exposed as it's needed when building a pipeline stage with multiple
/// outputs. Since each output could have a different type, it's more feasible to define the
/// outputs to use dynamic dispatching rather that static dispatching.
///
/// # Examples
///
/// Here's an example of a closure representing the task function given to the pipeline builder
/// when creating a "branching" stage. Three outputs are returned, each of a different type.
/// ```
/// use std::ops::Deref;
/// use async_pipes::BoxedAnySend;
///
/// tokio_test::block_on(async {
///     let task = |value: String| async move {
///         let length: usize = value.len();
///         let excited: String = format!("{}!", value);
///         let odd_length: bool = length % 2 == 1;
///
///         Option::<Vec<Option<BoxedAnySend>>>::Some(vec![
///             Some(Box::new(length)),
///             Some(Box::new(excited)),
///             Some(Box::new(odd_length)),
///         ])
///     };
///
///     // E.g.:
///     // ...
///     // .with_branching_stage("pipe_in", vec!["pipe_len", "pipe_excited", "pipe_odd"], <task>)
///     // ...
///
///     let mut results = task("hello".to_string()).await.unwrap();
///
///     let length = results.remove(0).unwrap().downcast::<usize>().unwrap();
///     let excited = results.remove(0).unwrap().downcast::<String>().unwrap();
///     let odd_length = results.remove(0).unwrap().downcast::<bool>().unwrap();
///
///     assert_eq!(*length, 5usize);
///     assert_eq!(*excited, "hello!".to_string());
///     assert_eq!(*odd_length, true);
/// });
/// ```
pub type BoxedAnySend = Box<dyn Any + Send + 'static>;

type TaskFuture = Pin<Box<dyn Future<Output = Option<Vec<TaskFutureOutput>>> + Send + 'static>>;
type TaskFutureOutput = Option<BoxedAnySend>;

type ProducerFn = Box<dyn FnMut() -> TaskFuture + Send + 'static>;
type TaskFn = Box<dyn Fn(BoxedAnySend) -> TaskFuture + Send + Sync + 'static>;

struct ProducerStage {
    function: ProducerFn,
    pipes: ProducerPipeNames,
}

struct ProducerPipeNames {
    writers: Vec<String>,
}

struct TaskStage {
    function: TaskFn,
    pipes: TaskPipeNames,
}

struct TaskPipeNames {
    reader: String,
    writers: Vec<String>,
}

struct Pipe<T> {
    writer: PipeWriter<T>,
    /// Use an option here to "take" it when a reader is used.
    /// Only allow one reader per stage.
    reader: Option<PipeReader<T>>,
}

/// Used to construct a [Pipeline].
///
/// Can be created using [Pipeline::builder].
///
///
#[derive(Default)]
pub struct PipelineBuilder {
    producer_fns: Vec<ProducerStage>,
    task_fns: Vec<TaskStage>,
}

impl PipelineBuilder {
    /// A "producer" stage; registers a list of inputs to be written to a provided pipe.
    ///
    /// The string provided to `pipe` defines where the values will be written to.
    /// The values will be written one at a time into the pipe.
    ///
    /// # Returns
    ///
    /// This pipeline builder.
    ///
    pub fn with_inputs<S, I>(self, pipe: S, inputs: Vec<I>) -> Self
    where
        S: AsRef<str>,
        I: Send + 'static,
    {
        self.with_branching_inputs(
            vec![pipe],
            inputs
                .into_iter()
                .map(|i| vec![Box::new(i) as BoxedAnySend])
                .collect(),
        )
    }

    /// A "producer" stage; registers a list of multiple inputs to be written to a list of corresponding pipes.
    ///
    /// The strings provided to `pipes` define where each input will go.
    /// The values will be written one at a time into each pipe.
    ///
    /// For example, say you have the following:
    ///
    /// `List of multiple inputs: [ [1, "hi", true], [2, "bye", false], [3, ".", false] ]`
    /// `List of pipes: [ "numbers", "strings", "bools" ]`
    ///
    /// The inputs would be sent to the pipes like this:
    ///
    /// `Pipe         1st   2nd    3rd`
    /// `"numbers" <- 1     2      3`
    /// `"strings" <- "hi"  "bye"  "."`
    /// `"bools"   <- true  false  false`
    ///
    /// # Returns
    ///
    /// This pipeline builder.
    ///
    pub fn with_branching_inputs<S>(self, pipes: Vec<S>, inputs: Vec<Vec<BoxedAnySend>>) -> Self
    where
        S: AsRef<str>,
    {
        let mut iter = inputs.into_iter();
        self.with_branching_producer(pipes, move || {
            let inputs = iter.next();
            async move { inputs.map(|is| is.into_iter().map(Some).collect()) }
        })
    }

    /// A "producer" stage; registers a stage that produces values and writes them into a pipe.
    ///
    /// The strings provided to `pipes` define where each input will go.
    ///
    /// The producer will continue producing values while the user-provided task function returns [Some].
    /// This means that it is possible to create an infinite stream of values by simply never returning [None].
    ///
    /// # Returns
    ///
    /// This pipeline builder.
    ///
    pub fn with_producer<S, I, F, Fut>(self, pipe: S, mut task: F) -> Self
    where
        S: AsRef<str>,
        I: Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Option<I>> + Send + 'static,
    {
        self.with_branching_producer(vec![pipe], move || {
            let task_fut = task();
            async move {
                task_fut
                    .await
                    .map(|t| vec![Some(Box::new(t) as BoxedAnySend)])
            }
        })
    }

    /// A "producer" stage; registers a new stage that produces multiple values and writes them into their
    /// respective pipe.
    ///
    /// The strings provided to `pipes` define where each input will go.
    ///
    /// The producer will continue producing values while the user-provided task function returns [Some].
    /// This means that it is possible to create an infinite stream of values by simply never returning [None].
    ///
    /// Each individual [Option] within the task output determines whether it will be sent to the corresponding
    /// pipe. If [Some] is specified, the inner value will be sent, if [None] is specified, nothing will be sent.
    ///
    /// As with all stages that have more than one ("branching") outputs, it's possible that each output could
    /// have a different type, and so to avoid large binary sizes from static dispatching, dynamic dispatching
    /// is used instead, utilizing the [BoxedAnySend] type. For examples on how to return these types of
    /// values in task functions, see [BoxedAnySend]'s examples.
    ///
    /// # Returns
    ///
    /// This pipeline builder.
    ///
    pub fn with_branching_producer<S, F, Fut>(mut self, pipes: Vec<S>, mut task: F) -> Self
    where
        S: AsRef<str>,
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Option<Vec<Option<BoxedAnySend>>>> + Send + 'static,
    {
        let pipes = pipes.iter().map(|p| p.as_ref().to_string()).collect();
        self.producer_fns.push(ProducerStage {
            function: Box::new(move || Box::pin(task())),
            pipes: ProducerPipeNames { writers: pipes },
        });
        self
    }

    /// A "consumer" stage; registers a new stage that consumes values from a pipe.
    ///
    /// The string provided to `pipe` define where values will be read from.
    ///
    /// The consumer will continue consuming values until the pipeline is terminated or the pipe it is
    /// receiving from is closed.
    ///
    /// # Returns
    ///
    /// This pipeline builder.
    ///
    pub fn with_consumer<S, I, F, Fut>(self, pipe: S, task: F) -> Self
    where
        S: AsRef<str>,
        I: Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.with_branching_stage(pipe, vec![], move |value| {
            let task_fut = task(value);
            async move {
                task_fut.await;
                Some(Vec::<Option<BoxedAnySend>>::new())
            }
        })
    }

    /// A "regular" stage; registers a new stage that operates on an input and produce a single output value
    /// that is written to a pipe.
    ///
    /// The string provided to `input_pipe` defines where values will be read from.
    /// The string provided to `output_pipe` defines where the produced output will go.
    ///
    /// The worker will continue working on input values until the pipeline is terminated or the pipe it is
    /// receiving from is closed.
    ///
    /// The [Option] returned by the task function determines whether it will be sent to the output pipe.
    /// If [Some] is specified, the inner value will be sent, if [None] is specified, nothing will be sent.
    ///
    /// # Returns
    ///
    /// This pipeline builder.
    ///
    pub fn with_stage<S, I, O, F, Fut>(self, input_pipe: S, output_pipe: S, task: F) -> Self
    where
        S: AsRef<str>,
        I: Send + 'static,
        O: Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<O>> + Send + 'static,
    {
        self.with_branching_stage(input_pipe, vec![output_pipe], move |value| {
            let task_fut = task(value);
            async move {
                task_fut
                    .await
                    .map(|v| Box::new(v) as BoxedAnySend)
                    .map(|v| vec![Some(v)])
            }
        })
    }

    /// A "regular" stage; registers a new stage that operates on an input and produces multiple values that are
    /// written into their respective pipe.
    ///
    /// The string provided to `input_pipe` defines where values will be read from.
    /// The strings provided to `output_pipes` define where each produced output will go.
    ///
    /// The worker will continue working on input values until the pipeline is terminated or the pipe it is
    /// receiving from is closed.
    ///
    /// * If the user-defined task function returns [None], nothing will be done.
    /// * If it returns [Some], the inner value ([Vec<Option<BoxedAnySend>>]) will have the following applied
    ///   to each output option:
    ///     * If [Some] is specified, the inner value will be sent to the corresponding pipe.
    ///     * If [None] is specified, nothing will be sent.
    ///
    /// As with all stages that have more than one ("branching") outputs, it's possible that each output could
    /// have a different type, and so to avoid large binary sizes from static dispatching, dynamic dispatching
    /// is used instead, utilizing the [BoxedAnySend] type. For examples on how to return these types of
    /// values in task functions, see [BoxedAnySend]'s examples.
    ///
    /// # Returns
    ///
    /// This pipeline builder.
    ///
    pub fn with_branching_stage<S, I, F, Fut>(
        mut self,
        input_pipe: S,
        output_pipes: Vec<S>,
        task: F,
    ) -> Self
    where
        S: AsRef<str>,
        I: Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<Vec<Option<BoxedAnySend>>>> + Send + 'static,
    {
        let input_pipe = input_pipe.as_ref().to_string();
        let output_pipes = output_pipes
            .iter()
            .map(|p| p.as_ref().to_string())
            .collect();
        let err_pipe = input_pipe.clone();

        self.task_fns.push(TaskStage {
            function: Box::new(move |value: BoxedAnySend| {
                let v = value.downcast::<I>().unwrap_or_else(|_| {
                    panic!(
                        "failed to downcast input value to {} from pipe '{}'",
                        type_name::<I>(),
                        err_pipe
                    )
                });
                Box::pin(task(*v))
            }),
            pipes: TaskPipeNames {
                reader: input_pipe,
                writers: output_pipes,
            },
        });
        self
    }

    /// When the pipeline is ready to be built, this is called and will return a pipeline if
    /// it was successfully built, otherwise it will return an error describing why it could not be built.
    ///
    /// # Errors
    ///
    /// 1. A pipe was specified as an "output" pipe for a stage and it doesn't exist
    /// 2. The reader of a pipe was used more than once
    ///
    pub fn build(self) -> Result<Pipeline, String> {
        // Create pipe IDs and register them in the synchronizer
        let mut synchronizer = Synchronizer::default();
        let ids = self.create_ids(&mut synchronizer);

        // Make the synchronizer immutable and the create the actual pipes
        let synchronizer = Arc::new(synchronizer);
        let mut pipes = self.create_pipes(&synchronizer, ids);

        let mut producers = JoinSet::new();
        let mut workers = JoinSet::new();
        let mut signal_txs = Vec::new();

        // Spawn producer workers
        for producer in self.producer_fns {
            let writers = Self::find_writers(&producer.pipes.writers, &pipes)?;
            producers.spawn(Self::new_detached_producer(producer.function, writers));
        }

        // Spawn task workers
        for task in self.task_fns {
            let writers = Self::find_writers(&task.pipes.writers, &pipes)?;

            let reader_name = &task.pipes.reader;
            let reader = Self::find_reader(reader_name, &mut pipes)?;

            let (signal_tx, signal_rx) = channel(1);
            signal_txs.push(signal_tx);

            workers.spawn(Self::new_detached_worker(
                task.function,
                reader,
                writers,
                signal_rx,
            ));
        }

        Ok(Pipeline {
            synchronizer,
            producers,
            workers,
            signal_txs,
        })
    }

    /// IDs are generated from the list of names of all the pipe readers in the task functions.
    ///
    /// This is because for each pipe, there is only reader, but there can be multiple writers.
    /// Also, producers do not have a reader, only writers, so we only take from the task functions.
    fn create_ids(&self, sync: &mut Synchronizer) -> HashMap<String, String> {
        let mut ids = HashMap::new();
        for name in self.names() {
            let id = new_id();
            sync.register(&id);
            ids.insert(name.clone(), id);
        }
        ids
    }

    /// The reader of a pipe is wrapped in an option to allow the build method to [Option::take] it
    /// to maintain the invariant that there's only one reader per pipe.
    fn create_pipes(
        &self,
        sync: &Arc<Synchronizer>,
        ids: HashMap<String, String>,
    ) -> HashMap<String, Pipe<BoxedAnySend>> {
        let mut pipes = HashMap::new();
        for (name, id) in ids {
            let (tx, rx) = channel(10);
            pipes.insert(
                name,
                Pipe {
                    writer: PipeWriter::new(&id, sync.clone(), tx),
                    reader: Some(PipeReader::new(&id, sync.clone(), rx)),
                },
            );
        }
        pipes
    }

    fn names(&self) -> Vec<&String> {
        let mut names = HashSet::new();
        for f in &self.task_fns {
            names.insert(&f.pipes.reader);
        }
        names.into_iter().collect()
    }

    fn find_writers(
        writer_names: &[String],
        pipes: &HashMap<String, Pipe<BoxedAnySend>>,
    ) -> Result<Vec<PipeWriter<BoxedAnySend>>, String> {
        let mut writers = Vec::new();
        for writer_name in writer_names {
            writers.push(Self::find_writer(writer_name, &pipes)?);
        }
        Ok(writers)
    }

    fn find_reader(
        name: &str,
        pipes: &mut HashMap<String, Pipe<BoxedAnySend>>,
    ) -> Result<PipeReader<BoxedAnySend>, String> {
        Ok(pipes
            .get_mut(name)
            .ok_or(&format!("no pipe with name {} found", name))?
            .reader
            .take()
            .ok_or("reader was already used")?)
    }

    fn find_writer(
        name: &str,
        pipes: &HashMap<String, Pipe<BoxedAnySend>>,
    ) -> Result<PipeWriter<BoxedAnySend>, String> {
        Ok(pipes
            .get(name)
            .ok_or(&format!("no pipe with name {} found", name))?
            .writer
            .clone())
    }

    async fn new_detached_producer(
        mut producer: ProducerFn,
        writers: Vec<PipeWriter<BoxedAnySend>>,
    ) {
        loop {
            let writers = writers.clone();
            if let Some(results) = (*producer)().await {
                write_results(writers, results).await;
            } else {
                break;
            }
        }
    }

    async fn new_detached_worker(
        task: TaskFn,
        mut reader: PipeReader<BoxedAnySend>,
        writers: Vec<PipeWriter<BoxedAnySend>>,
        mut signal_rx: Receiver<StageWorkerSignal>,
    ) {
        let mut tasks = JoinSet::new();

        loop {
            select! {
                Some(value) = reader.read() => {
                    let consumed = reader.consumed_callback();
                    let writers = writers.clone();
                    let result = task(value);

                    tasks.spawn(async move {
                        if let Some(results) = result.await {
                            write_results(writers, results).await;
                        }

                        // Mark input as consumed *after* writing outputs
                        // (avoids false positive of completed pipeline)
                        consumed();
                    });
                }

                // Join tasks to check for errors
                Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                    check_join_result(&result);
                }

                // Receive external signals
                Some(signal) = signal_rx.recv() => {
                    match signal {
                        StageWorkerSignal::Terminate => break,
                        StageWorkerSignal::Ping => println!("Pong!"),
                    }
                },
            }
        }

        tasks.shutdown().await;
    }
}

/// A pipeline provides the infrastructure for managing a set of workers that operate on and
/// transfer data between them using pipes.
///
/// # Examples
///
/// Creating a single producer and a single consumer.
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::atomic::Ordering::{Acquire, SeqCst};
/// use tokio::sync::Mutex;
/// use async_pipes::Pipeline;
///
/// tokio_test::block_on(async {
///     let count = Arc::new(Mutex::new(0usize));
///
///     let sum = Arc::new(AtomicUsize::new(0));
///     let task_sum = sum.clone();
///
///     Pipeline::builder()
///         // Produce values 1 through 10
///         .with_producer("data", move || {
///             let c = count.clone();
///             async move {
///                 let mut c = c.lock().await;
///                 if *c < 10 {
///                     *c += 1;
///                     Some(*c)
///                 } else {
///                     None
///                 }
///             }
///         })
///         .with_consumer("data", move |value: usize| {
///             let ts = task_sum.clone();
///             async move {
///                 ts.fetch_add(value, SeqCst);
///             }
///         })
///         .build()
///         .expect("failed to build pipeline")
///         .wait()
///         .await;
///
///     assert_eq!(sum.load(Acquire), 55);
/// });
/// ```
///
/// Creating a branching producer and two consumers for each branch.
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::atomic::Ordering::Acquire;
/// use tokio::sync::Mutex;
/// use async_pipes::{BoxedAnySend, Pipeline};
///
/// tokio_test::block_on(async {
///     let count = Arc::new(Mutex::new(0usize));
///
///     let odds_sum = Arc::new(AtomicUsize::new(0));
///     let task_odds_sum = odds_sum.clone();
///
///     let evens_sum = Arc::new(AtomicUsize::new(0));
///     let task_evens_sum = evens_sum.clone();
///
///     Pipeline::builder()
///         .with_branching_producer(vec!["evens", "odds"], move || {
///             let c = count.clone();
///             async move {
///                 let mut c = c.lock().await;
///                 if *c >= 10 {
///                     return None;
///                 }
///                 *c += 1;
///
///                 let output: Vec<Option<BoxedAnySend>> = if *c % 2 == 0 {
///                     vec![Some(Box::new(*c)), None]
///                 } else {
///                     vec![None, Some(Box::new(*c))]
///                 };
///
///                 Some(output)
///             }
///         })
///         .with_consumer("odds", move |n: usize| {
///             let odds_sum = task_odds_sum.clone();
///             async move {
///                 odds_sum.fetch_add(n, Ordering::SeqCst);
///             }
///         })
///         .with_consumer("evens", move |n: usize| {
///             let evens_sum = task_evens_sum.clone();
///             async move {
///                 evens_sum.fetch_add(n, Ordering::SeqCst);
///             }
///         })
///         .build()
///         .expect("failed to build pipeline!")
///         .wait()
///         .await;
///
///     assert_eq!(odds_sum.load(Acquire), 25);
///     assert_eq!(evens_sum.load(Acquire), 30);
/// });
/// ```
#[derive(Debug)]
pub struct Pipeline {
    synchronizer: Arc<Synchronizer>,
    producers: JoinSet<()>,
    workers: JoinSet<()>,
    signal_txs: Vec<Sender<StageWorkerSignal>>,
}

impl Pipeline {
    /// Create a new pipeline builder.
    pub fn builder() -> PipelineBuilder {
        PipelineBuilder::default()
    }

    /// Wait for the pipeline to complete.
    ///
    /// Once the pipeline is complete, a termination signal is sent to to all the workers.
    ///
    /// A pipeline progresses to completion by doing the following:
    ///   1. Wait for all "producers" to complete while also progressing stage workers
    ///   2. Wait for either all the stage workers to complete, or wait for the internal synchronizer to notify
    ///      of completion (i.e. there's no more data flowing through the pipeline)
    ///
    /// Step 1 implies that if the producers never finish, the pipeline will run forever. See
    /// [Pipeline::register_producer] for more info.
    pub async fn wait(mut self) {
        let workers_to_progress = Arc::new(Mutex::new(self.workers));
        let workers_to_finish = workers_to_progress.clone();

        let wait_for_producers = async {
            while let Some(result) = self.producers.join_next().await {
                check_join_result(&result);
            }
        };
        let wait_for_workers = |workers: Arc<Mutex<JoinSet<()>>>| async move {
            while let Some(result) = workers.lock().await.join_next().await {
                check_join_result(&result);
            }
        };
        let check_sync_completed = async {
            while !self.synchronizer.completed() {
                yield_now().await
            }

            for tx in self.signal_txs {
                tx.send(StageWorkerSignal::Terminate)
                    .await
                    .expect("failed to send done signal")
            }
        };

        // Effectively, make progress until all producers are done.
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
            std::panic!("task panicked!")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::{Acquire, Release};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::join;

    use crate::{Pipeline, StageWorkerSignal};

    #[tokio::test]
    async fn test_stage_producer() {
        let written = Arc::new(AtomicBool::new(false));
        let task_written = written.clone();

        Pipeline::builder()
            .with_producer("pipe", move || {
                let written = task_written.clone();
                async move {
                    if !written.load(Acquire) {
                        written.store(true, Release);
                        Some("hello!".to_string())
                    } else {
                        None
                    }
                }
            })
            .with_consumer("pipe", |value: String| async move {
                assert_eq!(value, "hello!".to_string());
            })
            .build()
            .unwrap()
            .wait()
            .await;

        assert!(written.load(Acquire), "value was not handled by worker!");
    }

    #[tokio::test]
    async fn test_stage_single_output() {
        let written = Arc::new(AtomicBool::new(false));
        let task_written = written.clone();

        Pipeline::builder()
            .with_inputs("first", vec![1usize])
            .with_stage(
                "first",
                "second",
                |value: usize| async move { Some(value + 1) },
            )
            .with_consumer("second", move |value: usize| {
                let written = task_written.clone();
                async move {
                    assert_eq!(value, 2);
                    written.store(true, Release);
                }
            })
            .build()
            .unwrap()
            .wait()
            .await;

        assert!(written.load(Acquire), "value was not handled by worker!");
    }

    #[tokio::test]
    #[should_panic]
    async fn test_stage_propagates_task_panic() {
        Pipeline::builder()
            .with_inputs("pipe", vec![()])
            .with_consumer("pipe", |_: ()| async move { panic!("AHH!") })
            .build()
            .unwrap()
            .wait()
            .await;
    }

    #[tokio::test]
    async fn test_stage_receives_signal_terminate() {
        let pipeline = Pipeline::builder()
            .with_inputs("pipe", vec![()])
            .with_consumer("pipe", |_: ()| async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                panic!("worker did not terminate!");
            })
            .build()
            .unwrap();

        let signaller = pipeline.signal_txs.first().unwrap().clone();
        let _ = join!(
            pipeline.wait(),
            signaller.send(StageWorkerSignal::Terminate),
        );
    }
}

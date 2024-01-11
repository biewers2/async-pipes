use std::any::{type_name, Any};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::panic;
use std::pin::Pin;
use std::sync::Arc;

use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::task::{yield_now, JoinError, JoinSet};

pub use builder::*;
use io::{PipeReader, PipeWriter};
use sync::Synchronizer;

mod builder;
mod io;
mod sync;

/// A Box that can hold any value that is [Send].
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
/// use async_pipes::branch;
///
/// #[tokio::main]
/// async fn main() {
///     let task = |value: String| async move {
///         let length: usize = value.len();
///         let excited: String = format!("{}!", value);
///         let odd_length: bool = length % 2 == 1;
///
///         Some(branch![length, excited, odd_length])
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
/// }
/// ```
pub type BoxedAnySend = Box<dyn Any + Send + 'static>;

type ProducerFn = Box<dyn FnMut() -> TaskFuture + Send + 'static>;
type TaskFn = Box<dyn Fn(BoxedAnySend) -> TaskFuture + Send + Sync + 'static>;
type IterCastFn = Box<dyn Fn(BoxedAnySend) -> Vec<BoxedAnySend> + Send + Sync + 'static>;

type TaskFuture = Pin<Box<dyn Future<Output = Option<Vec<Option<BoxedAnySend>>>> + Send + 'static>>;

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

struct IterStage {
    stage_type: IterStageType,
    caster: IterCastFn,
    pipes: TaskPipeNames,
}

enum IterStageType {
    Flatten,
}

struct Pipe<T> {
    writer: PipeWriter<T>,
    /// Use an option here to "take" it when a reader is used.
    /// Only allow one reader per pipe.
    reader: Option<PipeReader<T>>,
}

enum Stage {
    Producer(ProducerStage),
    Regular(TaskStage),
    Iterator(IterStage),
}

/// Signals sent to stage workers.
///
/// Useful for interrupting the natural workflow to tell it something.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
enum StageWorkerSignal {
    /// Used to tell stage workers to finish immediately without waiting for remaining tasks to end.
    Terminate,
}

impl Display for StageWorkerSignal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let signal = match self {
            Self::Terminate => "SIGTERM",
        };
        write!(f, "{signal}")
    }
}

/// A pipeline provides the infrastructure for managing a set of workers that run user-defined
/// "tasks" on data going through the pipes.
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
/// #[tokio::main]
/// async fn main() {
///     let count = Arc::new(Mutex::new(0usize));
///
///     let sum = Arc::new(AtomicUsize::new(0));
///     let task_sum = sum.clone();
///
///     Pipeline::builder()
///         // Produce values 1 through 10
///         .with_producer("data", move || {
///             let count = count.clone();
///             async move {
///                 let mut count = count.lock().await;
///                 if *count < 10 {
///                     *count += 1;
///                     Some(*count)
///                 } else {
///                     None
///                 }
///             }
///         })
///         .with_consumer("data", move |value: usize| {
///             let sum = task_sum.clone();
///             async move {
///                 sum.fetch_add(value, SeqCst);
///             }
///         })
///         .build()
///         .expect("failed to build pipeline")
///         .wait()
///         .await;
///
///     assert_eq!(sum.load(Acquire), 55);
/// }
/// ```
///
/// Creating a branching producer and two consumers for each branch.
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::atomic::Ordering::Acquire;
/// use tokio::sync::Mutex;
/// use async_pipes::{branch, NoOutput, Pipeline};
///
/// #[tokio::main]
/// async fn main() {
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
///                 let result = if *c % 2 == 0 {
///                     branch![*c, NoOutput]
///                 } else {
///                     branch![NoOutput, *c]
///                 };
///                 Some(result)
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
/// }
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
    ///   2. Wait for either all the stage workers to complete, or wait for the internal
    ///      synchronizer to notify of completion (i.e. there's no more data flowing through the
    ///      pipeline)
    ///
    /// Step 1 implies that if the producers never finish, the pipeline will run forever. See
    /// [PipelineBuilder::with_producer] for more info.
    pub async fn wait(mut self) {
        let workers_to_progress = Arc::new(Mutex::new(self.workers));
        let workers_to_finish = workers_to_progress.clone();

        let wait_for_producers = async {
            while let Some(result) = self.producers.join_next().await {
                check_join_result(result);
            }
        };
        let wait_for_workers = |workers: Arc<Mutex<JoinSet<()>>>| async move {
            while let Some(result) = workers.lock().await.join_next().await {
                check_join_result(result);
            }
        };
        let check_sync_completed = async move {
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
            _ = wait_for_workers(workers_to_progress),
                if !workers_to_progress.lock().await.is_empty() => {},
        }

        // If either the synchronizer determines we're done, or all workers completed, we're done
        select! {
            _ = wait_for_workers(workers_to_finish) => {},
            _ = check_sync_completed => {},
        }
    }
}

fn find_reader(
    name: &str,
    pipes: &mut HashMap<String, Pipe<BoxedAnySend>>,
) -> Result<PipeReader<BoxedAnySend>, String> {
    Ok(pipes
        .get_mut(name)
        .unwrap_or_else(|| panic!("no pipe with name '{}' found", name))
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
        .ok_or(format!("pipeline has open-ended pipe: '{}'", name))?
        .writer
        .clone())
}

fn find_writers(
    names: &[String],
    pipes: &HashMap<String, Pipe<BoxedAnySend>>,
) -> Result<Vec<PipeWriter<BoxedAnySend>>, String> {
    let mut writers = Vec::new();
    for name in names {
        writers.push(find_writer(name, pipes)?);
    }
    Ok(writers)
}

async fn write_results<O>(writers: &[PipeWriter<O>], results: Vec<Option<O>>) {
    if results.len() != writers.len() {
        panic!("len(results) != len(writers)");
    }

    for (result, writer) in results.into_iter().zip(writers) {
        if let Some(result) = result {
            writer.write(result).await;
        }
    }
}

fn downcast_from_pipe<T: 'static>(value: BoxedAnySend, pipe_name: &str) -> Box<T> {
    value.downcast::<T>().unwrap_or_else(|_| {
        panic!(
            "failed to downcast input value to {} from pipe '{}'",
            type_name::<T>(),
            pipe_name,
        )
    })
}

fn check_join_result<T>(result: Result<T, JoinError>) {
    if let Err(e) = result {
        if e.is_panic() {
            panic::resume_unwind(e.into_panic())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::collections::HashSet;
    use std::sync::Arc;

    use tokio::select;

    use super::*;

    impl Pipe<BoxedAnySend> {
        fn new(id: impl Into<String>, has_reader: bool) -> (String, Self) {
            let id = id.into();
            let sync = Arc::new(Synchronizer::default());
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let pipe = Self {
                writer: PipeWriter::new(id.clone(), sync.clone(), tx),
                reader: has_reader.then_some(PipeReader::new(id.clone(), sync, rx)),
            };
            (id, pipe)
        }
    }

    #[test]
    fn test_find_reader() {
        let pipe_id = "Pipe";
        let mut pipes = HashMap::from([Pipe::new(pipe_id, true)]);

        let reader = find_reader(pipe_id, &mut pipes);
        assert!(reader.is_ok());
        assert_eq!(reader.unwrap().get_pipe_id(), pipe_id);
    }

    #[test]
    #[should_panic]
    fn test_find_reader_panics_on_no_reader() {
        let _ = find_reader("Pipe", &mut HashMap::from([]));
    }

    #[test]
    fn test_find_reader_already_used() {
        let mut pipes = HashMap::from([Pipe::new("NoReader", false)]);

        let reader = find_reader("NoReader", &mut pipes);
        assert!(reader.is_err());
        assert_eq!(reader.unwrap_err(), "reader was already used".to_string());
    }

    #[test]
    fn test_find_writer() {
        let pipe_id = "Pipe";
        let pipes = HashMap::from([Pipe::new(pipe_id, true)]);

        let writer = find_writer(pipe_id, &pipes);
        assert!(writer.is_ok());
        assert_eq!(writer.unwrap().get_pipe_id(), pipe_id);
    }

    #[test]
    fn test_find_writer_open_ended() {
        let pipes = HashMap::from([]);

        let writer = find_writer("Pipe", &pipes);
        assert!(writer.is_err());
        assert_eq!(writer.unwrap_err(), "pipeline has open-ended pipe: 'Pipe'");
    }

    #[test]
    fn test_find_writers() {
        let pipes = HashMap::from([
            Pipe::new("One", true),
            Pipe::new("Two", false),
            Pipe::new("Three", true),
        ]);

        let pipe_ids = vec!["Two".to_string(), "Three".to_string()];
        let writers = find_writers(&pipe_ids, &pipes);
        assert!(writers.is_ok());

        let mut pipe_ids = HashSet::<String, RandomState>::from_iter(pipe_ids);
        let writers = writers.unwrap();
        assert_eq!(writers.len(), 2);

        for writer in writers {
            let id = writer.get_pipe_id();
            assert!(pipe_ids.remove(id), "missing ID");
        }
    }

    #[test]
    fn test_find_writers_open_ended() {
        let pipes = HashMap::from([
            Pipe::new("One", true),
            Pipe::new("Two", false),
            Pipe::new("Three", true),
        ]);

        let pipe_ids = vec!["Two".to_string(), "Three".to_string(), "Four".to_string()];
        let writers = find_writers(&pipe_ids, &pipes);
        assert!(writers.is_err());
        assert_eq!(writers.unwrap_err(), "pipeline has open-ended pipe: 'Four'");
    }

    #[tokio::test]
    async fn test_write_results() {}

    // #[tokio::test]
    // #[should_panic]
    // async fn test_write_results_mismatched_lengths() {
    //     write_results()
    // }

    #[tokio::test]
    async fn test_stage_receives_signal_terminate() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        let pipeline = Pipeline::builder()
            .with_inputs("pipe", vec![()])
            .with_consumer("pipe", move |_: ()| {
                let tx = tx.clone();
                async move {
                    tx.send(()).await.unwrap();
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    panic!("worker did not terminate!");
                }
            })
            .build()
            .unwrap();

        let signaller = pipeline.signal_txs.first().unwrap().clone();
        select! {
            _ = pipeline.wait() => {},
            _ = rx.recv() => {
                signaller.send(StageWorkerSignal::Terminate).await.unwrap();
            }
        }
    }
}

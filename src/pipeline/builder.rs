use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use itertools::Itertools;
use tokio::task::JoinSet;

use crate::pipeline::io::*;
use crate::pipeline::sync::*;
use crate::pipeline::workers::{
    new_detached_flattener, new_detached_producer, new_detached_worker,
};
use crate::pipeline::*;

struct CreateWorkersOutput {
    producers: JoinSet<()>,
    workers: JoinSet<()>,
    signal_txs: Vec<Sender<StageWorkerSignal>>,
}

/// Used to construct a [Pipeline].
///
/// Can be created using [Pipeline::builder].
///
///
#[derive(Default)]
pub struct PipelineBuilder {
    stages: Vec<Stage>,
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

    /// A "producer" stage; registers a list of multiple inputs to be written to a list of
    /// corresponding pipes.
    ///
    /// The strings provided to `pipes` define where each input will go.
    /// The values will be written one at a time into each pipe.
    ///
    /// For example, say you have the following:
    ///
    /// ```text
    /// List of multiple inputs: [ [1, "hi", true], [2, "bye", false], [3, ".", false] ]
    /// List of pipes: [ "numbers", "strings", "bools" ]
    /// ```
    ///
    /// The inputs would be sent to the pipes like this:
    ///
    /// ```text
    /// Pipe         1st   2nd    3rd
    /// "numbers" <- 1     2      3
    /// "strings" <- "hi"  "bye"  "."
    /// "bools"   <- true  false  false
    /// ```
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
    /// The producer will continue producing values while the user-provided task function returns
    /// [Some]. This means that it is possible to create an infinite stream of values by simply
    /// never returning [None].
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

    /// A "producer" stage; registers a new stage that produces multiple values and writes them into
    /// their respective pipe.
    ///
    /// The strings provided to `pipes` define where each input will go.
    ///
    /// The producer will continue producing values while the user-provided task function returns
    /// [Some]. This means that it is possible to create an infinite stream of values by simply
    /// never returning [None].
    ///
    /// Each individual [Option] within the task output determines whether it will be sent to the
    /// corresponding pipe. If [Some] is specified, the inner value will be sent, if [None] is
    /// specified, nothing will be sent.
    ///
    /// As with all stages that have more than one ("branching") outputs, it's possible that each
    /// output could have a different type, and so to avoid large binary sizes from static
    /// dispatching, dynamic dispatching is used instead, utilizing the [BoxedAnySend] type. For
    /// examples on how to return these types of values in task functions, see [BoxedAnySend]'s
    /// examples.
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
        self.stages.push(Stage::Producer {
            function: Box::new(move || Box::pin(task())),
            pipes: ProducerPipeNames { writers: pipes },
        });
        self
    }

    /// A "consumer" stage; registers a new stage that consumes values from a pipe.
    ///
    /// The string provided to `pipe` define where values will be read from.
    ///
    /// The consumer will continue consuming values until the pipeline is terminated or the pipe it
    /// is receiving from is closed.
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
        self.with_branching_stage(pipe, Vec::<String>::new(), move |value| {
            let task_fut = task(value);
            async move {
                task_fut.await;
                Some(Vec::<Option<BoxedAnySend>>::new())
            }
        })
    }

    /// A "regular" stage; registers a new stage that operates on an input and produce a single
    /// output value that is written to a pipe.
    ///
    /// The string provided to `input_pipe` defines where values will be read from.
    /// The string provided to `output_pipe` defines where the produced output will go.
    ///
    /// The worker will continue working on input values until the pipeline is terminated or the
    /// pipe it is receiving from is closed.
    ///
    /// The [Option] returned by the task function determines whether it will be sent to the output
    /// pipe. If [Some] is specified, the inner value will be sent, if [None] is specified, nothing
    /// will be sent.
    ///
    /// # Returns
    ///
    /// This pipeline builder.
    ///
    pub fn with_stage<I, O, F, Fut>(
        self,
        input_pipe: impl AsRef<str>,
        output_pipe: impl AsRef<str>,
        task: F,
    ) -> Self
    where
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

    /// A "regular" stage; registers a new stage that operates on an input and produces multiple
    /// values that are written into their respective pipe.
    ///
    /// The string provided to `input_pipe` defines where values will be read from.
    /// The strings provided to `output_pipes` define where each produced output will go.
    ///
    /// The worker will continue working on input values until the pipeline is terminated or the
    /// pipe it is receiving from is closed.
    ///
    /// * If the user-defined task function returns [None], nothing will be done.
    /// * If it returns [Some], the inner value ([`Vec<Option<BoxedAnySend>>`]) will have the
    ///   following applied to each output option:
    ///     * If [Some] is specified, the inner value will be sent to the corresponding pipe.
    ///     * If [None] is specified, nothing will be sent.
    ///
    /// As with all stages that have more than one ("branching") outputs, it's possible that each
    /// output could have a different type, and so to avoid large binary sizes from static
    /// dispatching, dynamic dispatching is used instead, utilizing the [BoxedAnySend] type. For
    /// examples on how to return these types of values in task functions, see [BoxedAnySend]'s
    /// examples.
    ///
    /// # Returns
    ///
    /// This pipeline builder.
    ///
    pub fn with_branching_stage<I, F, Fut>(
        mut self,
        input_pipe: impl AsRef<str>,
        output_pipes: Vec<impl AsRef<str>>,
        task: F,
    ) -> Self
    where
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

        self.stages.push(Stage::Regular {
            function: Box::new(move |value: BoxedAnySend| {
                let value = downcast_from_pipe(value, &err_pipe);
                Box::pin(task(*value))
            }),
            pipes: TaskPipeNames {
                reader: input_pipe,
                writers: output_pipes,
            },
            options: Default::default(),
        });
        self
    }

    /// An "iterator-based" stage; registers a new stage that takes the data from one pipe and
    /// "flattens" it, feeding the results into another.
    ///
    /// This is useful if you have a pipe that produces a list of values in a single task execution,
    /// but you want to use it as input to another stage that takes only the individual values.
    ///
    /// The generic parameter is used by the pipeline builder to know what concrete type to
    /// cast the value to, which mean turbofish syntax will be needed to specify what the iterator
    /// type of that pipe is, for example:
    /// `Pipeline::builder().with_flattener::<Vec<u8>>("data", "bytes")`
    ///
    /// The string provided to `from_pipe` defines where the iterator of values will be read from.
    /// The string provided to `to_pipe` defines where the individual values will go.
    ///
    /// The worker will continue working until the pipeline is terminated or the pipe it is
    /// receiving from is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicI32, Ordering};
    /// use async_pipes::Pipeline;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let sum = Arc::new(AtomicI32::new(0));
    ///     let task_sum = sum.clone();
    ///
    ///     Pipeline::builder()
    ///         .with_inputs("NumberSets", vec![vec![1, 2], vec![3, 4, 5]])
    ///         .with_flattener::<Vec<i32>>("NumberSets", "Numbers")
    ///         .with_consumer("Numbers", move |value: i32| {
    ///             let sum = task_sum.clone();
    ///             async move {
    ///                 sum.fetch_add(value, Ordering::SeqCst);
    ///             }
    ///         })
    ///         .build()
    ///         .expect("failed to build pipeline")
    ///         .wait()
    ///         .await;
    ///
    ///     assert_eq!(sum.load(Ordering::Acquire), 15);
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// This pipeline builder.
    ///
    pub fn with_flattener<It>(
        mut self,
        from_pipe: impl AsRef<str>,
        to_pipe: impl AsRef<str>,
    ) -> Self
    where
        It: IntoIterator + Send + 'static,
        It::Item: Send,
    {
        let input_pipe = from_pipe.as_ref().to_string();
        let output_pipe = to_pipe.as_ref().to_string();
        let err_pipe = input_pipe.clone();

        self.stages.push(Stage::Iterator {
            stage_type: IterStageType::Flatten,
            caster: Box::new(move |value: BoxedAnySend| {
                downcast_from_pipe::<It>(value, &err_pipe)
                    .into_iter()
                    .map(|v| Box::new(v) as BoxedAnySend)
                    .collect()
            }),
            pipes: TaskPipeNames {
                reader: input_pipe,
                writers: vec![output_pipe],
            },
            options: WorkerOptions::default_single_task(),
        });
        self
    }

    /// A utility function for improving readability when building pipelines.
    ///
    /// This makes splitting up task definitions into functions easier by allowing the caller
    /// to pass in a function that takes `self` and returns `self`, effectively providing
    /// continuity to a call chain.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicI32, Ordering};
    /// use async_pipes::Pipeline;
    /// use async_pipes::PipelineBuilder;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let sum = Arc::new(AtomicI32::new(0));
    ///
    ///     Pipeline::builder()
    ///         .with_inputs("Numbers", vec![1, 2, 3, 4, 5])
    ///         .also(build_consumer(sum.clone()))
    ///         .build()
    ///         .expect("failed to build pipeline")
    ///         .wait()
    ///         .await;
    ///
    ///     assert_eq!(sum.load(Ordering::Acquire), 15);
    /// }
    ///
    /// fn build_consumer(sum: Arc<AtomicI32>) -> impl FnOnce(PipelineBuilder) -> PipelineBuilder {
    ///     |builder| builder.with_consumer("Numbers", move |value: i32| {
    ///         let sum = sum.clone();
    ///         async move {
    ///             sum.fetch_add(value, Ordering::SeqCst);
    ///         }
    ///     })
    /// }
    /// ```
    ///
    pub fn also(self, handler: impl FnOnce(PipelineBuilder) -> PipelineBuilder) -> Self {
        handler(self)
    }

    /// When the pipeline is ready to be built, this is called and will return a pipeline if
    /// it was successfully built, otherwise it will return an error describing why it could not be
    /// built.
    ///
    /// # Errors
    ///
    /// 1. A pipe is "open-ended", meaning there's no stage consuming values from that pipe.
    /// 2. The reader of a pipe was used more than once.
    ///
    pub fn build(self) -> Result<Pipeline, String> {
        let configs = self.create_pipe_configs()?;

        // Register pipes in the synchronizer
        let mut synchronizer = Synchronizer::default();
        self.register_pipe_configs(&mut synchronizer, &configs);

        // Make the synchronizer immutable and the create the actual pipes
        let synchronizer = Arc::new(synchronizer);
        let pipes_map = self.create_pipes(&synchronizer, configs);

        let CreateWorkersOutput {
            producers,
            workers,
            signal_txs,
        } = self.create_workers(pipes_map)?;

        Ok(Pipeline {
            synchronizer,
            producers,
            workers,
            signal_txs,
        })
    }

    /// Create the producers, workers, and the associated signal channels.
    fn create_workers(
        self,
        mut pipes_map: HashMap<String, Pipe<BoxedAnySend>>,
    ) -> Result<CreateWorkersOutput, String> {
        let mut producers = JoinSet::new();
        let mut workers = JoinSet::new();
        let mut signal_txs = Vec::new();

        let mut worker_args =
            |pipe_names: TaskPipeNames, pipes: &mut HashMap<String, Pipe<BoxedAnySend>>| {
                let writers = find_writers(&pipe_names.writers, pipes)?;
                let reader = find_reader(&pipe_names.reader, pipes)?;

                let (signal_tx, signal_rx) = tokio::sync::mpsc::channel(1);
                signal_txs.push(signal_tx);

                Result::<_, String>::Ok((reader, writers, signal_rx))
            };

        let mut has_producer = false;
        for stage in self.stages {
            match stage {
                Stage::Producer { function, pipes } => {
                    let writers = find_writers(&pipes.writers, &pipes_map)?;
                    has_producer = true;
                    producers.spawn(new_detached_producer(function, writers));
                }

                Stage::Regular {
                    function,
                    pipes,
                    options,
                } => {
                    let (reader, writers, signal_rx) = worker_args(pipes, &mut pipes_map)?;
                    workers.spawn(new_detached_worker(
                        function,
                        reader,
                        writers,
                        signal_rx,
                        options.try_into()?,
                    ));
                }

                Stage::Iterator {
                    stage_type,
                    caster,
                    pipes,
                    options,
                } => {
                    let (reader, writers, signal_rx) = worker_args(pipes, &mut pipes_map)?;
                    workers.spawn(match stage_type {
                        IterStageType::Flatten => new_detached_flattener(
                            caster,
                            reader,
                            writers,
                            signal_rx,
                            options.try_into()?,
                        ),
                    });
                }
            }
        }

        if !has_producer {
            return Err("pipeline must have at least one producer".to_string());
        }

        Ok(CreateWorkersOutput {
            producers,
            workers,
            signal_txs,
        })
    }

    /// Construct the configuration of all the pipes.
    ///
    /// We can achieve a set of unique names easily by using the `reader` of the "task" and "iter"
    /// stages and de-duplicating that list.
    ///
    /// For each pipe, there is only one reader, but there can be multiple writers.
    /// Also, producers do not have a reader, only writers.
    fn create_pipe_configs(&self) -> Result<Vec<PipeConfig>, String> {
        let mut configs = Vec::new();
        for stage in &self.stages {
            match stage {
                Stage::Regular { pipes, options, .. } | Stage::Iterator { pipes, options, .. } => {
                    configs.push(PipeConfig {
                        name: pipes.reader.clone(),
                        options: options.clone().try_into()?,
                    });
                }

                _ => {}
            }
        }

        Ok(configs
            .into_iter()
            .unique_by(|conf| conf.name.clone())
            .collect())
    }

    /// Register all the pipe configurations to the synchronizer.
    ///
    /// This is done before creating the pipes as the synchronizer only needs to be mutable
    /// for this step, afterwards it can be considered "immutable".
    fn register_pipe_configs(&self, sync: &mut Synchronizer, pipe_configs: &[PipeConfig]) {
        for conf in pipe_configs {
            sync.register(&conf.name);
        }
    }

    /// The reader of a pipe is wrapped in an option to allow the build method to [Option::take] it
    /// to maintain the invariant that there's only one reader per pipe.
    fn create_pipes(
        &self,
        sync: &Arc<Synchronizer>,
        configs: Vec<PipeConfig>,
    ) -> HashMap<String, Pipe<BoxedAnySend>> {
        configs
            .into_iter()
            .map(|conf| {
                let buf_size = conf.options.reader_buffer_size.get();
                let (tx, rx) = tokio::sync::mpsc::channel(buf_size);
                let pipe = Pipe {
                    writer: PipeWriter::new(&conf.name, sync.clone(), tx),
                    reader: Some(PipeReader::new(&conf.name, sync.clone(), rx)),
                };
                (conf.name, pipe)
            })
            .collect()
    }
}

//! Create a lightweight, concurrent data processing pipeline for Rust applications.
//!
//! # Overview
//!
//! Async Pipes provides a simple way to create high-throughput data processing pipelines by utilizing Rust's
//! asynchronous runtime capabilities. This is done by this library providing the infrastructure for managing
//! asynchronous tasks and data transfer between the tasks so the developer only has to worry about the task-specific
//! implementation for each stage in the pipeline.
//!
//! A core feature of this library is that it designed to run in any runtime environment (single-threaded or
//! multi-threaded).
//!
//! # Terminology
//!
//! All of these are abstractions to help conceptualize how data is transferred and operated on in
//! the pipeline.
//!
//! * **Pipe** - Represents something where a type of data can flow.
//!   An example of this being a pipe that allows strings to flow through it.
//! * **Stage** - Represents the "nodes" in a pipeline where work is done.
//!   A stage typically includes the definition of the worker, an optional pipe connection
//!   for reading data from, and zero or more pipe connections for sending data to.
//! * **Worker** - A worker is internally defined by this library, and does the work of
//!   reading from the optional input pipe, performing a user-defined task on the input, and
//!   then writing the output of that task to the zero or more output pipes.
//! * **Pipeline** - Represents the overall set of stages and the pipes that connect the stages.
//!   Pipelines don't necessarily have to be linear, they can branch off of one stage into
//!   multiple stages.
//!
//! # Getting Started
//!
//! One of the core concepts of a pipeline is its pipes, which is the first thing defined when
//! constructing a pipeline. Pipes are defined by providing a list of their names. These names
//! are used later to get the I/O objects of a pipe, and those will be used to read from/write to the
//! pipe.
//! ```
//! use async_pipes::Pipeline;
//!
//! let pipe_names = vec!["Transfer", "Counted"];
//! let (mut pipeline, mut pipes) = Pipeline::from_pipes(pipe_names);
//! ```
//!
//! The `pipes` value is an instance of [Pipes], and using this the I/O objects can be created
//! for a pipe by calling [Pipes::create_io] and specifying the pipe's name. In the example below,
//! The I/O objects would be [`PipeReader<String>`] and [`PipeWriter<String>`].
//!
//! Pipe writers can be cloned as many times as needed, but there can only be a single pipe reader for a pipe.
//! ```
//! use async_pipes::Pipeline;
//!
//! let (mut pipeline, mut pipes) = Pipeline::from_pipes(vec!["Transfer", "Counted"]);
//!
//! let (transfer_writer, transfer_reader) = pipes.create_io::<String>("Transfer").unwrap();
//! let (counter_writer, counter_reader) = pipes.create_io::<usize>("Counted").unwrap();
//! ```
//!
//! With the I/O objects of a pipe, stages can be registered to operate on data flowing through the pipes.
//!
//! There are a few categories of stages, each distinguished to allow for different semantics. For example,
//! some of the categories are static/dynamic producers, regular stages, branching stages, consumers, etc.
//! For more info on these categories, see [Stage Categories](#stage-categories).
//!
//! Once a stage is registered, the worker will make progress if it can. This means (in a multi-threaded
//! context) if there is an available thread to run on, the worker will make progress on that thread.
//!
//! After registering the stages, call [Pipeline::wait] to wait for the pipeline to finish.
//! ```
//! use async_pipes::Pipeline;
//!
//! #[tokio::main]
//! async fn main() {
//!     use std::sync::Arc;
//! use std::sync::atomic::{AtomicUsize, Ordering};
//! let (mut pipeline, mut pipes) = Pipeline::from_pipes(vec!["Transfer", "Counted"]);
//!
//!     let (transfer_writer, transfer_reader) = pipes.create_io::<String>("Transfer").unwrap();
//!     let (counted_writer, counted_reader) = pipes.create_io::<usize>("Counted").unwrap();
//!
//!     let sum = Arc::new(AtomicUsize::new(0));
//!     let summer_sum = sum.clone();
//!
//!     let inputs = vec!["a".to_string(), "abc".to_string()];
//!     pipeline.register_inputs("Transfer", transfer_writer, inputs);
//!     pipeline.register("Counter", transfer_reader, counted_writer, |value: String| async move {
//!         Some(value.len())
//!     });
//!     pipeline.register_consumer("Summer", counted_reader, |value: usize| async move {
//!         summer_sum.fetch_add(value, Ordering::SeqCst);
//!     });
//!
//!     pipeline.wait().await;
//!
//!     assert_eq!(sum.load(Ordering::Acquire), 4);
//! }
//! ```
//!
//! # Stage Categories <a name="stage-categories"></a>
//!
//! #### Producer ("entry stage")
//! A producer is the only place where data can be fed into the pipeline.
//!
//! **Static (definite)**
//!
//! This is where a list of concrete values can be provided to the stage and the worker will loop over each
//! value and feed it into a [PipeWriter].
//! * [Pipeline::register_inputs]
//! * [Pipeline::register_branching_inputs]
//!
//! **Dynamic (indefinite)**
//!
//! This is useful when there are no pre-defined input values. Instead, a function that produces a single
//! value can be provided that produces an [Option] where it's continually called until [None] is returned.
//! This can be useful when receiving data over the network, or data is read from a file.
//! * [Pipeline::register_producer]
//! * [Pipeline::register_branching_producer]
//!
//! ### Consumer ("terminating stage")
//! A consumer is a final stage in the pipeline where data ends up. It takes in a single pipe reader and
//! produces no output.
//! * [Pipeline::register_consumer]
//!
//! ### Regular (1 input, 1 output)
//! This is an intermediate stage in the pipeline that takes in a single input, and produces one or more output.
//! * [Pipeline::register]
//! * [Pipeline::register_branching]
//!
//! # Stage Variants
//!
//! ### Branching (1 input, N outputs)
//! A branching stage is a stage where multiple output pipes are connected. This means the task defined by the
//! user in this stage returns two or more output values.
//! * [Pipeline::register_branching]
//! * [Pipeline::register_branching_inputs]
//! * [Pipeline::register_branching_producer]
//!
//! # Examples
//!
//! ```
//! use async_pipes::Pipeline;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create the pipeline from a list of pipe names.
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicUsize, Ordering};
//! use tokio::sync::Mutex;
//!
//! let (mut pipeline, mut pipes) =
//!         Pipeline::from_pipes(vec!["MapInput", "MapToReduce"]);
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
//!     // It's recommended to wrap large read-only data in an [Arc], as the closure is cloned
//!     // on each execution (which means context values are too). It's required to do this if the
//!     // value is mutable so the variable points to the same data.
//!     let total_count = Arc::new(AtomicUsize::new(0));
//!     let reduce_total_count = total_count.clone();
//!
//!     pipeline.register_consumer("ReduceStage", map_to_reduce_r, |value: String| async move {
//!         reduce_total_count.fetch_add(value.len(), Ordering::SeqCst);
//!     });
//!
//!     pipeline.wait().await;
//!
//!     // We see that after the data goes through our map and reduce stages,
//!     // we effectively get this: `len("a!") + len("bb!") + len("ccc!") = 9`
//!     assert_eq!(total_count.load(Ordering::Acquire), 9);
//! }
//! ```
//!
#![warn(missing_docs)]

use std::fmt::{Debug, Display, Formatter};
use std::future::Future;

use tokio::sync::mpsc::Receiver;

pub use io::*;
pub use pipeline::*;

mod io;
mod pipeline;
pub(crate) mod sync;
mod util;

/// Signals sent to stage workers.
///
/// Useful for interrupting the natural workflow to tell it something.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
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

impl Display for StageWorkerSignal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Ping => "SIGPING",
                Self::Terminate => "SIGTERM",
            }
        )
    }
}

/// Simple data structure to hold information about a stage worker.
#[derive(Debug)]
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

//! Create a lightweight, concurrent data processing pipeline for Rust applications.
//!
//! # Overview
//!
//! Async Pipes provides a simple way to create high-throughput data processing pipelines by
//! utilizing Rust's asynchronous runtime capabilities. This is done by managing task execution and
//! data flow so the developer only has to worry about the task-specific implementation for each
//! stage in the pipeline.
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
//! A pipeline can be built using the builder provided by [Pipeline::builder]. This allows the
//! pipeline to be configured before any work is done.
//! ```
//! use async_pipes::{Pipeline, PipelineBuilder};
//!
//! let builder: PipelineBuilder = Pipeline::builder();
//! ```
//!
//! Using the builder, stages can be defined, where a stage contains the name of a pipe to read from
//! (if applicable), the name of a pipe to write to (or more if applicable), some options for the
//! worker, and a user-defined "task" function.
//!
//! For information on what worker options are available, see [WorkerOptions].
//!
//! Demonstrated below is a pipeline being built with a producer stage, a regular stage, and a
//! consuming stage.
//! ```
//! use async_pipes::Pipeline;
//! use async_pipes::WorkerOptions;
//!
//! #[tokio::main]
//! async fn main() {
//!     let pipeline: Result<Pipeline, String> = Pipeline::builder()
//!         .with_inputs("InputPipe", vec![1, 2, 3])
//!         .with_stage(
//!             "InputPipe",
//!             "OutputPipe",
//!             WorkerOptions::default(),
//!             |n: i32| async move { Some(n + 1) }
//!         )
//!         .with_consumer(
//!             "OutputPipe",
//!             WorkerOptions::default_single_task(),
//!             |n: i32| async move { println!("{}", n) }
//!         )
//!         .build();
//!
//!     assert!(pipeline.is_ok());
//! }
//! ```
//!
//! With the builder, any number of stages can be defined with any number of pipes, but there are a
//! few requirements:
//! 1. There must be at least one producer - how else will data get into the pipeline?
//! 2. Every pipe must have a corresponding stage that reads data from it - this is required to
//!    avoid a deadlock from pipes being filled up but not emptied.
//!
//! These requirements are enforced by [PipelineBuilder::build] returning a
//! [Result<Pipeline, String>] where an error describing the missing requirement is returned.
//!
//! For example, here is an invalid pipeline due to requirement (1) not being followed:
//! ```
//! use async_pipes::Pipeline;
//! use async_pipes::WorkerOptions;
//!
//! #[tokio::main]
//! async fn main() {
//!     let pipeline = Pipeline::builder()
//!         .with_consumer("MyPipe", WorkerOptions::default(), |n: usize| async move {
//!             println!("{}", n);
//!         })
//!         .build();
//!
//!     assert_eq!(pipeline.unwrap_err(), "pipeline must have at least one producer");
//! }
//! ```
//!
//! And here is an invalid pipeline due to requirement (2) not being followed:
//! ```
//! use async_pipes::Pipeline;
//!
//! #[tokio::main]
//! async fn main() {
//!     let pipeline = Pipeline::builder()
//!         .with_inputs("MyPipe", vec![1, 2, 3])
//!         .build();
//!
//!     assert_eq!(pipeline.unwrap_err(), "pipeline has open-ended pipe: 'MyPipe'");
//! }
//! ```
//!
//! Once an `Ok(Pipeline)` is returned, it can be waited on using [Pipeline::wait], where it will
//! make progress until all workers finish or there is no more data in the pipeline.
//!
//! _Note_: When a pipeline is built, depending on the runtime it may or may not be running.
//! In single-threaded runtimes no progress will be made as the workers can't make progress on their
//! own unless the single thread yields to them. It is possible for them to make progress in multi-
//! threaded runtimes. However, the pipeline will never "finish" until [Pipeline::wait] is called.
//!
//! ```
//! use async_pipes::Pipeline;
//! use async_pipes::WorkerOptions;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), String> {
//!     Pipeline::builder()
//!         .with_inputs("InputPipe", vec![1, 2, 3])
//!         .with_stage("InputPipe", "OutputPipe", WorkerOptions::default(), |n: i32| async move {
//!             Some(n + 1)
//!         })
//!         .with_consumer("OutputPipe", WorkerOptions::default(), |n: i32| async move {
//!             println!("{}", n)
//!         })
//!         .build()?
//!         .wait()
//!         .await;
//!
//!     Ok(())
//! }
//! ```
//!
//! ### Stateful Stages
//!
//! It is possible to maintain state in a stage across tasks, however the state must be [Send].
//! Usually this is best done for non-Send objects by wrapping them in an [std::sync::Mutex]
//! (or even better, [tokio::sync::Mutex]).
//!
//! Another caveat with state in stages is that since the task function returns a future
//! (`async move { ... }`), it requires ownership of non-`'static` lifetime values in order to
//! continue working on other inputs as the future may not be able to reference borrowed state.
//! A way around this is to wrap values that may be expensive to clone in [std::sync::Arc].
//!
//! The following is an example of a mutable sum being used as a stateful item in a stage:
//! ```
//! use async_pipes::Pipeline;
//! use std::sync::Arc;
//! use tokio::sync::Mutex;
//! use async_pipes::WorkerOptions;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), String> {
//!     // [AtomicUsize] may be preferred here, but we use [Mutex] for the sake of this example
//!     let sum = Arc::new(Mutex::new(0));
//!     // For the assertion at the end of this example
//!     let test_sum = sum.clone();
//!
//!     Pipeline::builder()
//!         .with_inputs("InputPipe", vec![1, 2, 3])
//!         .with_stage("InputPipe", "OutputPipe", WorkerOptions::default(), move |n: i32| {
//!             // As the sum is owned by this closure, we need to clone it to move an owned value
//!             // into the `async move` block.
//!             let sum = sum.clone();
//!             async move {
//!                 let mut sum = sum.lock().await;
//!                 *sum += n;
//!                 Some(*sum)
//!             }
//!         })
//!         .with_consumer("OutputPipe", WorkerOptions::default(), |n: i32| async move {
//!             println!("Counter now at: {}", n)
//!         })
//!         .build()?
//!         .wait()
//!         .await;
//!
//!     assert_eq!(*test_sum.lock().await, 6);
//!     Ok(())
//! }
//! ```
//!
//! # Stage Categories <a name="stage-categories"></a>
//!
//! ### Producer ("entry stage")
//! A producer is the only place where data can be fed into the pipeline.
//!
//! **Static (definite)**
//!
//! This is where a list of concrete values can be provided to the stage and the worker will loop
//! over each value and feed it into a pipe.
//! * [PipelineBuilder::with_inputs]
//! * [PipelineBuilder::with_branching_inputs]
//!
//! **Dynamic (indefinite)**
//!
//! This is useful when there are no pre-defined input values. Instead, a function that produces a
//! single value can be provided that produces an [Option] where it's continually called until
//! [None] is returned. This can be useful when receiving data over the network, or data is read
//! from a file.
//! * [PipelineBuilder::with_producer]
//! * [PipelineBuilder::with_branching_producer]
//!
//! ### Consumer ("terminating stage")
//! A consumer is a final stage in the pipeline where data ends up. It takes in a single pipe to
//! read from and produces no output.
//! * [PipelineBuilder::with_consumer]
//!
//! ### Regular (1 input, 1 output)
//! This is an intermediate stage in the pipeline that takes in a single input, and produces one or
//! more output.
//! * [PipelineBuilder::with_stage]
//! * [PipelineBuilder::with_branching_stage]
//!
//! ### Utility
//! This is an intermediate stage in the pipeline that can be used to do common operations on data
//! between pipes.
//! * [PipelineBuilder::with_flattener]
//!
//! # Stage Variants
//!
//! ### Branching (1 input, N outputs)
//! A branching stage is a stage where multiple output pipes are connected. This means the task
//! defined by the user in this stage returns two or more output values.
//! * [PipelineBuilder::with_branching_inputs]
//! * [PipelineBuilder::with_branching_producer]
//! * [PipelineBuilder::with_branching_stage]
//!
//! # Examples
//!
//! ```
//! use std::sync::Arc;
//!
//! use async_pipes::Pipeline;
//!
//! use std::sync::atomic::{AtomicUsize, Ordering};
//! use tokio::sync::Mutex;
//! use async_pipes::WorkerOptions;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), String> {
//!     // Due to the task function returning a future (`async move { ... }`), data needs
//!     // to be wrapped in an [Arc] and then cloned in order to be moved into the task
//!     // while still referencing it from this scope
//!     let total_count = Arc::new(AtomicUsize::new(0));
//!     let task_total_count = total_count.clone();
//!
//!     Pipeline::builder()
//!         .with_inputs("MapPipe", vec!["a", "bb", "ccc"])
//!
//!         // Read from the 'MapPipe' and write to the 'ReducePipe'
//!         .with_stage(
//!             "MapPipe",
//!             "ReducePipe",
//!             WorkerOptions::default(),
//!             |value: &'static str| async move {
//!                 // We return an option to tell the stage whether to write the new value
//!                 // to the pipe or ignore it
//!                 Some(format!("{}!", value))
//!             }
//!         )
//!
//!         // Read from the 'ReducePipe'.
//!         .with_consumer("ReducePipe", WorkerOptions::default(), move |value: String| {
//!             // The captured `task_total_count` can't move out of this closure, so we
//!             // have to clone it to give ownership to the async block. Remember, it's
//!             // wrapped in an [Arc] so we're still referring to the original data.
//!             let total_count = task_total_count.clone();
//!             async move {
//!                 total_count.fetch_add(value.len(), Ordering::SeqCst);
//!             }
//!         })
//!
//!         // Build the pipeline and wait for it to finish
//!         .build()?
//!         .wait()
//!         .await;
//!
//!     // We see that after the data goes through our map and reduce stages,
//!     // we effectively get this: `len("a!") + len("bb!") + len("ccc!") = 9`
//!     assert_eq!(total_count.load(Ordering::Acquire), 9);
//!     Ok(())
//! }
//! ```
//!
#![warn(missing_docs)]

pub use pipeline::*;

mod pipeline;

/// A value used in coordination with [branch] to indicate there is no value to be sent to a
/// pipe.
///
/// # Examples
///
/// ```
/// use async_pipes::{NoOutput, BoxedAnySend, branch};
///
/// let outputs: Vec<Option<BoxedAnySend>> = branch![
///     "one",
///     NoOutput,
///     3,
/// ];
///
/// assert!(outputs[0].is_some());
/// assert!(outputs[1].is_none());
/// assert!(outputs[2].is_some());
/// ```
#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub struct NoOutput;

/// Defines an idiomatic way to provide values to a static branching producer stage (i.e. concrete
/// input values).
///
/// A list of tuples of values (of possibly different types) can be provided, and those values will be boxed
/// and then put into a [Vec].
///
/// # Examples
///
/// Here's an example of what is returned by the macro call.
/// ```
/// use async_pipes::{BoxedAnySend, branch_inputs};
///
/// let inputs: Vec<Vec<BoxedAnySend>> = branch_inputs![
///     (1usize, 1i32, 1u8),
///     (2usize, 2i32, 2u8),
///     (3usize, 3i32, 3u8),
/// ];
///
/// assert_eq!(inputs.len(), 3);
/// ```
///
/// Here's an example of the macro being used in a pipeline.
/// ```
/// use async_pipes::{branch_inputs, Pipeline};
/// use async_pipes::WorkerOptions;
///
/// #[tokio::main]
/// async fn main() {
///     Pipeline::builder()
///         .with_branching_inputs(
///             vec!["One", "Two"],
///             branch_inputs![
///                 (1usize, "Hello"),
///                 (1usize, "World"),
///                 (1usize, "!"),
///             ],
///         )
///         .with_consumer("One", WorkerOptions::default(), |value: usize| async move {
///             /* ... */
///         })
///         .with_consumer("Two", WorkerOptions::default(), |value: &'static str| async move {
///             /* ... */
///         })
///         .build()
///         .unwrap()
///         .wait()
///         .await;
/// }
/// ```
#[macro_export]
macro_rules! branch_inputs {
    ($(( $($x:expr),+ $(,)? )),* $(,)?) => {
        vec![
            $( branch_inputs!($($x),+) ),*
        ]
    };

    ($($x:expr),+ $(,)?) => {
        vec![
            $(std::boxed::Box::new($x) as $crate::BoxedAnySend),+
        ]
    };
}

/// Defines an idiomatic way to return values in a branching stage.
///
/// A list of values (possibly of different types) can be provided. These values will be boxed and
/// then wrapped in a [Some]. In order to specify [None] (i.e. no value should be sent to the
/// respective pipe), [NoOutput] should be used in the place of a value. The macro will detect this
/// and use [None] in its place.
///
/// # Examples
///
/// Here's an example of what is returned by the macro call.
/// ```
/// use async_pipes::{BoxedAnySend, NoOutput, branch};
///
/// let inputs: Vec<Option<BoxedAnySend>> = branch![1, "hello", true, NoOutput, 12.0];
///
/// assert_eq!(inputs.len(), 5);
/// assert!(inputs[3].is_none())
/// ```
///
/// Here's an example of the macro being used in a pipeline.
/// ```
/// use async_pipes::{branch, branch_inputs, Pipeline};
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::Arc;
/// use async_pipes::WorkerOptions;
///
/// #[tokio::main]
/// async fn main() {
///     Pipeline::builder()
///         .with_inputs("Count", vec![1, 2, 3])
///         .with_branching_stage(
///             "Count",
///             vec!["Value", "Doubled"],
///             WorkerOptions::default(),
///             |value: i32| async move {
///                 Some(branch![value, value * 2])
///             }
///         )
///         .with_consumer("Value", WorkerOptions::default(), |value: i32| async move {
///             /* ... */
///         })
///         .with_consumer("Doubled", WorkerOptions::default(), |value: i32| async move {
///             /* ... */
///         })
///         .build()
///         .unwrap()
///         .wait()
///         .await;
/// }
/// ```
#[macro_export]
macro_rules! branch {
    ($($x:expr),+ $(,)?) => {
        std::vec![
            $({
                let x: $crate::BoxedAnySend = std::boxed::Box::new($x);
                x.downcast_ref::<$crate::NoOutput>().is_none().then_some(x)
            }),+
        ]
    };
}

#[cfg(test)]
mod tests {
    use crate::{NoOutput, Pipeline, WorkerOptions};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Use the code of this test for the `README`'s `Simple, Linear Pipeline Example`.
    #[tokio::test]
    async fn test_readme_simple_linear_pipeline() {
        let total = Arc::new(AtomicUsize::new(0));
        let task_total = total.clone();

        Pipeline::builder()
            .with_inputs("MapPipe", vec!["a", "bb", "ccc"])
            .with_stage(
                "MapPipe",
                "ReducePipe",
                WorkerOptions::default(),
                |value: &'static str| async move { Some(format!("{}!", value)) },
            )
            .with_consumer(
                "ReducePipe",
                WorkerOptions::default_single_task(),
                move |value: String| {
                    let total = task_total.clone();
                    async move {
                        total.fetch_add(value.len(), Ordering::SeqCst);
                    }
                },
            )
            .build()
            .expect("failed to build pipeline!")
            .wait()
            .await;

        assert_eq!(total.load(Ordering::Acquire), 9);
    }

    /// Use the code of this test for the `README`'s `Branching, Cyclic Pipeline Example`.
    #[tokio::test]
    async fn test_readme_branching_cyclic_pipeline() {
        let initial_urls = vec![
            "https://example.com".to_string(),
            "https://rust-lang.org".to_string(),
        ];

        Pipeline::builder()
            .with_inputs("ToFetch", initial_urls)
            .with_flattener::<Vec<String>>("ToFlattenThenFetch", "ToFetch")
            .with_stage(
                "ToFetch",
                "ToCrawl",
                WorkerOptions::default_multi_task(),
                |_url: String| async move {
                    // Fetch content from url...
                    Some("<html>Sample Content</html>".to_string())
                },
            )
            .with_branching_stage(
                "ToCrawl",
                vec!["ToFlattenThenFetch", "ToLog"],
                WorkerOptions::default_single_task(),
                |_html: String| async move {
                    // Crawl HTML, extracting embedded URLs and content
                    let has_embedded_urls = false; // Mimic the crawler not finding any URLs

                    let output = if has_embedded_urls {
                        let urls = vec![
                            "https://first.com".to_string(),
                            "https://second.com".to_string(),
                        ];
                        branch![urls, NoOutput]
                    } else {
                        branch![NoOutput, "Extracted content".to_string()]
                    };

                    Some(output)
                },
            )
            .with_consumer(
                "ToLog",
                WorkerOptions::default_single_task(),
                |content: String| async move { println!("{content}") },
            )
            .build()
            .expect("failed to build pipeline!")
            .wait()
            .await;
    }
}

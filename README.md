# Plumber

Create a lightweight, in-memory data processing pipeline for Rust applications.

## Description

Plumber provides a simple way to create high-throughput data processing pipelines by utilizing Rust's
asynchronous runtime capabilities. This is done by providing the infrastructure for managing the asynchronous
tasks and the data transfer between the tasks so the developer only has to worry about the task-specific
implementation for each stage in the pipeline.

## Getting Started

_WIP_

#### Simple, Linear Pipeline Example

```rust
use std::sync::Arc;
use tokio::sync::Mutex;
use plumber::Pipeline;

#[tokio::main]
async fn main() {
    // Create the pipeline from a list of pipe names.
    let (mut pipeline, mut pipes) =
        Pipeline::from_pipes(vec!["MapInput", "MapToReduce", "ReduceToLog"]);

    // We create "writers" (*_w) and "readers" (*_r) to transfer data
    let (map_input_w, map_input_r) = pipes.create_io("MapInput").unwrap();
    let (map_to_reduce_w, map_to_reduce_r) = pipes.create_io("MapToReduce").unwrap();

    // After creating the pipes, stage workers are registered in the pipeline.
    //
    // Things to note:
    // - As of now, stage names are primarily for debugging purposes.
    // - The type of value transferred in a pipe is determined here by the explicit type `|value: String|`
    // - The task definition (last argument) returns a Vec<Option<T>>
    //     - One Option<T> for each pipe writer provided, respective of the order specified
    //     - The Option<T> determines if the task "produced output" for that pipe (i.e. Providing `None`
    //       causes nothing to be written to the respective output)
    pipeline.register_stage(
        "MapStage",
        map_input_r,
        vec![map_to_reduce_w],
        |value: String| async move {
            let new_value = format!("{}!", value);
            vec![Some(new_value)]
        },
    );

    // Variables can be updated by a task by wrapping it in a `Mutex` (to make it mutable) and then in
    // an `Arc` (for data ownership between more than one thread).
    let total_count = Arc::new(Mutex::new(0));
    let reduce_total_count = total_count.clone();

    pipeline.register_stage(
        "ReduceStage",
        map_to_reduce_r,
        vec![],
        |value: String| async move {
            *reduce_total_count.lock().await += value.len();
            Vec::<Option<()>>::new()
        },
    );

    // Provide the pipeline with initial data, and then wait on the pipeline to complete.
    for value in ["a", "bb", "ccc"] {
        map_input_w.write(value.to_string()).await;
    }
    pipeline.wait().await;

    // We see that after the data goes through our map and reduce stages, we effectively get this:
    //
    //     len("a!") + len("bb!") + len("ccc!") = 9
    //
    assert_eq!(*total_count.lock().await, 9);
}
```

## Documentation

WIP

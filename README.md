# Async Pipes

Create a lightweight, concurrent data processing pipeline for Rust applications.

## Description

Async Pipes provides a simple way to create high-throughput data processing pipelines by utilizing Rust's
asynchronous runtime capabilities. This is done by this library providing the infrastructure for managing
asynchronous tasks and data transfer between the tasks so the developer only has to worry about the task-specific
implementation for each stage in the pipeline.

## Getting Started

_WIP_

#### Simple, Linear Pipeline Example

```rust
use std::sync::Arc;
use tokio::sync::Mutex;
use async_pipes::Pipeline;
use async_pipes::atomic_mut;

#[tokio::main]
async fn main() {
    // Create the pipeline from a list of pipe names.
    let (mut pipeline, mut pipes) =
        Pipeline::from_pipes(vec!["MapInput", "MapToReduce", "ReduceToLog"]);

    // We create "writers" (*_w) and "readers" (*_r) to transfer data
    let (map_input_w, map_input_r) = pipes.create_io("MapInput").unwrap();
    let (map_to_reduce_w, map_to_reduce_r) = pipes.create_io("MapToReduce").unwrap();

    // After creating the pipes, stage workers are registered in the pipeline.
    pipeline.register_inputs("Producer", map_input_w, vec!["a", "bb", "ccc"]);

    // We return an option to tell the stage whether to write `new_value` to the pipe or ignore it.
    pipeline.register("MapStage", map_input_r, map_to_reduce_w, |value: &'static str| async move {
        let new_value = format!("{}!", value);
        Some(new_value)
    });

    // Variables can be updated in a task by wrapping it in a `Mutex` (to make it mutable)
    // and then in an `Arc` (for data ownership across task executions).
    let total_count = atomic_mut(0);
    let reduce_total_count = total_count.clone();
    pipeline.register_consumer("ReduceStage", map_to_reduce_r, |value: String| async move {
        *reduce_total_count.lock().await += value.len();
    });

    pipeline.wait().await;

    // We see that after the data goes through our map and reduce stages,
    // we effectively get this: `len("a!") + len("bb!") + len("ccc!") = 9`
    assert_eq!(*total_count.lock().await, 9);
}
```

## Documentation

WIP

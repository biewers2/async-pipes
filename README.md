# Async Pipes

Create a lightweight, concurrent data processing pipeline for Rust applications.

## Description

Async Pipes provides a simple way to create high-throughput data processing pipelines by utilizing Rust's
asynchronous runtime capabilities. This is done by this library providing the infrastructure for managing
asynchronous tasks and data transfer between the tasks so the developer only has to worry about the task-specific
implementation for each stage in the pipeline.

For information on getting started with Async Pipes, see the [documentation](#documentation).

## Documentation

[Async Pipes - Docs.rs](https://docs.rs/async-pipes/0.1.0/async_pipes/)

#### Simple, Linear Pipeline Example

```rust
#[tokio::main]
async fn main() {
    let total_count = Arc::new(AtomicUsize::new(0));
    let task_total_count = total_count.clone();

    Pipeline::builder()
        .with_inputs("MapPipe", vec!["a", "bb", "ccc"])
        .with_stage("MapPipe", "ReducePipe", |value: &'static str| async move {
            Some(format!("{}!", value))
        })
        .with_consumer("ReducePipe", move |value: String| {
            let total_count = task_total_count.clone();
            async move {
                total_count.fetch_add(value.len(), Ordering::SeqCst);
            }
        })
        .build()
        .expect("failed to build pipeline!")
        .wait()
        .await;

    assert_eq!(total_count.load(Ordering::Acquire), 9);
}
```

# Async Pipes

Create a lightweight, concurrent data processing pipeline for Rust applications.

## Description

Async Pipes provides a simple way to create high-throughput data processing pipelines by utilizing Rust's
asynchronous runtime capabilities. This is done by this library providing the infrastructure for managing
asynchronous tasks and data transfer between the tasks so the developer only has to worry about the task-specific
implementation for each stage in the pipeline.

This library distinguishes itself from manually crafted pipelines (i.e. using channels) by being
able to handle cyclic channels and by being scalable through spawning concurrent tasks.

For information on getting started with Async Pipes, see the [documentation](#documentation).

## Documentation

[Async Pipes - Docs.rs](https://docs.rs/async-pipes/latest/async_pipes/)

#### Simple, Linear Pipeline Example

```rust
#[tokio::main]
async fn main() {
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
```

#### Branching, Cyclic Pipeline Example (e.g. Web Crawler)

```rust
#[tokio::main]
async fn main() {
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
```
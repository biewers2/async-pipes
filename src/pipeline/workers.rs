use std::future::Future;

use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinSet;

use crate::pipeline::io::{PipeReader, PipeWriter};
use crate::pipeline::{
    check_join_result, write_results, IterCastFn, ProducerFn, StageWorkerSignal, TaskFn,
};
use crate::{BoxedAnySend, MultiTaskWorkerOpts, SingleTaskWorkerOpts, WorkerOpts};

/// Create a new worker that produces values until there are no more to produce.
pub async fn new_detached_producer(
    mut producer: ProducerFn,
    writers: Vec<PipeWriter<BoxedAnySend>>,
) {
    while let Some(results) = (*producer)().await {
        write_results(&writers, results).await;
    }
}

/// Create a new worker that runs and manages tasks.
pub async fn new_detached_worker(
    task: TaskFn,
    reader: PipeReader<BoxedAnySend>,
    writers: Vec<PipeWriter<BoxedAnySend>>,
    signal_rx: Receiver<StageWorkerSignal>,
    options: WorkerOpts,
) {
    match options {
        WorkerOpts::SingleTask(options) => {
            new_detached_single_task_worker(
                reader,
                writers,
                signal_rx,
                options,
                move |value, writers| {
                    let results = task(value);
                    async move {
                        if let Some(results) = results.await {
                            write_results(&writers, results).await;
                        }
                    }
                },
            )
            .await;
        }

        WorkerOpts::MultiTask(options) => {
            new_detached_multi_task_worker(
                reader,
                writers,
                signal_rx,
                options,
                move |value, writers| {
                    let results = task(value);
                    async move {
                        if let Some(results) = results.await {
                            write_results(&writers, results).await;
                        }
                    }
                },
            )
            .await;
        }
    }
}

/// Create a new worker that "flattens" data from one pipe to another.
pub async fn new_detached_flattener(
    caster: IterCastFn,
    reader: PipeReader<BoxedAnySend>,
    writers: Vec<PipeWriter<BoxedAnySend>>,
    signal_rx: Receiver<StageWorkerSignal>,
    options: WorkerOpts,
) {
    new_detached_single_task_worker(
        reader,
        writers,
        signal_rx,
        options.into_single_task_worker_options(),
        move |value, writers| {
            let values = caster(value);
            async move {
                for value in values {
                    write_results(&writers, vec![Some(value)]).await;
                }
            }
        },
    )
    .await;
}

/// Creates a new worker that runs tasks synchronously.
async fn new_detached_single_task_worker<F, Fut>(
    mut reader: PipeReader<BoxedAnySend>,
    writers: Vec<PipeWriter<BoxedAnySend>>,
    mut signal_rx: Receiver<StageWorkerSignal>,
    _options: SingleTaskWorkerOpts,
    new_task: F,
) where
    F: Fn(BoxedAnySend, Vec<PipeWriter<BoxedAnySend>>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    loop {
        select! {
            Some((value, _consumed)) = reader.read() => {
                let writers = writers.clone();
                new_task(value, writers).await;
            }

            // Receive external signals
            Some(signal) = signal_rx.recv() => {
                match signal {
                    StageWorkerSignal::Terminate => break,
                }
            },
        }
    }
}

/// Creates a new worker that runs tasks asynchronously by spawning tasks to a [JoinSet].
async fn new_detached_multi_task_worker<F, Fut>(
    mut reader: PipeReader<BoxedAnySend>,
    writers: Vec<PipeWriter<BoxedAnySend>>,
    mut signal_rx: Receiver<StageWorkerSignal>,
    options: MultiTaskWorkerOpts,
    task: F,
) where
    F: Fn(BoxedAnySend, Vec<PipeWriter<BoxedAnySend>>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let MultiTaskWorkerOpts { max_task_count, .. } = &options;

    let mut tasks = JoinSet::new();
    loop {
        select! {
            Some((value, consumed)) = reader.read(), if tasks.len() < *max_task_count => {
                let writers = writers.clone();
                let task = task(value, writers);

                tasks.spawn(async move {
                    // Take ownership in order to drop it once task ends
                    let _c = consumed;
                    task.await
                });
            }

            // Join tasks to check for errors
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                check_join_result(result);
            }

            // Receive external signals
            Some(signal) = signal_rx.recv() => {
                match signal {
                    StageWorkerSignal::Terminate => break,
                }
            },
        }
    }
    tasks.shutdown().await;
}

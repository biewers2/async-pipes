use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

use async_pipes::{branch, Pipeline, WorkerOptions};

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
        .with_consumer(
            "pipe",
            WorkerOptions::default_single_task(),
            |value: String| async move {
                assert_eq!(value, "hello!".to_string());
            },
        )
        .build()
        .unwrap()
        .wait()
        .await;

    assert!(written.load(Acquire), "value was not handled by worker!");
}

#[tokio::test]
async fn test_stage_branching_producer() {
    let value_written = Arc::new(AtomicBool::new(false));
    let task_value_written = value_written.clone();
    let double_written = Arc::new(AtomicBool::new(false));
    let task_double_written = double_written.clone();

    let value_sum = Arc::new(AtomicUsize::new(0));
    let task_value_sum = value_sum.clone();
    let double_sum = Arc::new(AtomicUsize::new(0));
    let task_double_sum = double_sum.clone();

    let count = Arc::new(AtomicUsize::new(0));

    Pipeline::builder()
        .with_branching_producer(vec!["Value", "Doubled"], move || {
            let count = count.clone();
            async move {
                let c = count.load(Acquire);
                if c < 10 {
                    count.fetch_add(1, SeqCst);
                    Some(branch![c, c * 2])
                } else {
                    None
                }
            }
        })
        .with_consumer(
            "Value",
            WorkerOptions::default_single_task(),
            move |value: usize| {
                let written = task_value_written.clone();
                let sum = task_value_sum.clone();
                async move {
                    written.store(true, Release);
                    sum.fetch_add(value, SeqCst);
                }
            },
        )
        .with_consumer(
            "Doubled",
            WorkerOptions::default_single_task(),
            move |value: usize| {
                let written = task_double_written.clone();
                let sum = task_double_sum.clone();
                async move {
                    written.store(true, Release);
                    sum.fetch_add(value, SeqCst);
                }
            },
        )
        .build()
        .unwrap()
        .wait()
        .await;

    assert!(
        value_written.load(Acquire),
        "value was not handled by worker!"
    );
    assert!(
        double_written.load(Acquire),
        "value was not handled by worker!"
    );

    assert_eq!(value_sum.load(Acquire), 45);
    assert_eq!(double_sum.load(Acquire), 90);
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
            WorkerOptions::default_single_task(),
            |value: usize| async move { Some(value + 1) },
        )
        .with_consumer(
            "second",
            WorkerOptions::default_single_task(),
            move |value: usize| {
                let written = task_written.clone();
                async move {
                    assert_eq!(value, 2);
                    written.store(true, Release);
                }
            },
        )
        .build()
        .unwrap()
        .wait()
        .await;

    assert!(written.load(Acquire), "value was not handled by worker!");
}

#[tokio::test]
async fn test_stage_flattener() {
    let sum = Arc::new(AtomicUsize::new(0));
    let task_sum = sum.clone();

    Pipeline::builder()
        .with_inputs("first", vec![vec![1usize, 2usize, 3usize]])
        .with_flattener::<Vec<usize>>("first", "second")
        .with_consumer(
            "second",
            WorkerOptions::default_single_task(),
            move |value: usize| {
                let sum = task_sum.clone();
                async move {
                    sum.fetch_add(value, SeqCst);
                }
            },
        )
        .build()
        .unwrap()
        .wait()
        .await;

    assert_eq!(sum.load(Acquire), 6);
}

#[tokio::test]
#[should_panic]
async fn test_stage_propagates_task_panic() {
    Pipeline::builder()
        .with_inputs("pipe", vec![()])
        .with_consumer(
            "pipe",
            WorkerOptions::default_single_task(),
            |_: ()| async move { panic!("AHH!") },
        )
        .build()
        .unwrap()
        .wait()
        .await;
}

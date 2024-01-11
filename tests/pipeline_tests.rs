use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

use async_pipes::{branch, branch_inputs, NoOutput, Pipeline};

#[tokio::test]
async fn pipeline_returns_error_on_no_producer() {
    let error = Pipeline::builder()
        .with_stage("one", "two", |_: ()| async move { Some(()) })
        .with_consumer("two", |_: usize| async move {})
        .build()
        .unwrap_err();

    assert_eq!(error, "pipeline must have at least one producer");
}

#[tokio::test]
async fn pipeline_returns_error_on_open_ended_pipes() {
    let error = Pipeline::builder()
        .with_inputs("one", vec![()])
        .with_stage("one", "two", |_: ()| async move { Some(()) })
        .build()
        .unwrap_err();

    assert_eq!(error, "pipeline has open-ended pipe: 'two'");
}

/// Check that a simple, one-stage, linear pipeline can be created and can transfer data from a
/// pipe's writer (start) to its reader (end).
///
/// Here's the effective layout:
///
///     producer -> consumer
///
#[tokio::test]
async fn simple_linear_pipeline() {
    let written = Arc::new(AtomicBool::new(false));
    let task_written = written.clone();

    Pipeline::builder()
        .with_inputs("one", vec![5usize])
        .with_consumer("one", move |value: usize| {
            let tw = task_written.clone();
            async move {
                assert_eq!(value, 5);
                tw.store(true, Release);
            }
        })
        .build()
        .unwrap()
        .wait()
        .await;

    assert!(written.load(Acquire), "value was not handled by worker!")
}

/// Check that a complex, multi-stage, linear pipeline can be created and can transfer data through
/// the entire pipeline.
///
/// Here's the effective layout:
///
///     producer -> complex0 -> complex1 -> complex2 -> consumer
///
#[tokio::test]
async fn complex_linear_pipeline() {
    let written = Arc::new(AtomicBool::new(false));
    let task_written = written.clone();

    Pipeline::builder()
        .with_inputs("one", vec![1usize])
        .with_stage("one", "two", |value: usize| async move {
            assert_eq!(value, 1);
            Some(value + 1)
        })
        .with_stage("two", "three", |value: usize| async move {
            assert_eq!(value, 2);
            Some(value + 2)
        })
        .with_stage("three", "four", |value: usize| async move {
            assert_eq!(value, 4);
            Some(value + 3)
        })
        .with_consumer("four", move |value: usize| {
            let tw = task_written.clone();
            async move {
                assert_eq!(value, 7);
                tw.store(true, Release);
            }
        })
        .build()
        .unwrap()
        .wait()
        .await;

    assert!(written.load(Acquire), "value was not handled by worker!")
}

/// Test a cycle existing in the pipeline, and if the flow of content of the data is correct at each
/// stage.
///
/// Here's the effective layout:
///
///     producer -> cyclic0 -> cyclic1 -> consumer
///                    ^          |
///                    '----------'
///
#[tokio::test]
async fn cyclic_pipeline() {
    let first_passed = Arc::new(AtomicBool::new(false));
    let task_first_passed = first_passed.clone();

    let written = Arc::new(AtomicBool::new(false));
    let task_written = written.clone();

    Pipeline::builder()
        .with_inputs("one", vec![0usize])
        .with_stage("one", "two", move |value: usize| {
            let first_passed = task_first_passed.clone();

            async move {
                if !first_passed.load(Acquire) {
                    assert_eq!(value, 0);
                } else {
                    assert_eq!(value, 2);
                }
                Some(value + 1)
            }
        })
        .with_branching_stage("two", vec!["one", "three"], move |value: usize| {
            let first_passed = first_passed.clone();
            async move {
                let result = if !first_passed.load(Acquire) {
                    first_passed.store(true, Release);
                    assert_eq!(value, 1);
                    branch![value + 1, NoOutput]
                } else {
                    assert_eq!(value, 3);
                    branch![NoOutput, value + 1]
                };
                Some(result)
            }
        })
        .with_consumer("three", move |value: usize| {
            let tw = task_written.clone();
            async move {
                assert_eq!(value, 4);
                tw.store(true, Release);
            }
        })
        .build()
        .unwrap()
        .wait()
        .await;

    assert!(written.load(Acquire), "value was not handled by worker!")
}

/// Test a pipeline that has many branches and see if the final stage receives all the data.
///
/// Here's the effective layout:
///
///              .> branch1a .
///             /             \
///     producer -> branch1b  -> consumer
///             \             /
///              `> branch1c `
///
#[tokio::test]
async fn branching_pipeline() {
    let total = Arc::new(AtomicUsize::new(0));
    let task_total = total.clone();

    Pipeline::builder()
        .with_branching_inputs(
            vec!["one-a", "one-b", "one-c"],
            branch_inputs![(1usize, 1usize, 1usize)],
        )
        .with_stage("one-a", "two", |value: usize| async move {
            assert_eq!(value, 1);
            Some(value + 1)
        })
        .with_stage("one-b", "two", |value: usize| async move {
            assert_eq!(value, 1);
            Some(value + 1)
        })
        .with_stage("one-c", "two", |value: usize| async move {
            assert_eq!(value, 1);
            Some(value + 1)
        })
        .with_consumer("two", move |value: usize| {
            let total = task_total.clone();
            async move {
                total.fetch_add(value, SeqCst);
            }
        })
        .build()
        .unwrap()
        .wait()
        .await;

    assert_eq!(total.load(Acquire), 6);
}

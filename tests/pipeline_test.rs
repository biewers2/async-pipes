use async_pipes::{atomic_mut, atomic_mut_cloned, Pipeline};

/// Check that a simple, one-stage, linear pipeline can be created and can transfer data from a pipe's
/// writer (start) to its reader (end).
///
/// Here's the effective layout:
///
///     producer -> consumer
///
#[tokio::test]
async fn simple_linear_pipeline() {
    let (mut pipeline, mut pipes) = Pipeline::from_pipes(vec!["one"]);
    let (writer, reader) = pipes.create_io("one").unwrap();

    let (written, worker_written) = atomic_mut_cloned(false);
    pipeline.register_inputs("producer", writer, vec![5]);
    pipeline.register_consumer("consumer", reader, |value: usize| async move {
        assert_eq!(value, 5);
        *worker_written.lock().await = true;
    });
    pipeline.wait().await;

    assert!(*written.lock().await, "value was not handled by worker!")
}

/// Check that a complex, multi-stage, linear pipeline can be created and can transfer data through the
/// entire pipeline.
///
/// Here's the effective layout:
///
///     producer -> complex0 -> complex1 -> complex2 -> consumer
///
#[tokio::test]
async fn complex_linear_pipeline() {
    let pipes = vec!["one", "two", "three", "four"];
    let (mut pipeline, mut pipes) = Pipeline::from_pipes(pipes);

    let (one_w, one_r) = pipes.create_io("one").unwrap();
    let (two_w, two_r) = pipes.create_io("two").unwrap();
    let (three_w, three_r) = pipes.create_io("three").unwrap();
    let (four_w, four_r) = pipes.create_io("four").unwrap();

    let (written, worker_written) = atomic_mut_cloned(false);

    pipeline.register_inputs("producer", one_w, vec![1]);
    pipeline.register("complex0", one_r, two_w, |value: usize| async move {
        assert_eq!(value, 1);
        Some(value + 1)
    });
    pipeline.register("complex1", two_r, three_w, |value: usize| async move {
        assert_eq!(value, 2);
        Some(value + 2)
    });
    pipeline.register("complex2", three_r, four_w, |value: usize| async move {
        assert_eq!(value, 4);
        Some(value + 3)
    });
    pipeline.register_consumer("consumer", four_r, |value: usize| async move {
        assert_eq!(value, 7);
        *worker_written.lock().await = true;
    });
    pipeline.wait().await;

    assert!(*written.lock().await, "value was not handled by worker!")
}

/// Test a cycle existing in the pipeline, and if the flow of content of the data is correct at each stage.
///
/// Here's the effective layout:
///
///     producer -> cyclic0 -> cyclic1 -> consumer
///                    ^          |
///                    '----------'
///
#[tokio::test]
async fn cyclic_pipeline() {
    let pipes = vec!["one", "two", "three"];
    let (mut pipeline, mut pipes) = Pipeline::from_pipes(pipes);

    let (one_w, one_r) = pipes.create_io("one").unwrap();
    let (two_w, two_r) = pipes.create_io("two").unwrap();
    let (three_w, three_r) = pipes.create_io("three").unwrap();

    let (first_passed, cyclic0_first_passed) = atomic_mut_cloned(false);
    let (written, worker_written) = atomic_mut_cloned(false);

    let wk_one_w = one_w.clone();
    pipeline.register_inputs("producer", one_w, vec![0]);
    pipeline.register("cyclic0", one_r, two_w, |value: usize| async move {
        if !*cyclic0_first_passed.lock().await {
            assert_eq!(value, 0);
        } else {
            assert_eq!(value, 2);
        }
        Some(value + 1)
    });
    pipeline.register_branching(
        "cyclic1",
        two_r,
        vec![wk_one_w, three_w],
        |value: usize| async move {
            let mut fp = first_passed.lock().await;
            if !*fp {
                *fp = true;
                assert_eq!(value, 1);
                vec![Some(value + 1), None]
            } else {
                assert_eq!(value, 3);
                vec![None, Some(value + 1)]
            }
        },
    );
    pipeline.register_consumer("consumer", three_r, |value: usize| async move {
        assert_eq!(value, 4);
        *worker_written.lock().await = true;
    });
    pipeline.wait().await;

    assert!(*written.lock().await, "value was not handled by worker!")
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
    let pipes = vec!["Input", "OneA", "OneB", "OneC", "Two"];
    let (mut pipeline, mut pipes) = Pipeline::from_pipes(pipes);

    let (one_a_w, one_a_r) = pipes.create_io("OneA").unwrap();
    let (one_b_w, one_b_r) = pipes.create_io("OneB").unwrap();
    let (one_c_w, one_c_r) = pipes.create_io("OneC").unwrap();
    let (two_w, two_r) = pipes.create_io("Two").unwrap();

    pipeline.register_branching_inputs(
        "producer",
        vec![one_a_w, one_b_w, one_c_w],
        vec![vec![1, 1, 1]],
    );

    let w1a_two_w = two_w.clone();
    let w1b_two_w = two_w.clone();
    let w1c_two_w = two_w.clone();

    pipeline.register("branch1a", one_a_r, w1a_two_w, |value: usize| async move {
        assert_eq!(value, 1);
        Some(value + 1)
    });
    pipeline.register("branch1b", one_b_r, w1b_two_w, |value: usize| async move {
        assert_eq!(value, 1);
        Some(value + 1)
    });
    pipeline.register("branch1c", one_c_r, w1c_two_w, |value: usize| async move {
        assert_eq!(value, 1);
        Some(value + 1)
    });

    let total = atomic_mut(0);
    let worker_total = total.clone();
    pipeline.register_consumer("consumer", two_r, |value: usize| async move {
        *worker_total.lock().await += value;
    });

    pipeline.wait().await;

    assert_eq!(*total.lock().await, 6);
}

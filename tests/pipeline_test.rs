use std::sync::Arc;

use tokio::sync::Mutex;

use plumber::Pipeline;

fn atomic_value<T>(initial_value: T) -> (Arc<Mutex<T>>, Arc<Mutex<T>>) {
    let value = Arc::new(Mutex::new(initial_value));
    (value.clone(), value)
}

/// Check that a simple, one-stage, linear pipeline can be created and can transfer data from a pipe's
/// writer (start) to its reader (end).
///
/// Here's the effective layout:
///
///     (input) -> simple
///
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn simple_linear_pipeline() {
    let pipe = "one";
    let (mut pipeline, mut pipes) = Pipeline::from_pipes(vec![pipe]);

    let (writer, reader) = pipes.create_io(pipe).unwrap();

    let (written, worker_written) = atomic_value(false);
    pipeline.register_worker("simple", reader, vec![], |value: usize| async move {
        assert_eq!(value, 5);
        *worker_written.lock().await = true;
        Vec::<Option<()>>::new()
    });

    writer.write(5).await;
    pipeline.wait().await;

    assert!(*written.lock().await, "value was not handled by worker!")
}

/// Check that a complex, multi-stage, linear pipeline can be created and can transfer data through the
/// entire pipeline.
///
/// Here's the effective layout:
///
///     (input) -> complex0 -> complex1 -> complex2 -> complex3
///
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn complex_linear_pipeline() {
    let pipes = vec!["one", "two", "three", "four"];
    let (mut pipeline, mut pipes) = Pipeline::from_pipes(pipes);

    let (one_w, one_r) = pipes.create_io("one").unwrap();
    let (two_w, two_r) = pipes.create_io("two").unwrap();
    let (three_w, three_r) = pipes.create_io("three").unwrap();
    let (four_w, four_r) = pipes.create_io("four").unwrap();

    let (written, worker_written) = atomic_value(false);

    pipeline.register_worker("complex0", one_r, vec![two_w], |value: usize| async move {
        assert_eq!(value, 1);
        vec![Some(value + 1)]
    });
    pipeline.register_worker(
        "complex1",
        two_r,
        vec![three_w],
        |value: usize| async move {
            assert_eq!(value, 2);
            vec![Some(value + 2)]
        },
    );
    pipeline.register_worker(
        "complex2",
        three_r,
        vec![four_w],
        |value: usize| async move {
            assert_eq!(value, 4);
            vec![Some(value + 3)]
        },
    );
    pipeline.register_worker("complex3", four_r, vec![], |value: usize| async move {
        assert_eq!(value, 7);
        *worker_written.lock().await = true;
        Vec::<Option<()>>::new()
    });

    one_w.write(1).await;
    pipeline.wait().await;

    assert!(*written.lock().await, "value was not handled by worker!")
}

/// Test a cycle existing in the pipeline, and if the flow of content of the data is correct at each stage.
///
/// Here's the effective layout:
///
///     cyclic0 -> cyclic1 -> cyclic2
///        ^          |
///        '----------'
///
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cyclic_pipeline() {
    let pipes = vec!["one", "two", "three"];
    let (mut pipeline, mut pipes) = Pipeline::from_pipes(pipes);

    let (one_w, one_r) = pipes.create_io("one").unwrap();
    let (two_w, two_r) = pipes.create_io("two").unwrap();
    let (three_w, three_r) = pipes.create_io("three").unwrap();

    let (first_passed, cyclic0_first_passed) = atomic_value(false);
    let (written, worker_written) = atomic_value(false);

    pipeline.register_worker("cyclic0", one_r, vec![two_w], |value: usize| async move {
        if !*cyclic0_first_passed.lock().await {
            assert_eq!(value, 0);
        } else {
            assert_eq!(value, 2);
        }
        vec![Some(value + 1)]
    });

    let worker_one_w = one_w.clone();
    pipeline.register_worker(
        "cyclic1",
        two_r,
        vec![worker_one_w, three_w],
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
    pipeline.register_worker("cyclic2", three_r, vec![], |value: usize| async move {
        assert_eq!(value, 4);
        *worker_written.lock().await = true;
        Vec::<Option<()>>::new()
    });

    one_w.write(0).await;
    pipeline.wait().await;

    assert!(*written.lock().await, "value was not handled by worker!")
}

/// Test a pipeline that has many branches and see if the final stage receives all the data.
///
/// Here's the effective layout:
///
///             .> branch1a .
///            /             \
///     branch0 -> branch1b  -> branch2
///            \             /
///             `> branch1c `
///
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn branching_pipeline() {
    let pipes = vec!["Input", "OneA", "OneB", "OneC", "Two"];
    let (mut pipeline, mut pipes) = Pipeline::from_pipes(pipes);

    let (input_w, input_r) = pipes.create_io("Input").unwrap();
    let (one_a_w, one_a_r) = pipes.create_io("OneA").unwrap();
    let (one_b_w, one_b_r) = pipes.create_io("OneB").unwrap();
    let (one_c_w, one_c_r) = pipes.create_io("OneC").unwrap();
    let (two_w, two_r) = pipes.create_io("Two").unwrap();

    pipeline.register_worker(
        "branch0",
        input_r,
        vec![one_a_w, one_b_w, one_c_w],
        |value: usize| async move {
            assert_eq!(value, 0);
            vec![Some(value + 1), Some(value + 1), Some(value + 1)]
        },
    );

    let w1a_two_w = two_w.clone();
    pipeline.register_worker(
        "branch1a",
        one_a_r,
        vec![w1a_two_w],
        |value: usize| async move {
            assert_eq!(value, 1);
            vec![Some(value + 1)]
        },
    );
    let w1b_two_w = two_w.clone();
    pipeline.register_worker(
        "branch1b",
        one_b_r,
        vec![w1b_two_w],
        |value: usize| async move {
            assert_eq!(value, 1);
            vec![Some(value + 1)]
        },
    );
    let w1c_two_w = two_w.clone();
    pipeline.register_worker(
        "branch1c",
        one_c_r,
        vec![w1c_two_w],
        |value: usize| async move {
            assert_eq!(value, 1);
            vec![Some(value + 1)]
        },
    );

    let total = Arc::new(Mutex::new(0));
    let worker_total = total.clone();
    pipeline.register_worker("branch2", two_r, vec![], |value: usize| async move {
        *worker_total.lock().await += value;
        Vec::<Option<()>>::new()
    });

    input_w.write(0).await;
    pipeline.wait().await;

    assert_eq!(*total.lock().await, 6);
}

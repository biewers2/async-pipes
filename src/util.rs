use std::sync::Arc;
use tokio::sync::Mutex;

/// Less-verbose way to create an [`Arc<Mutex<T>>`] instance.
///
/// This is useful for providing state to stage workers in a [crate::Pipeline].
///
/// When registering stage workers, task definitions require captured values to be
/// [Send] and [Sync], as the task may be executed numerous times and possibly in parallel.
pub fn atomic_mut<T>(initial_value: T) -> Arc<Mutex<T>> {
    Arc::new(Mutex::new(initial_value))
}

/// Less-verbose way to create two [`Arc<Mutex<T>>`]s pointing to the same value.
///
/// This is useful for providing state to stage workers in a [crate::Pipeline] while also being able to
/// access it outside of the pipeline.
///
/// When registering stage workers, task definitions require captured values to be
/// [Send] and [Sync], as the task may be executed numerous times and possibly in parallel.
pub fn atomic_mut_cloned<T>(initial_value: T) -> (Arc<Mutex<T>>, Arc<Mutex<T>>) {
    let value = Arc::new(Mutex::new(initial_value));
    (value.clone(), value)
}

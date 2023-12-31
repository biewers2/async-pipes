## 0.2.0 (Dec 31, 2023)

Complete redesign of how pipelines are built. Using the same understanding of how to manage and run workers/tasks,
the interface and implementation was completely redesigned and is now based on a `Builder` design pattern.
* `Pipeline::builder` is now used to construct new pipelines.
* All `Pipeline::register_*` methods are now moved to the builder, taking the form `PipelineBuilder::with_*`.
* Rather than using static dispatching to define the types for each pipe, dynamic dispatching is now used, utilizing
  Rust's `Any` type to create a boxed `Any + Send` type called `BoxedAnySend` (creative, right?).
* Pipe writers and readers are no longer part of the public interface.
  * Pipes are created internally based on the collective set of pipe names the user provides to the builder.
  * Pipe names (unique) are used during stage construction to determine which pipes hook up to what workers.

## 0.1.0 (Dec 29, 2023)

* Initial release
  * :celebrate:

#![deny(missing_docs)]
#![doc = include_str!("../README.md")]
//! A small, dedicated Tokio runtime for CPU-bound asynchronous work.
//!
//! [`CpuRuntime`] keeps CPU-heavy futures away from a latency-sensitive
//! application runtime. Its worker count is capped by a caller-provided
//! reserve, and workers are assigned [`qos_threads::Qos::Low`] when they
//! start.

use std::fmt;
use std::future::Future;

use qos_threads::Qos;

/// A dedicated Tokio runtime for CPU-bound asynchronous work.
///
/// Construct one runtime for a process and share its [`tokio::runtime::Handle`]
/// with owned components that need to schedule CPU work. The runtime uses
/// `max(1, available_parallelism - reserve)` workers, so a machine always
/// retains at least `reserve` cores for other work when enough cores exist.
pub struct CpuRuntime {
    /// `Some` until [`Drop`]. Taking the runtime during drop allows the
    /// non-blocking `shutdown_background` path from an async context.
    runtime: Option<tokio::runtime::Runtime>,
    threads: usize,
}

impl CpuRuntime {
    /// Builds a runtime with a reserved number of cores.
    ///
    /// Each worker is named `tokio-cpu-<n>` and assigned low `QoS` at startup.
    ///
    /// # Errors
    ///
    /// Returns [`CpuRuntimeError`] if Tokio cannot create the worker runtime.
    pub fn new(reserve: usize) -> Result<Self, CpuRuntimeError> {
        let parallelism = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let threads = parallelism.saturating_sub(reserve).max(1);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name_fn({
                let next = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                move || {
                    let worker = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    format!("tokio-cpu-{worker}")
                }
            })
            .on_thread_start(|| {
                let _ = qos_threads::set_current_thread(Qos::Low);
            })
            .enable_all()
            .build()
            .map_err(|error| CpuRuntimeError(error.to_string()))?;

        Ok(Self {
            runtime: Some(runtime),
            threads,
        })
    }

    /// Returns the number of worker threads configured for this runtime.
    #[must_use]
    pub const fn threads(&self) -> usize {
        self.threads
    }

    /// Spawns a CPU-bound future onto this runtime.
    ///
    /// Dropping the returned handle detaches the task, matching
    /// [`tokio::runtime::Handle::spawn`] semantics.
    ///
    /// # Panics
    ///
    /// Panics if the runtime has started shutting down.
    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime
            .as_ref()
            .expect("CPU runtime remains available until drop")
            .spawn(future)
    }

    /// Runs a CPU-bound future on this runtime and awaits its result.
    ///
    /// # Panics
    ///
    /// Propagates a panic from the future, or panics if shutdown drops the
    /// task before it produces a result.
    pub async fn run<F>(&self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self.spawn(future).await {
            Ok(value) => value,
            Err(join_error) => match join_error.try_into_panic() {
                Ok(panic) => std::panic::resume_unwind(panic),
                Err(join_error) => panic!("CPU runtime dropped the task: {join_error}"),
            },
        }
    }

    /// Returns a cloneable handle for scheduling work from owned state.
    ///
    /// # Panics
    ///
    /// Panics if the runtime has started shutting down.
    #[must_use]
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.runtime
            .as_ref()
            .expect("CPU runtime remains available until drop")
            .handle()
            .clone()
    }
}

impl Drop for CpuRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

impl fmt::Debug for CpuRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CpuRuntime")
            .field("threads", &self.threads)
            .finish_non_exhaustive()
    }
}

/// The CPU runtime could not be built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CpuRuntimeError(String);

impl fmt::Display for CpuRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "failed to build CPU runtime: {}", self.0)
    }
}

impl std::error::Error for CpuRuntimeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runs_future_and_returns_value() {
        let runtime = CpuRuntime::new(0).expect("test runtime should build");
        assert_eq!(runtime.run(async { 20 + 22 }).await, 42);
    }

    #[tokio::test]
    async fn runs_many_tasks() {
        let runtime = CpuRuntime::new(0).expect("test runtime should build");
        let mut total = 0_u64;
        for value in 0..100_u64 {
            total += runtime.run(async move { value * 2 }).await;
        }
        assert_eq!(total, (0..100).map(|value: u64| value * 2).sum());
    }

    #[test]
    fn reserve_larger_than_parallelism_still_leaves_one_worker() {
        let runtime = CpuRuntime::new(usize::MAX).expect("test runtime should build");
        assert_eq!(runtime.threads(), 1);
    }

    #[test]
    fn reserve_zero_uses_all_available_parallelism() {
        let parallelism = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let runtime = CpuRuntime::new(0).expect("test runtime should build");
        assert_eq!(runtime.threads(), parallelism);
    }

    #[tokio::test]
    async fn workers_use_generic_names() {
        let runtime = CpuRuntime::new(0).expect("test runtime should build");
        let name = runtime
            .run(async {
                std::thread::current()
                    .name()
                    .map(str::to_owned)
                    .unwrap_or_default()
            })
            .await;
        assert!(
            name.starts_with("tokio-cpu-"),
            "CPU worker must be named tokio-cpu-<n>, got {name:?}"
        );
    }

    #[tokio::test]
    async fn propagates_task_panics() {
        let joined = tokio::spawn(async {
            let runtime = CpuRuntime::new(0).expect("test runtime should build");
            runtime.run(async { panic!("CPU work failed") }).await
        })
        .await;
        assert!(joined.is_err(), "task panic must surface to the awaiter");
    }
}

# tokio-cpu-runtime

[![Crates.io](https://img.shields.io/crates/v/tokio-cpu-runtime.svg)](https://crates.io/crates/tokio-cpu-runtime)
[![Downloads](https://img.shields.io/crates/d/tokio-cpu-runtime.svg)](https://crates.io/crates/tokio-cpu-runtime)
[![Documentation](https://docs.rs/tokio-cpu-runtime/badge.svg)](https://docs.rs/tokio-cpu-runtime)
[![CI](https://github.com/legra-ai/tokio-cpu-runtime/actions/workflows/ci.yml/badge.svg)](https://github.com/legra-ai/tokio-cpu-runtime/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

A small, dedicated Tokio runtime for CPU-bound asynchronous work.

## Why a second runtime?

Tokio's normal runtime often serves latency-sensitive networking and control
work. Long CPU-bound futures can occupy those workers and increase response
latency. `tokio-cpu-runtime` provides a separate multi-thread runtime for that
work while keeping the caller's runtime responsive.

The runtime:

- caps workers at `max(1, available_parallelism - reserve)`;
- keeps at least one worker even when the reserve exceeds machine capacity;
- names workers `tokio-cpu-<n>`;
- assigns workers low `QoS` through [`qos-threads`](https://crates.io/crates/qos-threads);
- can be awaited from another Tokio runtime;
- shuts down without blocking the dropping async task.

## Example

```rust
use tokio_cpu_runtime::CpuRuntime;

#[tokio::main]
async fn main() -> Result<(), tokio_cpu_runtime::CpuRuntimeError> {
    let cpu = CpuRuntime::new(2)?;
    let answer = cpu.run(async { 20 + 22 }).await;

    assert_eq!(answer, 42);
    Ok(())
}
```

For owned components, clone [`CpuRuntime::handle`](https://docs.rs/tokio-cpu-runtime/latest/tokio_cpu_runtime/struct.CpuRuntime.html#method.handle) and use that handle to
spawn work without borrowing the runtime owner.

## Scope

This crate schedules asynchronous CPU-bound futures. It does not make blocking
I/O asynchronous and does not replace Tokio's blocking-task facilities for
unavoidable synchronous operations.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)); or
- MIT License ([LICENSE-MIT](LICENSE-MIT)).

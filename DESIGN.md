# Design Document: Bounded Asynchronous Worker Pool

## Overview
This library provides a thread-safe, memory-bounded asynchronous worker pool for executing arbitrary futures. It is designed for high-concurrency environments where system stability depends on strict resource limits and backpressure propagation.

## Concurrency Model
The architecture follows a **Decoupled Pipeline** model consisting of three distinct stages:

### 1. The Ingress Layer (Producer)
*   **Backpressure via Bounded Channels:** I have utilized a bounded `async-channel`. When the internal job queue reaches its capacity, the `submit()` method asynchronously yields. This suspends the caller without blocking the underlying OS thread, naturally propagating backpressure to the source.
*   **Uniform Interface:** To handle heterogeneous async tasks, I utilized the **Trait Objects** (`dyn Future`). Jobs are **Boxed** (for heap allocation and known size) and **Pinned** (to ensure memory safety for self-referential async state machines).

### 2. The Buffer (Coordination)
*   **MPMC Queue:** I have used a Multi-Producer Multi-Consumer (MPMC) channel. Unlike a standard `mpsc` queue wrapped in a `Mutex`, MPMC allows multiple workers to contend for jobs using atomic operations at the driver level, significantly reducing lock contention.

### 3. The Execution Plane (Consumers)
*   **Fixed Resource Footprint:** On initialization, the pool spawns exactly `N` long-lived Tokio tasks. This ensures that memory and CPU overhead remain constant regardless of the volume of submitted jobs, preventing "spawn-burst" resource exhaustion.
*   **Worker Lifecycle:** Each worker sits in a non-blocking `recv()` loop. Upon completion of a task, the worker immediately becomes available for the next queued job.

## Key Features

### Graceful Shutdown
The shutdown process is handled via **Sender Dropping**.
1.  When `shutdown()` is invoked, the `Sender` is removed from the internal `Option` and dropped. 
2.  This signals to the channel that no further jobs will be submitted.
3.  Workers continue to drain the remaining jobs in the buffer. 
4.  Once the buffer is empty, `recv()` returns a `Closed` error, and the worker tasks exit their loops.
5.  The pool awaits all `JoinHandles` to ensure zero in-flight data loss before returning.

### Lock-Free Metrics
Observability is implemented using `std::sync::atomic::AtomicUsize`. 
*   Counters for **Queued**, **Active**, and **Completed** jobs are updated using `Ordering::SeqCst`.
*   This provides a 100% lock-free telemetry system, ensuring that monitoring the pool's health does not impact the throughput of the workers.

### Dynamic Scaling
The pool supports **Scaling Up** at runtime. By cloning the internal `Receiver` and moving it into new Tokio tasks, additional workers can join the consumer group seamlessly without interrupting active processing.

## Trade-offs and Decisions

### Simplicity vs. Performance
The `async-channel` has been chosen over a manual `VecDeque + Mutex + Condvar` implementation. While a manual implementation would reduce dependencies, it is significantly more prone to deadlocks and usually results in higher lock contention compared to the highly optimized atomic-wait queues in `async-channel`.

### Correctness vs. Speed
The shutdown implementation prioritizes **Correctness**. By joining every worker handle, we ensure that the application does not exit until every job has either finished or failed. This introduces a slight latency in the shutdown sequence but guarantees data integrity.

## Known Limitations
*   **Scaling Down:** The current implementation supports adding workers, but not removing them dynamically. Removing workers safely would require a "Poison Pill" pattern to ensure a specific worker exits only after finishing its current task.
*   **Job Cancellation:** Once a job is submitted, there is no external mechanism to cancel that specific future.

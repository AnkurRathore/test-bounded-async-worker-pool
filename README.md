# Bounded Async Worker Pool

## Summary

You have been tasked with building a **bounded asynchronous worker pool** in Rust.

The system receives jobs dynamically and processes them concurrently. However,
to ensure system stability and predictable memory usage, the number of
concurrently executing jobs must be strictly limited.

The solution must apply **backpressure** when the system is at capacity and must
support **graceful shutdown** without losing in-flight work.

All code must be non-blocking and designed for high concurrency.

## Requirements

### Core Functionality

* Accept jobs dynamically at runtime
* Process jobs asynchronously
* Allow at most **N jobs** to execute concurrently
* When the system is at capacity, new jobs must wait
* Support graceful shutdown:
  * Stop accepting new jobs
  * Allow in-flight jobs to complete

### API Expectations

Your solution should expose a clear API similar to:

```rust
pool.submit(job).await;
pool.shutdown().await;
```

The exact API design is up to you, but it should be simple, clear, and safe to
use from asynchronous contexts.

### Constraints
* Blocking calls are not allowed
* Busy waiting is forbidden
* Unbounded queues are not allowed
* Spawning unlimited tasks is not allowed
* Memory usage must remain bounded
* The worker pool must behave correctly under load

### Additional Requirements

* Your source should contain both unit and integration tests where necessary
* All code must be formatted using the standard formatting tool
* Code must compile without clippy errors
* The solution must use safe Rust only

### Design & Reasoning (Required)

* Along with the code, include a document (for example DESIGN.md) explaining:
* The overall architecture and concurrency model
* How backpressure is implemented
* How graceful shutdown is handled
* Trade-offs between simplicity, performance, and correctness
* Known limitations of the solution
* Submissions without a design explanation will not be reviewed.

### Submission

Please fork this repository to your own GitHub account and submit a pull request
to your own repository.

Your pull request should include:
* A clear description of your approach
* Any assumptions or trade-offs made
* Instructions on how to run tests

A link to the pull request can be submitted once it is ready for review.

### Bonus

* Ability to dynamically resize the worker pool
* Metrics for active, queued, and completed jobs
* GitHub Actions to run formatting, tests, and clippy checks on each push
* Pre-commit hooks to enforce formatting and linting

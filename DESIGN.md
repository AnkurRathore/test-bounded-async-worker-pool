### The Architectural Blueprint

We can think of this as a pipeline with three distinct stages: **The Ingress, The Buffer, and The Execution Plane.**

#### 1. Ingress (The Client Interface)
*   **API:** `submit<F>(job: F)`
*   **Behavior:** Takes an `async` block. We box and pin the future so it becomes our generic `Job` type.
*   **Backpressure Handling:** This is where we should use a bounded channel's `.send().await` method. If the queue is full, `.send().await` automatically yields the execution task. It essentially pauses the client submission until a slot opens up in the queue. No busy-waiting, no memory spikes.

#### 2. The Buffer (Memory Constraints)
*   **Data Structure:** A Bounded MPMC (Multi-Producer Multi-Consumer) channel.
*   **Why MPMC?** Because we have `N` workers pulling from it. If we used standard `mpsc` we would have to wrap it in a Mutex, which would bottleneck the workers. `async-channel` handles this concurrently with atomic locking at the driver level, ensuring workers can pull jobs instantly without blocking each other.

#### 3. The Execution Plane (The Workers)
*   **Model:** A fixed pool of `N` long-lived worker tasks. 
*   **Behavior:** Instead of spawning tasks dynamically as jobs arrive, we spawn exactly `N` tasks on `new()`. Each worker sits in a continuous `while let Ok(job) = rx.recv().await` loop. As soon as a worker finishes `job.await`, it immediately loops back to `rx.recv().await` to grab the next job. 
*   **Scale:** Because the workers are fixed, the memory and compute resource usage remains perfectly bounded.

#### 4. The Lifecycle Management (Shutdown)
*   **Mechanism:** When `shutdown()` is called, we take/drop the sender half of the channel.
*   **The Chain Reaction:** Closing the sender tells the workers "No more jobs are coming, and the channel is empty." The workers finish their current in-flight job, check the channel, get a `ChannelClosed` error, and cleanly `break` the loop, completing their task gracefully. We then `join` all worker handles to ensure the program waits until the last in-flight job safely completes before the main program exits!

use async_channel::{Sender, bounded};
use std::future::Future;
use std::pin::Pin;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// A Job is boxed, pinned future that can be sent across threads.
pub type Job = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// A Bounded Asynchronous Worker Pool
pub struct WorkerPool {
    /// The sender end of the channel to send jobs to the worker pool.
    /// We use a Mutex to allow for mutable access to the sender across async contexts.
    sender: Mutex<Option<Sender<Job>>>,
    /// A vector of JoinHandles for the worker tasks.
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl WorkerPool {
    /// Creates a new WorkerPool with the specified number of workers and job queue capacity
    /// # `num_workers` - The number of worker tasks to spawn.
    /// # `queue_capacity` - The maximum number of jobs that can be queued at once
    pub fn new(num_workers: usize, queue_capacity: usize) -> Self {
        //Create a bounded channel for job queueing
        let (sender, receiver) = bounded::<Job>(queue_capacity);

        let mut workers = Vec::with_capacity(num_workers);

        // Spawn worker tasks
        for _ in 0..num_workers {
            let rx_clone = receiver.clone();

            let handle = tokio::spawn(async move {
                // The worker sits in this loop, waiting for jobs to execute
                while let Ok(job) = rx_clone.recv().await {
                    job.await;
                }
            });
            workers.push(handle);
        }
        Self {
            sender: Mutex::new(Some(sender)),
            workers: Mutex::new(workers),
        }
    }

    /// Submits a job to the worker pool for execution.
    /// # `job` - The job to be executed, which is a boxed future.
    /// # Returns - A Result indicating whether the job was successfully submitted or if the pool is
    pub async fn submit<F>(&self, job: F) -> Result<(), &'static str>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Lock the sender to ensure thread-safe access
        let sender_guard = self.sender.lock().await;

        if let Some(tx) = sender_guard.as_ref() {
            // Box and Pin the job to fit the Job type
            let boxed_job: Job = Box::pin(job);

            //Send the job to the channel, returning an error if the channel is full
            // Suspend this if channel is full, applying backpressure to the submitter
            if tx.send(boxed_job).await.is_err() {
                return Err("Failed to submit job: Worker pool has been shut down");
            }
            Ok(())
        } else {
            Err("Cannot submit job: Worker pool has been shut down")
        }
    }

    /// Shuts down the worker pool gracefully, waiting for all workers to finish their current jobs.
    /// After calling this method, no new jobs can be submitted to the pool.
    pub async fn shutdown(&self) {
        // Drop the sender
        {
            let mut sender_guard = self.sender.lock().await;
            // Take the sender out of the Option, effectively dropping it and closing the channel
            let _ = sender_guard.take();
        }
        // 2 Join all worker handles
        let mut workers_guard = self.workers.lock().await;
        let mut handles = Vec::new();
        std::mem::swap(&mut *workers_guard, &mut handles);
        for handle in handles {
            // Await each worker to ensure they finish processing current jobs
            let _ = handle.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn test_jobs_execute_successfully() {
        let pool = WorkerPool::new(2, 5);
        let counter = Arc::new(AtomicUsize::new(0));

        // Submit 10 jobs that increment the counter
        for _ in 0..10 {
            let c = Arc::clone(&counter);
            pool.submit(async move {
                c.fetch_add(1, Ordering::SeqCst);
            })
            .await
            .unwrap();
        }
        pool.shutdown().await;
        // Assert that all jobs were executed
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[tokio::test]
    async fn test_graceful_shutdown_waits_for_jobs() {
        let pool = WorkerPool::new(2, 10);
        let counter = Arc::new(AtomicUsize::new(0));

        // Submit 5 jobs that take some time to complete
        for _ in 0..5 {
            let c = Arc::clone(&counter);
            pool.submit(async move {
                sleep(Duration::from_millis(100)).await; // Simulate work
                c.fetch_add(1, Ordering::SeqCst);
            })
            .await
            .unwrap();
        }
        // Shutdown the pool while jobs are still running
        pool.shutdown().await;
        // Assert that all jobs were executed before shutdown completed
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn test_submit_after_shutdown_fails() {
        let pool = WorkerPool::new(2, 5);
        pool.shutdown().await;

        // Attempt to submit a job after shutdown
        let result = pool
            .submit(async {
                println!("This job should not be executed");
            })
            .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap(),
            "Cannot submit job: Worker pool has been shut down"
        );
    }
}

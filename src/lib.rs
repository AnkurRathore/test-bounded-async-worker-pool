use async_channel::{Sender, bounded, Receiver};

use std::future::Future;
use std::pin::Pin;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use std::sync::atomic::{AtomicUsize,Ordering};
use std::sync::Arc;

/// A Job is boxed, pinned future that can be sent across threads.
pub type Job = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// A Bounded Asynchronous Worker Pool
pub struct WorkerPool {
    /// The sender end of the channel to send jobs to the worker pool.
    /// We use a Mutex to allow for mutable access to the sender across async contexts.
    sender: Mutex<Option<Sender<Job>>>,
    /// A vector of JoinHandles for the worker tasks.
    workers: Mutex<Vec<JoinHandle<()>>>,
    receiver: Receiver<Job>,
    // Counters for monitoring the number of jobs in different states
    queued_count: Arc<AtomicUsize>,
    active_count: Arc<AtomicUsize>,
    completed_count: Arc<AtomicUsize>,
}

#[derive(Debug,Clone)]
pub struct PoolStats{
    pub queued: usize,
    pub active: usize,
    pub completed: usize,
}



impl WorkerPool {
    /// Creates a new WorkerPool with the specified number of workers and job queue capacity
    /// # `num_workers` - The number of worker tasks to spawn.
    /// # `queue_capacity` - The maximum number of jobs that can be queued at once
    pub fn new(num_workers: usize, queue_capacity: usize) -> Self {
        //Create a bounded channel for job queueing
        let (sender, receiver) = bounded::<Job>(queue_capacity);

        let mut workers = Vec::with_capacity(num_workers);

        let queued_count = Arc::new(AtomicUsize::new(0));
        let active_count = Arc::new(AtomicUsize::new(0));
        let completed_count = Arc::new(AtomicUsize::new(0));

        // Spawn worker tasks
        for _ in 0..num_workers {
            let rx_clone = receiver.clone();
            let a_count = Arc::clone(&active_count);
            let c_count = Arc::clone(&completed_count);
            let q_count = Arc::clone(&queued_count);

            let handle = tokio::spawn(async move {
                // The worker sits in this loop, waiting for jobs to execute
                while let Ok(job) = rx_clone.recv().await {
                    // Update counters
                    q_count.fetch_sub(1, Ordering::SeqCst); // Job is no longer queued
                    a_count.fetch_add(1, Ordering::SeqCst); // Job is now active
                    job.await;
                    a_count.fetch_sub(1, Ordering::SeqCst); // Job is no longer active
                    c_count.fetch_add(1, Ordering::SeqCst); // Job is completed
                }
            });
            workers.push(handle);
        }
        Self {
            sender: Mutex::new(Some(sender)),
            workers: Mutex::new(workers),
            receiver: receiver,
            queued_count,
            active_count,
            completed_count,
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
            self.queued_count.fetch_add(1, Ordering::SeqCst);
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

    // Return current stats of the pool
    pub fn stats(&self) -> PoolStats {
        PoolStats{
            queued: self.queued_count.load(Ordering::SeqCst),
            active: self.active_count.load(Ordering::SeqCst),
            completed: self.completed_count.load(Ordering::SeqCst),
        }
    }

    // Dybnamically add more workers to the pool
    pub async fn add_workers(&self, num_workers: usize) {
        let mut workers_guard = self.workers.lock().await;
        
        for _ in 0..num_workers {
            let rx_clone = self.receiver.clone();
            let a_count = Arc::clone(&self.active_count);
            let c_count = Arc::clone(&self.completed_count);
            let q_count = Arc::clone(&self.queued_count);

            let handle = tokio::spawn(async move {
                while let Ok(job) = rx_clone.recv().await {
                    q_count.fetch_sub(1, Ordering::SeqCst);
                    a_count.fetch_add(1, Ordering::SeqCst);
                    job.await;
                    a_count.fetch_sub(1, Ordering::SeqCst);
                    c_count.fetch_add(1, Ordering::SeqCst);
                }
            });
            workers_guard.push(handle);
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

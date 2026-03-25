use async_channel::{Sender, bounded};
use core::num;
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
}

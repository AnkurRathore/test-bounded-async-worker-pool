use bounded_async_worker_pool::WorkerPool;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    println!("T=0: Creating pool with 2 workers and queue capacity of 3");
    let pool = WorkerPool::new(2, 3);

    // Spawn a background task to submit jobs
    let _pool_clone = &pool; // We can't clone the pool directly without Arc, but we can pass references in scopes

    // Let's submit 6 jobs
    for i in 1..=6 {
        println!("Submitting Job {}", i);
        let res = pool
            .submit(async move {
                println!("   Worker started Job {}", i);
                sleep(Duration::from_secs(2)).await; // Simulate heavy work
                println!("   Worker finished Job {}", i);
            })
            .await;

        if res.is_ok() {
            println!("Job {} successfully pushed to queue", i);
        }
    }

    println!("Initiating graceful shutdown...");
    pool.shutdown().await;
    println!("Shutdown complete. All jobs processed. Exiting program.");
}

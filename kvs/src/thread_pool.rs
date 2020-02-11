use super::Result;
use std::panic::AssertUnwindSafe;
use std::sync::{mpsc::Sender, Arc, Mutex};
use std::thread::JoinHandle;

/// ThreadPool contains basic methods of a thread pool
pub trait ThreadPool {
    /// Create a new thread pool with specified count of threads
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;
    /// Spawn a job with thread pool
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

/// NaiveThreadPool is not really a thread pool
pub struct NaiveThreadPool();

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(NaiveThreadPool())
    }
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = std::thread::spawn(job);
    }
}

enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

/// A thread pool with global task queue
pub struct SharedQueueThreadPool {
    sender: Sender<ThreadPoolMessage>,
    handles: Vec<Option<JoinHandle<()>>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (tx, rx) = std::sync::mpsc::channel::<ThreadPoolMessage>();
        let rx = Arc::new(Mutex::new(rx));
        let mut handles = Vec::with_capacity(threads as usize);
        for _ in 0..threads {
            let rx1 = rx.clone();
            let handle = std::thread::spawn(move || loop {
                let job = rx1.lock().unwrap().recv();
                if job.is_err() {
                    break;
                }
                let job = job.unwrap();
                match job {
                    ThreadPoolMessage::Shutdown => {
                        break;
                    }
                    ThreadPoolMessage::RunJob(f) => {
                        // Use catch_unwind to avoid descreasing of threads.
                        // catch_unwind costs little performance and makes the code clear
                        let _ = std::panic::catch_unwind(AssertUnwindSafe(f));
                    }
                };
            });
            handles.push(Some(handle));
        }
        Ok(SharedQueueThreadPool {
            handles,
            sender: tx,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = self.sender.send(ThreadPoolMessage::RunJob(Box::new(job)));
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.handles.len() {
            let _ = self.sender.send(ThreadPoolMessage::Shutdown);
        }
        for h in self.handles.iter_mut() {
            if let Some(h) = h.take() {
                let _ = h.join();
            }
        }
    }
}

/// rayon::ThreadPool implementation of ThreadPool
pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()
            .map(RayonThreadPool)
            .map_err(|e| super::Error::Rayon(e))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.spawn(job);
    }
}

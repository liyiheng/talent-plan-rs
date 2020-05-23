use std::panic::catch_unwind;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

struct ThreadPool {
    sender: Sender<Msg>,
    handles: Vec<Option<JoinHandle<()>>>,
}

enum Msg {
    Shutdown,
    Job(Box<dyn FnOnce() + Send + 'static>),
}

impl ThreadPool {
    // Create a worker thread and return its JoinHandle
    fn create_thread(rx: Arc<Mutex<Receiver<Msg>>>) -> JoinHandle<()> {
        std::thread::spawn(move || loop {
            let job = rx.lock().unwrap().recv();
            if job.is_err() {
                break;
            }
            let job = job.unwrap();
            match job {
                Msg::Shutdown => break,
                Msg::Job(f) => {
                    // Use catch_unwind here to keep all threads in the pool alive
                    let wrapper = AssertUnwindSafe(f);
                    if let Err(e) = catch_unwind(move || {
                        wrapper();
                    }) {
                        println!("Error occurred:{:?}", e);
                    }
                }
            };
        })
    }
    fn new(threads: u32) -> Result<Self, ()> {
        let (tx, rx) = std::sync::mpsc::channel::<Msg>();
        let rx = Arc::new(Mutex::new(rx));
        let handles = (0..threads)
            .map(|_| Some(Self::create_thread(rx.clone())))
            .collect();
        Ok(ThreadPool {
            handles,
            sender: tx,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = self.sender.send(Msg::Job(Box::new(job)));
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        // Notify all threads to shutdown
        for _ in 0..self.handles.len() {
            let _ = self.sender.send(Msg::Shutdown);
        }

        for h in self.handles.iter_mut() {
            if let Some(h) = h.take() {
                h.join().unwrap();
            }
        }
    }
}

struct JobStatus {
    jobs_completed: AtomicU32,
}

fn main() {
    // Create a pool with 4 threads
    let pool = ThreadPool::new(4).unwrap();
    // Send 10 jobs to the pool that would panic
    for _ in 0..10 {
        pool.spawn(move || {
            thread::sleep(Duration::from_millis(50));
            // Panics are OK
            panic!("Oops!");
        });
    }

    let status = Arc::new(JobStatus {
        jobs_completed: AtomicU32::new(0),
    });
    // Run 100 common jobs
    let job_cnt = 100;
    for _ in 0..job_cnt {
        let status_shared = status.clone();
        pool.spawn(move || {
            thread::sleep(Duration::from_millis(10));
            status_shared.jobs_completed.fetch_add(1, Ordering::Release);
        });
    }
    while status.jobs_completed.load(Ordering::Acquire) < job_cnt {
        println!("waiting... ");
        thread::sleep(Duration::from_millis(100));
    }
    println!("All jobs done ");
}

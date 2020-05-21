use std::panic::catch_unwind;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
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
    fn new(threads: u32) -> Result<Self, ()> {
        let (tx, rx) = std::sync::mpsc::channel::<Msg>();
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
            });
            handles.push(Some(handle));
        }
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

fn main() {
    let pool = ThreadPool::new(8).unwrap();
    for i in 0..80 {
        pool.spawn(move || {
            std::thread::sleep(Duration::from_secs(1));
            println!("{}", i);
            if i % 2 == 0 {
                // Panics are OK
                panic!("Oops");
            }
        });
    }
}

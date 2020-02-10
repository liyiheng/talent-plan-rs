use crate::{thread_pool::*, *};
use std::io::BufReader;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

/// Response of a command
#[derive(Deserialize, Serialize)]
pub struct Response {
    /// Value of a key
    pub value: Option<String>,
    /// Error message
    pub error: Option<String>,
}

/// KvServer over TCP
/// ```norun
/// let server = KvServer::new("localhost:1234", pool, db).unwrap();
/// server.start();
/// ```
pub struct KvServer<T: KvsEngine, P: ThreadPool> {
    db: T,
    listener: TcpListener,
    pool: P,
}

impl<T: KvsEngine, P: ThreadPool> KvServer<T, P> {
    /// Create a new server with a thread pool and db
    /// returns error if failed to bind address
    pub fn new<A: ToSocketAddrs>(addr: A, pool: P, db: T) -> Result<KvServer<T, P>> {
        let listener = TcpListener::bind(addr)?;
        Ok(KvServer { db, listener, pool })
    }

    /// Start to listen the address and deal with requests
    pub fn start(&self) {
        for stream in self.listener.incoming() {
            if let Ok(s) = stream {
                let db = self.db.clone();
                self.pool.spawn(move || {
                    Self::handle_stream(db, s);
                });
            } else {
                break;
            }
        }
    }

    fn handle_stream(db: T, s: TcpStream) {
        let mut reader = BufReader::new(s);
        let mut data = vec![];
        loop {
            data.clear();
            let read_result = reader.read_until(b'\n', &mut data);
            if read_result.is_err() {
                break;
            }
            data.pop();
            let response = Self::handle_cmd(&db, &data);
            let mut dat = serde_json::to_vec(&response).unwrap();
            dat.push(b'\n');
            if reader.get_mut().write_all(&dat).is_err() {
                break;
            }
        }
    }

    fn handle_cmd(db: &T, data: &[u8]) -> Response {
        let cmd = serde_json::from_slice(data);
        if cmd.is_err() {
            let e = cmd.err().unwrap().to_string();
            return Response {
                value: None,
                error: Some(e),
            };
        }
        match cmd.unwrap() {
            Command::Get(k) => match db.get(k.to_owned()) {
                Ok(v) => Response {
                    value: v,
                    error: None,
                },
                Err(e) => Response {
                    value: None,
                    error: Some(e.to_string()),
                },
            },
            Command::Set(k, v) => match db.set(k.to_owned(), v.to_owned()) {
                Err(e) => Response {
                    value: None,
                    error: Some(e.to_string()),
                },
                Ok(_) => Response {
                    value: None,
                    error: None,
                },
            },
            Command::Rm(k) => match db.remove(k.to_owned()).map_err(|e| e.to_string()) {
                Err(e) => Response {
                    value: None,
                    error: Some(e),
                },
                Ok(_) => Response {
                    value: None,
                    error: None,
                },
            },
        }
    }
}

/// A client to communicate with KvServer
pub struct Client {
    reader: BufReader<TcpStream>,
}

impl Client {
    /// Create a new client from an address
    pub fn new<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let s = TcpStream::connect(addr)?;
        Ok(Client {
            reader: BufReader::new(s),
        })
    }

    /// Send a command to server and returns the response
    pub fn send(&mut self, cmd: &Command) -> Result<Response> {
        let mut dat = serde_json::to_vec(cmd)?;
        dat.push(b'\n');
        self.reader.get_mut().write_all(&dat)?;
        let mut data = vec![];
        self.reader.read_until(b'\n', &mut data)?;
        data.pop();
        let resp = serde_json::from_slice(&data)?;
        Ok(resp)
    }
}

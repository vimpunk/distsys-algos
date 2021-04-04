use crate::types::{Clock, Op, ProcessId, Resource, Timestamp};
use futures::FutureExt;
use rand::Rng;
use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    sync::Arc,
};
use tokio::sync::mpsc;

pub fn new_swarm(process_count: usize, resource: &Arc<Resource>) -> Vec<Process> {
    log::info!("initializing {} processes", process_count);
    let mut processes: Vec<_> = (0..process_count)
        .map(|i| Process::new(ProcessId(i), Arc::clone(&resource)))
        .collect();

    // distribute senders among processes
    for i in 0..processes.len() {
        let i_id = processes[i].id;
        let i_tx = processes[i].tx.clone();
        for k in (0..processes.len()).filter(|k| *k != i) {
            processes[k]
                .others
                .insert(i_id, OtherProcess::new(i_tx.clone()));
            processes[k].clock.tick();
        }
    }

    processes
}

pub fn choose_initial_resource_holder(processes: &mut [Process], mut rng: impl Rng) {
    let initial_res_holder = processes[rng.gen_range(0..processes.len())].id;
    log::info!("initial resource holder: {}", initial_res_holder);
    let initial_request = Timestamp::initial(initial_res_holder);
    for p in processes {
        p.request_queue.insert(initial_request);
    }
}

type Sender = mpsc::UnboundedSender<Message>;
type Receiver = mpsc::UnboundedReceiver<Message>;

#[derive(Clone, Copy, Debug)]
struct Message {
    ts: Timestamp,
    op: Op,
}

pub struct Process {
    id: ProcessId,
    clock: Clock,
    tx: Sender,
    rx: Receiver,
    others: HashMap<ProcessId, OtherProcess>,
    request_queue: BTreeSet<Timestamp>,
    resource: Arc<Resource>,
}

struct OtherProcess {
    tx: Sender,
    last_msg_ts: Option<Clock>,
}

impl OtherProcess {
    pub fn new(tx: Sender) -> Self {
        Self {
            tx,
            last_msg_ts: None,
        }
    }
}

impl Process {
    pub fn new(id: ProcessId, resource: Arc<Resource>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            id,
            clock: Default::default(),
            tx,
            rx,
            others: Default::default(),
            request_queue: Default::default(),
            resource,
        }
    }

    pub async fn run(&mut self) {
        // before sending our request, check if other processes haven't already
        // sent us messages
        // https://github.com/tokio-rs/tokio/issues/3350
        while let Some(Some(msg)) = tokio::task::unconstrained(self.rx.recv()).now_or_never() {
            self.handle_msg(msg);
        }

        // send our request to all other processes if this is not the starting
        // process, in which case it will contain its own request in the queue
        let req_ts = Timestamp::initial(self.id);
        let (req_ts, mut acquired_resource) = if self.request_queue.contains(&req_ts) {
            self.use_resource(req_ts).await;
            (req_ts, true)
        } else {
            let req_ts = self.make_request();
            (req_ts, false)
        };

        while let Some(msg) = self.rx.recv().await {
            log::trace!("{} received message: {:?}", self, msg);

            self.handle_msg(msg);

            if !acquired_resource {
                acquired_resource = self.try_acquire_resource(req_ts).await;
                // TODO: break from loop without breaking other processes
            }
        }
    }

    async fn try_acquire_resource(&mut self, req_ts: Timestamp) -> bool {
        // after each message that is handled, check if the process has
        // seen messages from all processes. we need to hear from all
        // processes before we can act on a request--this is because the
        // total ordering of the system of clocks is only complete when each
        // process has seen all other process' timestamps. further, our
        // request needs to be timestamped earlier than any other message
        // we've heard from.
        let other_msgs_newer_than_request = self.others.values().all(|p| {
            if let Some(last_msg_ts) = p.last_msg_ts {
                last_msg_ts > req_ts.clock
            } else {
                false
            }
        });
        let our_request_is_earliest = self
            .request_queue
            .iter()
            .filter(|r| **r != req_ts)
            .all(|r| *r > req_ts);

        if other_msgs_newer_than_request && our_request_is_earliest {
            self.use_resource(req_ts).await;
            true
        } else {
            false
        }
    }

    async fn use_resource(&mut self, req_ts: Timestamp) {
        // we can hold onto the resource
        log::info!("{} acquired resource", self);
        self.resource.handle(self.id).await;
        log::info!("{} released resource", self);
        // after releasing it notify other processes
        self.broadcast_msg(Op::Release(req_ts));
    }

    fn handle_msg(&mut self, msg: Message) {
        // adjust our clock based on the clock of the other process
        self.clock.adjust(&msg.ts.clock);
        // perform a tick for the event of the receipt
        self.clock.tick();

        // we need to see messages from all processes before we can execute any
        // request, so record time of receipt
        self.others
            .get_mut(&msg.ts.process)
            .expect("invalid process id")
            .last_msg_ts = Some(msg.ts.clock);

        // handle message
        match msg.op {
            Op::Request => {
                // place the request in the position ordered by the request
                // timestamp
                self.request_queue.insert(msg.ts);
                // need to send an ack that the request was received
                self.send_msg(msg.ts.process, Op::Ack);
            }
            Op::Release(ts) => {
                self.request_queue.remove(&ts);
            }
            Op::Ack => (),
        }
    }

    fn make_request(&mut self) -> Timestamp {
        // the request timestamp has our current clock value which we need to
        // send to each process
        let req_ts = self.broadcast_msg(Op::Request);

        // put the request in our request queue
        assert!(
            self.request_queue.iter().all(|ts| ts.process != self.id),
            "{} cannot have multiple pending requests",
            self,
        );
        self.request_queue.insert(req_ts);
        log::trace!("{} requesting with {:?}", self, req_ts);

        req_ts
    }

    fn send_msg(&mut self, dest: ProcessId, op: Op) {
        let msg = Message {
            ts: self.curr_timestamp(),
            op,
        };
        log::trace!("{} sending to {} message {:?}", self, dest, msg);
        self.others
            .get(&dest)
            .expect("invalid process id")
            .tx
            .send(msg)
            .expect("cannot send message");
        // sending a message constitutes an event requiring advancing the clock
        self.clock.tick();
    }

    fn broadcast_msg(&mut self, op: Op) -> Timestamp {
        let ts = self.curr_timestamp();
        let msg = Message { ts, op };
        log::trace!("{} broadcasting message {:?}", self, msg);
        for process in self.others.values() {
            process.tx.send(msg).expect("cannot send message");
            self.clock.tick();
        }
        ts
    }

    fn curr_timestamp(&self) -> Timestamp {
        Timestamp {
            clock: self.clock,
            process: self.id,
        }
    }
}

impl fmt::Display for Process {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-@{}", self.id, self.clock.0)
    }
}

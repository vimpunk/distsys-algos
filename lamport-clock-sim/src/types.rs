use std::{
    fmt,
    sync::atomic::{self, AtomicUsize},
    time::Duration,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessId(pub usize);

impl fmt::Display for ProcessId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "process#{}", self.0)
    }
}

#[derive(Default)]
pub struct Resource(AtomicUsize);

impl Resource {
    pub async fn handle(&self, process: ProcessId) {
        let old = self.0.fetch_add(1, atomic::Ordering::SeqCst);
        assert_eq!(
            old,
            0,
            "{} acquired resource without synchronization \
            (use count: {})",
            process,
            old + 1
        );

        // simulate some work
        tokio::time::sleep(Duration::from_secs(1)).await;

        // then release it
        self.0.fetch_sub(1, atomic::Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Clock(pub usize);

impl Clock {
    pub fn tick(&mut self) {
        self.0 += 1;
    }

    pub fn adjust(&mut self, other: &Clock) {
        if self.0 <= other.0 {
            self.0 = other.0 + 1;
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
    // NOTE: order of fields here is important: we want the generated Ord and
    // PartialOrd impls to first compare the clock and only consider the process
    // if the clocks are the same
    pub clock: Clock,
    pub process: ProcessId,
}

#[derive(Clone, Copy, Debug)]
pub enum Op {
    Request,
    Release(Timestamp),
    Ack,
}

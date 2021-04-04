use self::types::Resource;
use rand::Rng;
use std::{sync::Arc, time::Duration};

mod process;
mod types;

// We're using the single threaded scheduler purely so that reading logs is
// easier. The actual implementation is not dependent on this, as serializing
// suspension points on a single thread can still lead to race conditions if the
// algorithm is implemented incorrectly.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::init();

    let resource = Arc::new(Resource::default());
    let process_count = 16;

    // initialize processes
    let mut processes = process::new_swarm(process_count, &resource);

    // choose randomly among processes one that starts handling the data and let
    // everyone else know of this fact by inserting the initial request
    let mut rng = rand::thread_rng();
    process::choose_initial_resource_holder(&mut processes, &mut rng);

    let mut tasks = Vec::with_capacity(processes.len());
    for mut p in processes {
        log::debug!("spawning task for {}", p);
        let spawn_delay = Duration::from_millis(rng.gen_range(0..200));
        tasks.push(tokio::task::spawn(async move {
            tokio::time::sleep(spawn_delay).await;
            log::info!("starting {}", p);
            p.run().await
        }));
    }

    // FIXME: will never join
    for (i, t) in tasks.into_iter().enumerate() {
        if t.await.is_err() {
            log::error!("error joining on thread#{}", i);
        }
    }
}

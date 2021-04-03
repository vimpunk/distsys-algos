use self::{
    process::Process,
    types::{ProcessId, Resource},
};
use rand::Rng;
use std::{sync::Arc, time::Duration};

mod process;
mod types;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::init();

    let resource = Arc::new(Resource::default());
    let process_count = 16;

    // initialize processes
    log::info!("initializing {} processes", process_count);
    let mut processes: Vec<_> = (0..process_count)
        .map(|i| Process::new(ProcessId(i), Arc::clone(&resource)))
        .collect();
    process::distribute_senders(&mut processes);

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

    // TODO: will never join
    for (i, t) in tasks.into_iter().enumerate() {
        if t.await.is_err() {
            log::error!("error joining on thread#{}", i);
        }
    }
}

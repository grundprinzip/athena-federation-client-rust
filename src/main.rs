extern crate pretty_env_logger;
#[macro_use]
extern crate log;

// Rexport the models module
pub mod models;

fn main() {
    pretty_env_logger::init();

    trace!("a trace example");
    debug!("deboogging");
    info!("such information");
    warn!("o_O");
    error!("boom");

    println!("Hello, world!");
}

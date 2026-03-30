use env_logger::Env;

pub fn setup_logging() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
}
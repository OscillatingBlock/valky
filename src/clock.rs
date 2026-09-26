use tokio::time::Instant;

pub trait Clock: Send + Sync {
    fn now(&self) -> u64; // epoch millis
}

pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_millis() as u64
    }
}

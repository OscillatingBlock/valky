pub trait Clock: Send + Sync {
    fn now(&self) -> u64;
}

#[derive(Default)]
pub struct SystemClock;
impl Clock for SystemClock {
    //Returns current time since UNIX_EPOCH as milli seconds
    fn now(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_millis() as u64
    }
}

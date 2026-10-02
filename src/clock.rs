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

/// Controllable clock for tests. Shares time via atomics so it can be
/// put behind `Arc` and advanced while a `Server` is running.
pub struct FakeClock {
    now_ms: std::sync::atomic::AtomicU64,
}

impl FakeClock {
    pub fn new(start_ms: u64) -> Self {
        Self {
            now_ms: std::sync::atomic::AtomicU64::new(start_ms),
        }
    }

    pub fn advance(&self, delta_ms: u64) {
        self.now_ms
            .fetch_add(delta_ms, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn set(&self, now_ms: u64) {
        self.now_ms
            .store(now_ms, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> u64 {
        self.now_ms.load(std::sync::atomic::Ordering::SeqCst)
    }
}

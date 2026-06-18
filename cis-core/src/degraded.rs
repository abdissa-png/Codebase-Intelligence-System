//! **Phase 13** — `vector_degraded` debounce (**v2.6**), disk pressure flag (**FR-1.10**).

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Approximate free disk space percent for `path` (100.0 on failure / non-unix).
pub fn disk_free_percent(path: &Path) -> f64 {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::mem::MaybeUninit;
        let c_path = match CString::new(path.as_os_str().as_encoded_bytes()) {
            Ok(p) => p,
            Err(_) => return 100.0,
        };
        unsafe {
            let mut stat: libc::statvfs = MaybeUninit::zeroed().assume_init();
            if libc::statvfs(c_path.as_ptr(), &mut stat) != 0 {
                return 100.0;
            }
            let free = stat.f_bavail as u64;
            let total = stat.f_blocks as u64;
            if total == 0 {
                return 100.0;
            }
            (free as f64 / total as f64) * 100.0
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        100.0
    }
}

/// **§01.4** — HWM/LWM from policy; debounce before clearing (**vector_recovery_debounce_s**).
#[derive(Debug)]
pub struct VectorDegradedController {
    hwm: u32,
    lwm: u32,
    debounce: Duration,
    degraded: AtomicBool,
    below_since: Mutex<Option<Instant>>,
    queue_depth: AtomicU64,
}

impl VectorDegradedController {
    pub fn new(hwm: u32, lwm: u32, debounce_s: u32) -> Self {
        Self {
            hwm,
            lwm,
            debounce: Duration::from_secs(debounce_s as u64),
            degraded: AtomicBool::new(false),
            below_since: Mutex::new(None),
            queue_depth: AtomicU64::new(0),
        }
    }

    pub fn set_queue_depth(&self, d: u64) {
        self.queue_depth.store(d, Ordering::SeqCst);
        self.tick();
    }

    fn tick(&self) {
        let d = self.queue_depth.load(Ordering::SeqCst) as u32;
        if d >= self.hwm {
            self.degraded.store(true, Ordering::SeqCst);
            *self.below_since.lock().unwrap() = None;
            return;
        }
        if d < self.lwm {
            let mut g = self.below_since.lock().unwrap();
            if g.is_none() {
                *g = Some(Instant::now());
            } else if let Some(t0) = *g {
                if t0.elapsed() >= self.debounce {
                    self.degraded.store(false, Ordering::SeqCst);
                    *g = None;
                }
            }
        } else {
            *self.below_since.lock().unwrap() = None;
        }
    }

    pub fn is_vector_degraded(&self) -> bool {
        self.tick();
        self.degraded.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Default)]
pub struct DiskPressureFlag(AtomicBool);

impl DiskPressureFlag {
    pub fn set_disk_pressure(&self, on: bool) {
        self.0.store(on, Ordering::SeqCst);
    }

    pub fn disk_pressure(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_degraded_clears_after_lwm_debounce() {
        let c = VectorDegradedController::new(100, 50, 0);
        c.set_queue_depth(101);
        assert!(c.is_vector_degraded());
        c.set_queue_depth(40);
        assert!(!c.is_vector_degraded());
    }
}

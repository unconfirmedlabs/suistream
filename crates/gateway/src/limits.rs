//! Admission control: global stream caps, distinct-mask cap, and the
//! (disabled) API-key hook. Per-client limits belong at the edge (Cloudflare).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::mask::MaskKey;

#[derive(Clone, Debug)]
pub struct LimitsConfig {
    /// Streams using zstd or gzip.
    pub max_compressed: usize,
    /// Uncompressed streams cost ~8x the egress; capped separately.
    pub max_identity: usize,
    pub max_masks: usize,
}

/// Which stream pool a request draws from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pool {
    Compressed,
    Identity,
}

#[derive(Default)]
struct State {
    compressed: usize,
    identity: usize,
    masks: HashMap<MaskKey, usize>,
}

impl State {
    fn used(&mut self, pool: Pool) -> &mut usize {
        match pool {
            Pool::Compressed => &mut self.compressed,
            Pool::Identity => &mut self.identity,
        }
    }
}

#[derive(Clone)]
pub struct Limits {
    cfg: LimitsConfig,
    state: Arc<Mutex<State>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Rejection {
    Global,
    Masks,
}

impl Rejection {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Masks => "masks",
        }
    }
}

/// Releases the admission on drop.
pub struct Admission {
    limits: Limits,
    pool: Pool,
    mask: MaskKey,
}

impl Drop for Admission {
    fn drop(&mut self) {
        let mut s = self.limits.state.lock().unwrap();
        *s.used(self.pool) -= 1;
        if let Some(n) = s.masks.get_mut(&self.mask) {
            *n -= 1;
            if *n == 0 {
                s.masks.remove(&self.mask);
            }
        }
    }
}

impl Limits {
    pub fn new(cfg: LimitsConfig) -> Self {
        Self {
            cfg,
            state: Default::default(),
        }
    }

    pub fn admit(&self, pool: Pool, mask: MaskKey) -> Result<Admission, Rejection> {
        let max = match pool {
            Pool::Compressed => self.cfg.max_compressed,
            Pool::Identity => self.cfg.max_identity,
        };
        let mut s = self.state.lock().unwrap();
        if *s.used(pool) >= max {
            return Err(Rejection::Global);
        }
        if !s.masks.contains_key(&mask) && s.masks.len() >= self.cfg.max_masks {
            return Err(Rejection::Masks);
        }
        *s.used(pool) += 1;
        *s.masks.entry(mask.clone()).or_default() += 1;
        Ok(Admission {
            limits: self.clone(),
            pool,
            mask,
        })
    }

    pub fn masks_active(&self) -> usize {
        self.state.lock().unwrap().masks.len()
    }
}

/// Who is calling. Anonymous until API keys are introduced.
#[derive(Clone, Debug)]
pub enum Principal {
    Anonymous,
}

/// API-key hook. Keys would arrive as `Authorization: Bearer <key>` or
/// `?key=<key>`; enabling them means validating here and keying limits by
/// principal. Disabled: every caller is anonymous.
pub fn authorize(
    _bearer: Option<&str>,
    _query_key: Option<&str>,
) -> Result<Principal, &'static str> {
    Ok(Principal::Anonymous)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits::new(LimitsConfig {
            max_compressed: 3,
            max_identity: 1,
            max_masks: 2,
        })
    }

    #[test]
    fn caps_and_release() {
        let l = limits();
        let a = l.admit(Pool::Compressed, MaskKey::Full).unwrap();
        let _b = l.admit(Pool::Compressed, MaskKey::Full).unwrap();
        let _c = l.admit(Pool::Compressed, MaskKey::Full).unwrap();
        assert_eq!(
            l.admit(Pool::Compressed, MaskKey::Full).err(),
            Some(Rejection::Global)
        );
        // Identity has its own, stricter pool.
        let _d = l.admit(Pool::Identity, MaskKey::Full).unwrap();
        assert_eq!(
            l.admit(Pool::Identity, MaskKey::Full).err(),
            Some(Rejection::Global)
        );
        drop(a);
        assert!(l.admit(Pool::Compressed, MaskKey::Full).is_ok());
    }

    #[test]
    fn mask_cap() {
        let l = Limits::new(LimitsConfig {
            max_compressed: 10,
            max_identity: 10,
            max_masks: 2,
        });
        let m = |s: &str| MaskKey::Partial(s.into());
        let _a = l.admit(Pool::Compressed, MaskKey::Full).unwrap();
        let b = l.admit(Pool::Compressed, m("digest")).unwrap();
        // Same mask again is fine; a third distinct one is not.
        let _b2 = l.admit(Pool::Compressed, m("digest")).unwrap();
        assert_eq!(
            l.admit(Pool::Compressed, m("summary")).err(),
            Some(Rejection::Masks)
        );
        drop(b);
        assert_eq!(l.masks_active(), 2);
    }
}

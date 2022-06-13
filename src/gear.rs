use super::RollingHash;
use std::default::Default;
use std::mem;

pub type Digest = u64;

/// The effective window size used by `gear`
pub const WINDOW_SIZE: usize = mem::size_of::<Digest>() * 8;

pub struct Gear {
    digest: Digest,
}

impl Default for Gear {
    fn default() -> Self {
        Self {
            digest: 0,
        }
    }
}

include!("_gear_rand.rs");

impl RollingHash for Gear {
    type Digest = Digest;

    #[inline(always)]
    fn roll_byte(&mut self, b: u8) {
        self.digest <<= 1;
        self.digest = self.digest.wrapping_add(G[b as usize]);
    }

    fn roll(&mut self, buf: &[u8]) {
        crate::roll_windowed(self, WINDOW_SIZE, buf);
    }

    #[inline(always)]
    fn digest(&self) -> Digest {
        self.digest
    }

    #[inline]
    fn reset(&mut self) {
        self.digest = 0;
    }
}

impl Gear {
    /// Create new Gear engine
    pub fn new() -> Self {
        Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_window_size() {
        let ones = vec![0x1; 1024];
        let zeroes = vec![0x0; 1024];

        let mut gear = Gear::new();
        gear.roll(&ones);
        let digest = gear.digest();

        let mut gear = Gear::new();
        gear.roll(&zeroes);

        for (i, &b) in ones.iter().enumerate() {
            if gear.digest() == digest {
                assert_eq!(i, WINDOW_SIZE);
                return;
            }
            gear.roll_byte(b);
        }

        panic!("matching digest not found");
    }
}

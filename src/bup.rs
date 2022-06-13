use super::RollingHash;
use std::default::Default;

pub type Digest = u32;

const DEFAULT_WINDOW_BITS: usize = 6;
const DEFAULT_WINDOW_SIZE: usize = 1 << DEFAULT_WINDOW_BITS;

const CHAR_OFFSET: usize = 31;

/// Rolling checksum method used by `bup`
///
/// Strongly based on
/// https://github.com/bup/bup/blob/706e8d273/lib/bup/bupsplit.c
/// https://github.com/bup/bup/blob/706e8d273/lib/bup/bupsplit.h
/// (a bit like https://godoc.org/camlistore.org/pkg/rollsum)
pub struct Bup<const WINDOW_SIZE: usize = DEFAULT_WINDOW_SIZE> {
    s1: usize,
    s2: usize,
    window: [u8; WINDOW_SIZE],
    wofs: usize,
}

impl<const WINDOW_SIZE: usize> Default for Bup<WINDOW_SIZE> {
    fn default() -> Self {
        assert_ne!(WINDOW_SIZE, 0);
        Bup {
            s1: WINDOW_SIZE * CHAR_OFFSET,
            s2: WINDOW_SIZE * (WINDOW_SIZE - 1) * CHAR_OFFSET,
            window: [0; WINDOW_SIZE],
            wofs: 0,
        }
    }
}

impl<const WINDOW_SIZE: usize> RollingHash for Bup<WINDOW_SIZE> {
    type Digest = Digest;

    #[inline(always)]
    fn roll_byte(&mut self, newch: u8) {
        // Since this crate is performance ciritical, and
        // we're in strict control of `wofs`, it is justified
        // to skip bound checking to increase the performance
        // https://github.com/rust-lang/rfcs/issues/811

        // SAFETY: `wofs` is always in the range [0, WINDOW_SIZE)
        //         and WINDOW_SIZE is always > 0
        let prevch = unsafe { *self.window.get_unchecked(self.wofs) };
        self.add(prevch, newch);
        unsafe { *self.window.get_unchecked_mut(self.wofs) = newch };
        self.wofs = (self.wofs + 1) % WINDOW_SIZE;
    }

    fn roll(&mut self, buf: &[u8]) {
        crate::roll_windowed(self, WINDOW_SIZE, buf);
    }

    #[inline(always)]
    fn digest(&self) -> Digest {
        ((self.s1 as Digest) << 16) | ((self.s2 as Digest) & 0xffff)
    }

    fn find_chunk_edge_cond<F>(&mut self, buf: &[u8], mut cond: F) -> Option<usize>
    where
        F: FnMut(&Self) -> bool,
    {
        let first_window = buf.windows(WINDOW_SIZE).next().unwrap_or(buf);
        for (i, &byte) in first_window.iter().enumerate() {
            self.roll_byte(byte);
            if cond(self) {
                return Some(i + 1);
            }
        }

        if buf.len() > WINDOW_SIZE {
            let mut last_window = buf;
            // WINDOW_SIZE + 1, because we need the old byte to shift out
            for (i, window) in buf.windows(WINDOW_SIZE + 1).enumerate() {
                let (&drop, window) = window.split_first().unwrap();
                self.add(drop, *window.last().unwrap());
                if cond(self) {
                    self.wofs = 0;
                    self.window.copy_from_slice(window);
                    return Some(i + WINDOW_SIZE + 1);
                }
                last_window = window;
            }
            // No chunk edge found, need to copy back into the window
            self.wofs = 0;
            self.window.copy_from_slice(last_window);
        }
        None
    }

    #[inline]
    fn reset(&mut self) {
        *self = Bup::default();
    }
}

impl Bup<DEFAULT_WINDOW_SIZE> {
    /// Create new Bup engine with default chunking settings
    pub fn new() -> Self {
        Default::default()
    }
}

impl<const WINDOW_SIZE: usize> Bup<WINDOW_SIZE> {
    #[inline(always)]
    fn add(&mut self, drop: u8, add: u8) {
        self.s1 += add as usize;
        self.s1 -= drop as usize;
        self.s2 += self.s1;
        self.s2 -= WINDOW_SIZE * (drop as usize + CHAR_OFFSET);
    }
}

/// Counts the number of low bits set in the rollsum, assuming
/// the digest has the bottom `chunk_bits` bits set to `1`
/// (i.e. assuming a digest at a default bup chunk edge, as
/// returned by `find_chunk_edge`).
/// Be aware that there's a deliberate 'bug' in this function
/// in order to match expected return values from other bupsplit
/// implementations.
// Note: because of the state is reset after finding an edge, assist
// users use this correctly by making them pass in a digest they've
// obtained.
pub fn count_bits(chunk_bits: u32, digest: Digest) -> u32 {
    let rsum = digest >> chunk_bits;

    // Ignore the next bit as well. This isn't actually
    // a problem as the distribution of values will be the same,
    // but it is unexpected.
    let rsum = rsum >> 1;
    rsum.trailing_ones() + chunk_bits
}

#[cfg(test)]
mod tests {
    use super::*;
    use nanorand::{Rng, WyRand};

    #[test]
    fn bup_selftest() {
        const SELFTEST_SIZE: usize = 100000;
        let mut buf = [0u8; SELFTEST_SIZE];

        fn sum(buf: &[u8]) -> u32 {
            let mut e = Bup::new();
            e.roll(buf);
            e.digest()
        }

        let mut rng = WyRand::new_seed(0x01020304);
        rng.fill_bytes(&mut buf);

        let sum1a: u32 = sum(&buf[0..]);
        let sum1b: u32 = sum(&buf[1..]);

        let sum2a: u32 =
            sum(&buf[SELFTEST_SIZE - DEFAULT_WINDOW_SIZE * 5 / 2..SELFTEST_SIZE - DEFAULT_WINDOW_SIZE]);
        let sum2b: u32 = sum(&buf[0..SELFTEST_SIZE - DEFAULT_WINDOW_SIZE]);

        let sum3a: u32 = sum(&buf[0..DEFAULT_WINDOW_SIZE + 4]);
        let sum3b: u32 = sum(&buf[3..DEFAULT_WINDOW_SIZE + 4]);

        assert_eq!(sum1a, sum1b);
        assert_eq!(sum2a, sum2b);
        assert_eq!(sum3a, sum3b);
    }

    #[test]
    fn count_bits() {
        // Ignores `chunk_bits + 1`th bit
        assert_eq!(super::count_bits(1, 0b001), 1);
        assert_eq!(super::count_bits(1, 0b011), 1);
        assert_eq!(super::count_bits(1, 0b101), 2);
        assert_eq!(super::count_bits(1, 0b111), 2);
        assert_eq!(super::count_bits(1, 0xFFFFFFFF), 31);

        assert_eq!(super::count_bits(5, 0b0001011111), 6);
        assert_eq!(super::count_bits(5, 0b1011011111), 7);
        assert_eq!(super::count_bits(5, 0xFFFFFFFF), 31);
    }
}

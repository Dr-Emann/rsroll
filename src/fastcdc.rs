use super::{Chunker, RollingHash};
use crate::gear::Gear;
use std::cmp;
use std::default::Default;

fn get_masks(avg_size: usize, nc_level: usize, seed: u64) -> (u64, u64) {
    let bits = (avg_size.next_power_of_two() - 1).count_ones();
    if bits == 13 {
        // From the paper
        return (0x0003590703530000, 0x0000d90003530000);
    }
    let mut mask = 0u64;
    let mut v = seed;
    let a = 6364136223846793005;
    let c = 1442695040888963407;
    while mask.count_ones() < bits - nc_level as u32 {
        v = v.wrapping_mul(a).wrapping_add(c);
        mask = (mask | 1).rotate_left(v as u32 & 0x3f);
    }
    let mask_long = mask;
    while mask.count_ones() < bits + nc_level as u32 {
        v = v.wrapping_mul(a).wrapping_add(c);
        mask = (mask | 1).rotate_left(v as u32 & 0x3f);
    }
    let mask_short = mask;
    (mask_short, mask_long)
}

/// FastCDC chunking
///
/// * Paper: "FastCDC: a Fast and Efficient Content-Defined Chunking Approach for Data Deduplication"
/// * Paper-URL: https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf
/// * Presentation: https://www.usenix.org/sites/default/files/conference/protected-files/atc16_slides_xia.pdf
pub struct FastCDC {
    current_chunk_size: u64,
    gear: Gear,
    mask_short: u64,
    mask_long: u64,
    min_size: u64,
    avg_size: u64,
    max_size: u64,
}

impl Default for FastCDC {
    fn default() -> Self {
        FastCDC::new()
    }
}

impl FastCDC {
    /// Create new FastCDC engine with default chunking settings
    pub fn new() -> Self {
        FastCDC::new_with_chunk_bits(13)
    }

    fn reset(&mut self) {
        self.gear.reset();
        self.current_chunk_size = 0;
    }

    /// Create new `FastCDC` engine with custom chunking settings
    ///
    /// `chunk_bits` is number of bits that need to match in
    /// the edge condition. `CHUNK_BITS` constant is the default.
    pub fn new_with_chunk_bits(chunk_bits: u32) -> Self {
        let (mask_short, mask_long) = get_masks(1 << chunk_bits, 2, 0);
        const SPREAD_BITS: u32 = 3;

        let min_size = (1 << (chunk_bits - SPREAD_BITS + 1)) as u64;

        let avg_size = (1 << chunk_bits) as u64;
        let max_size = (1 << (chunk_bits + SPREAD_BITS)) as u64;

        Self {
            current_chunk_size: 0,
            gear: Gear::new(),
            mask_short,
            mask_long,
            min_size,
            avg_size,
            max_size,
        }
    }
}

impl Chunker for FastCDC {
    /// Find chunk edge using `FastCDC` defaults.
    fn chunk_end(&mut self, whole_buf: &[u8]) -> Option<usize> {
        let mut left = whole_buf;
        let mask_short = self.mask_short;
        let mask_long = self.mask_long;

        debug_assert!(self.current_chunk_size < self.max_size);

        // ignore edges in bytes that are smaller than min_size
        if self.current_chunk_size < self.min_size {
            let roll_bytes = cmp::min(self.min_size - self.current_chunk_size, left.len() as u64);
            self.gear.roll(&left[..roll_bytes as usize]);
            self.current_chunk_size += roll_bytes;
            left = &left[roll_bytes as usize..];
        }

        // roll through early bytes with smaller probability
        if self.current_chunk_size < self.avg_size {
            let roll_bytes = cmp::min(self.avg_size - self.current_chunk_size, left.len() as u64);
            let result = self
                .gear
                .find_chunk_edge_cond(&left[..roll_bytes as usize], |g| {
                    g.digest() & mask_short == 0
                });

            if let Some(i) = result {
                self.reset();
                return Some(i + (whole_buf.len() - left.len()));
            }

            self.current_chunk_size += roll_bytes;
            left = &left[roll_bytes as usize..];
        }

        // roll through late bytes with higher probability
        if self.current_chunk_size < self.max_size {
            let roll_bytes = cmp::min(self.max_size - self.current_chunk_size, left.len() as u64);
            let result = self
                .gear
                .find_chunk_edge_cond(&left[..roll_bytes as usize], |g| {
                    g.digest() & mask_long == 0
                });

            if let Some(i) = result {
                self.reset();
                return Some(i + (whole_buf.len() - left.len()));
            }

            self.current_chunk_size += roll_bytes;
            left = &left[roll_bytes as usize..];
        }

        if self.current_chunk_size >= self.max_size {
            debug_assert_eq!(self.current_chunk_size, self.max_size);
            self.reset();
            return Some(whole_buf.len() - left.len());
        }

        debug_assert!(left.is_empty());
        None
    }
}

#![cfg_attr(feature = "bench", feature(test))]

#[cfg(feature = "bench")]
extern crate test;

/// Rolling sum and chunk splitting used by
/// `bup` - https://github.com/bup/bup/
#[cfg(feature = "bup")]
pub mod bup;

#[cfg(feature = "bup")]
pub use crate::bup::Bup;

#[cfg(feature = "gear")]
pub mod gear;
#[cfg(feature = "gear")]
pub use crate::gear::Gear;

/// Rolling sum engine trait
pub trait RollingHash {
    type Digest;

    /// Roll over one byte
    fn roll_byte(&mut self, byte: u8);

    /// Roll over a slice of bytes
    fn roll(&mut self, buf: &[u8]) {
        buf.iter().for_each(|&b| self.roll_byte(b));
    }

    /// Return current rolling sum digest
    fn digest(&self) -> Self::Digest;

    /// Resets the internal state
    fn reset(&mut self);

    /// Find the end of the chunk.
    ///
    /// Feed engine bytes from `buf` and stop when chunk split was found.
    ///
    /// Use `cond` function as chunk split condition.
    ///
    /// When edge is find, state of `self` is reset, using `reset()` method.
    ///
    /// Returns:
    ///
    /// * None - no chunk split was found
    /// * Some - offset of the first unconsumed byte of `buf` and the digest of
    ///   the whole chunk. `offset` == buf.len() if the chunk ended right after
    ///   the whole `buf`.
    fn find_chunk_edge_cond<F>(&mut self, buf: &[u8], mut cond: F) -> Option<usize>
    where
        F: FnMut(&Self) -> bool,
    {
        for (i, &b) in buf.iter().enumerate() {
            self.roll_byte(b);

            if cond(self) {
                return Some(i + 1);
            }
        }
        None
    }
}

pub trait Chunker {
    /// Find the next split position
    ///
    /// When the end of a chunk is found, the state of the chunker is reset.
    ///
    /// Returns:
    /// * None - no chunk split was found, all of `buf` belongs to the current chunk
    /// * Some - prefix length of `buf` that belongs to the current chunk.
    ///          data after the returned length have not been processed yet.
    fn chunk_end(&mut self, buf: &[u8]) -> Option<usize>;

    fn for_each_chunk_end<'a, F>(&mut self, mut buf: &'a [u8], mut f: F)
    where
        F: FnMut(&'a [u8]),
    {
        while let Some(chunk_end) = self.chunk_end(buf) {
            let (chunk, rest) = buf.split_at(chunk_end);
            f(chunk);
            buf = rest;
        }
    }
}

pub struct RollingHashChunker<RH: RollingHash> {
    rh: RH,
    mask: RH::Digest,
}

impl<RH> RollingHashChunker<RH>
where
    RH: RollingHash,
{
    pub fn with_mask(rh: RH, mask: RH::Digest) -> Self {
        Self { rh, mask }
    }
}

impl<RH> Chunker for RollingHashChunker<RH>
where
    RH: RollingHash,
    RH::Digest: Copy,
    RH::Digest: Default,
    RH::Digest: std::ops::BitAnd<Output = RH::Digest>,
    RH::Digest: std::cmp::PartialEq,
{
    fn chunk_end(&mut self, buf: &[u8]) -> Option<usize> {
        let mask = self.mask;
        let res = self.rh.find_chunk_edge_cond(buf, |rh| rh.digest() & mask == RH::Digest::default());
        if res.is_some() {
            self.rh.reset();
        }
        res
    }
}

#[inline]
fn roll_windowed<E: RollingHash>(engine: &mut E, window_size: usize, data: &[u8]) {
    let last_window = data.windows(window_size).last().unwrap_or(data);
    for &b in last_window {
        engine.roll_byte(b);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nanorand::{Rng, WyRand};

    fn rand_data(len: usize) -> Vec<u8> {
        let mut data = vec![0; len];
        let mut rng = WyRand::new_seed(0x01020304);
        rng.fill_bytes(&mut data);
        data
    }

    macro_rules! test_engine {
        ($name:ident, $engine:ty) => {
            mod $name {
                use super::*;

                #[test]
                fn roll_byte_same_as_roll() {
                    let mut engine1 = <$engine>::default();
                    let mut engine2 = <$engine>::default();

                    let data = rand_data(1024);
                    for (i, &b) in data.iter().enumerate() {
                        engine1.roll_byte(b);

                        engine2.reset();
                        engine2.roll(&data[..=i]);
                        assert_eq!(engine1.digest(), engine2.digest());

                        let mut engine3 = <$engine>::default();
                        engine3.roll(&data[..=i]);
                        assert_eq!(engine1.digest(), engine3.digest());
                    }
                }

                #[test]
                fn chunk_edge_correct_digest() {
                    let mut engine1 = <$engine>::default();

                    let data = rand_data(512 * 1024);
                    let mut remaining = &data[..];
                    let f = |engine: &$engine| -> bool { engine.digest() & 0x0F == 0x0F };
                    while let Some(i) = engine1.find_chunk_edge_cond(remaining, f) {
                        let digest = engine1.digest();
                        engine1.reset();
                        assert_ne!(i, 0);
                        let mut engine2 = <$engine>::default();
                        // find_chunk doesn't check the state before adding any values
                        for j in 0..i {
                            engine2.roll_byte(remaining[j]);
                            // Only expect true from f on the last value
                            assert_eq!(f(&engine2), j == i - 1);
                        }
                        assert_eq!(engine2.digest(), digest);

                        remaining = &remaining[i..];
                        engine2.reset();
                        assert_eq!(engine2.digest(), engine1.digest());
                    }
                    // No edges found in the remaining data, let's check
                    let mut engine2 = <$engine>::default();
                    for &b in remaining {
                        engine2.roll_byte(b);
                        assert!(!f(&engine2));
                    }
                    assert_eq!(engine2.digest(), engine1.digest());

                    // Ensure the window is still intact
                    engine1.roll_byte(0x1);
                    engine2.roll_byte(0x1);
                    assert_eq!(engine2.digest(), engine1.digest());
                }
            }
        };
    }

    #[cfg(feature = "bup")]
    test_engine!(bup, Bup);

    #[cfg(feature = "gear")]
    test_engine!(gear, Gear);
}

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use nanorand::Rng;
use rollsum::RollingHash;

fn bench_roll_byte(c: &mut Criterion) {
    const SIZE: usize = 128 * 1024;

    let mut data = vec![0u8; SIZE];
    let mut rng = nanorand::WyRand::new_seed(0x01020304);
    rng.fill_bytes(&mut data);

    let mut group = c.benchmark_group("roll");
    group.throughput(Throughput::Bytes(SIZE as u64));

    macro_rules! bench_engine {
        ($name:ty) => {{
            group.bench_function(concat!(stringify!($name), "/byte_by_byte"), |b| {
                let mut engine = <$name>::new();
                b.iter(|| {
                    for _ in 0..SIZE {
                        engine.roll_byte(black_box(0));
                    }
                });
            });

            group.bench_function(concat!(stringify!($name), "/all"), |b| {
                let mut engine = <$name>::new();
                b.iter(|| {
                    engine.roll(black_box(&data));
                    black_box(engine.digest());
                });
            });

        }};
    }

    macro_rules! bench_chunker {
        ($name:ident, $build:expr) => {
            group.bench_function(concat!(stringify!($name), "/split"), |b| {
                use rollsum::Chunker;
                let mut chunker = $build;
                b.iter(|| {
                    chunker.for_each_chunk_end(&data, |chunk| {
                        black_box(chunk.len());
                    });
                });
            });
        };
    }

    #[cfg(feature = "gear")]
    bench_engine!(rollsum::Gear);
    #[cfg(feature = "bup")]
    bench_engine!(rollsum::Bup);

    #[cfg(feature = "gear")]
    bench_chunker!(Gear, rollsum::RollingHashChunker::<rollsum::Gear>::with_mask(rollsum::Gear::new(), (1 << 15) - 1));
    #[cfg(feature = "bup")]
    bench_chunker!(Bup, rollsum::RollingHashChunker::<rollsum::Bup>::with_mask(rollsum::Bup::new(), (1 << 15) - 1));
    bench_chunker!(FastCDC, rollsum::FastCDC::new());
}

criterion_group!(benches, bench_roll_byte);
criterion_main!(benches);

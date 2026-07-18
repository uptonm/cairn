use cairn_storage::Engine;
use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::tempdir;

fn bench_writes(c: &mut Criterion) {
    c.bench_function("put_1k_seq", |b| {
        b.iter_batched(
            || tempdir().unwrap(),
            |dir| {
                let mut e = Engine::open(dir.path()).unwrap();
                for i in 0..1000u32 {
                    e.put(&i.to_be_bytes(), b"value-payload").unwrap();
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_reads(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut e = Engine::open(dir.path()).unwrap();
    for i in 0..10_000u32 {
        e.put(&i.to_be_bytes(), b"value-payload").unwrap();
    }
    e.flush().unwrap();
    e.compact().unwrap();
    c.bench_function("get_hit_cold", |b| {
        let mut i = 0u32;
        b.iter(|| {
            let _ = e.get(&(i % 10_000).to_be_bytes()).unwrap();
            i = i.wrapping_add(1);
        });
    });
}

criterion_group!(benches, bench_writes, bench_reads);
criterion_main!(benches);

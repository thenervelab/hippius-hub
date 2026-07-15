// Workload A · A6 — client hot-loop microbenchmarks: the addressing hash, and the
// new compression codec's cost. Both land on 100% of uploaded bytes, so their
// per-byte cost is a direct upload-throughput term.
//
// THE HASH QUESTION (open-Q3). The endgame plan proposed replacing our SHA-256
// chunk/file addressing with keyed BLAKE3 (the Xet wire format), flagging "BLAKE3
// materially slower than our SHA path" as a kill criterion and saying "benchmark,
// do not assume parity." Both planning docs then quoted 3.19 / 2.36 GiB/s from a
// bench that did not exist in the repo. This is that bench. On this base the sha2
// `asm` gate is already wide (a964e74, Cargo.toml:54 — aarch64 Linux+macOS and
// x86_64 Linux), so `Sha256::digest` here is the HARDWARE path (ARMv8 SHA / SHA-NI),
// i.e. exactly what ships. There is no soft-SHA build to measure or fix.
//
// Run (macOS needs maturin's own linker flag, since the pyo3 extension-module lib
// leaves Python symbols undefined for a standalone bench binary):
//   RUSTFLAGS="-C link-arg=-undefined -C link-arg=dynamic_lookup" \
//     cargo bench --bench hashing
//
// RESULT — Apple Silicon (aarch64-apple-darwin), 2026-07-14, criterion median:
//   primitive        256 KiB      64 MiB
//   sha256 (hw)      3.16 GiB/s   3.17 GiB/s
//   blake3           2.36 GiB/s   2.34 GiB/s
//   blake3_keyed     2.34 GiB/s   2.33 GiB/s
//   zstd L3 encode   12.2 GiB/s (compressible) · 8.8 GiB/s (incompressible)
//
// CONCLUSION: keep SHA-256. On the hardware path it ships with, SHA-256 is ~35%
// FASTER than keyed BLAKE3, so migrating to BLAKE3 (the Xet wire format) would be a
// hashing REGRESSION *and* a wire break — and interop with an unmodified hf_xet
// client is dead anyway (no HF_XET_* endpoint), so the address hash is a free
// internal choice. zstd encode is 3-4x faster than the hash, so the A2 codec is
// never the throughput floor — the hash is. (open-Q3 closed, with a sourced number.)

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sha2::{Digest, Sha256};
use std::hint::black_box;

/// Deterministic, non-trivially-compressible fill so the hashers/compressor see
/// realistic work and runs reproduce without an RNG. Hash throughput is
/// content-independent; compression throughput is not, so we also build an
/// all-same-byte buffer separately below.
fn pseudo_random(len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x: u32 = 0x9E37_79B9;
    for b in &mut v {
        x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        *b = (x >> 24) as u8;
    }
    v
}

/// The keyed-BLAKE3 key is a compile-time constant in Xet (`DATA_KEY`). Any fixed
/// 32-byte key exercises the same code path; the value does not affect throughput.
const KEYED_BLAKE3_KEY: [u8; 32] = [0x42; 32];

fn bench_addressing_hash(c: &mut Criterion) {
    // 64 MiB = the pack-size hash; 256 KiB = the weights-profile chunk size (A1).
    for &size in &[256usize * 1024, 64 * 1024 * 1024] {
        let buf = pseudo_random(size);
        let mut group = c.benchmark_group("addressing_hash");
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("sha256", size), &buf, |b, buf| {
            b.iter(|| black_box(Sha256::digest(black_box(buf))));
        });
        group.bench_with_input(BenchmarkId::new("blake3", size), &buf, |b, buf| {
            b.iter(|| black_box(blake3::hash(black_box(buf))));
        });
        group.bench_with_input(BenchmarkId::new("blake3_keyed", size), &buf, |b, buf| {
            b.iter(|| black_box(blake3::keyed_hash(&KEYED_BLAKE3_KEY, black_box(buf))));
        });
        group.finish();
    }
}

fn bench_zstd_encode(c: &mut Criterion) {
    // The A2 codec is a private module, so the bench can't import it; zstd::bulk is
    // exactly what src/codec.rs calls, so this measures the same work. BG4 is four
    // of these over quarter-size planes plus a linear de-interleave, so its encode
    // throughput is the same order — measured properly once the codec is public.
    const LEVEL: i32 = 3; // == codec::ZSTD_LEVEL
    let size = 256usize * 1024; // one weights-profile chunk
    let compressible = vec![7u8; size]; // best case — what a low-entropy tensor plane looks like
    let incompressible = pseudo_random(size); // worst case — encode still runs, output discarded

    let mut group = c.benchmark_group("zstd_encode");
    group.throughput(Throughput::Bytes(size as u64));
    for (name, buf) in [("compressible", &compressible), ("incompressible", &incompressible)] {
        group.bench_with_input(BenchmarkId::new("level3", name), buf, |b, buf| {
            b.iter(|| {
                let out = zstd::bulk::compress(black_box(buf), LEVEL);
                black_box(out.map(|v| v.len()))
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_addressing_hash, bench_zstd_encode);
criterion_main!(benches);

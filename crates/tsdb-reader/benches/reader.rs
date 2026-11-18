#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::path::Path;
use tsdb_reader::Index;

const INDEX: &[u8] = include_bytes!("../tests/fixtures/testblock/index");

fn parse_index(c: &mut Criterion) {
    c.bench_function("parse_5_5m_sample_index", |b| {
        b.iter(|| black_box(Index::parse(Path::new("bench-index"), black_box(INDEX)).unwrap()))
    });
}

criterion_group!(benches, parse_index);
criterion_main!(benches);

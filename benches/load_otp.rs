use std::{
    fs::File,
    io::{BufReader, Read, Seek},
};

use criterion::{criterion_group, criterion_main, Criterion};
use fast_beam::{BeamFile};

// struct NullInterner;

// impl Interner for NullInterner {
//     type Atom = String;

//     fn intern(&self, name: &str) -> Self::Atom {
//         name.to_string()
//     }
// }

pub fn bench_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("Index file");

    group.bench_function("from_file", |b| {
        b.iter(|| BeamFile::from_file("fixtures/test.beam").unwrap())
    });

    group.bench_function("from_reader BufReader", |b| {
        b.iter(|| {
            let reader = BufReader::new(File::open("fixtures/test.beam").unwrap());
            BeamFile::from_reader(reader).unwrap()
        })
    });

    group.finish();
}

pub fn bench_all_raw_chunks(c: &mut Criterion) {
    let mut group = c.benchmark_group("Read all raw chunks");

    fn read_chunks<R: Read + Seek>(mut file: BeamFile<R>) -> Vec<Vec<u8>> {
        let chunks: Vec<_> = file
            .iter_raw()
            .map(|(_id, result)| result.unwrap())
            .collect();
        assert_eq!(chunks.len(), 10);
        chunks
    }

    group.bench_function("all from_file", |b| {
        b.iter(|| {
            let file = BeamFile::from_file("fixtures/test.beam").unwrap();
            read_chunks(file)
        })
    });

    group.bench_function("all from_reader BufReader", |b| {
        b.iter(|| {
            let reader = BufReader::new(File::open("fixtures/test.beam").unwrap());
            let file = BeamFile::from_reader(reader).unwrap();
            read_chunks(file)
        })
    });

    group.bench_function("beam_file", |b| {
        b.iter(|| {
            let file = beam_file::RawBeamFile::from_file("fixtures/test.beam").unwrap();
            file.chunks
        })
    });

    group.finish();
}

criterion_group!(benches, bench_index, bench_all_raw_chunks);
criterion_main!(benches);

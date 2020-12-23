use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion, SamplingMode, Throughput};

use rustonosmium::databases::*;
use rustonosmium::osm_data::*;
use uom::si::f64::*;
use uom::si::length::kilometer;

const TEST_OSM_FILE: &str = "/Users/lkroll/Programming/Sailing/test-data/world.osm";

pub fn criterion_benchmark(c: &mut Criterion) {
    count_nodes_benchmark(c);
    filter_tags_benchmark(c);
    distance_benchmark(c);
}

pub fn count_nodes_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Count Nodes");
    let file_meta = std::fs::metadata(TEST_OSM_FILE).expect("file metadata");
    group.throughput(Throughput::Bytes(file_meta.len()));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    // group.bench_function("xml.rs", |b| {
    //     b.iter(bench_count_nodes_xml_rs);
    // });
    group.bench_function("quick_xml", |b| {
        b.iter(bench_count_nodes_quick_xml);
    });
    group.bench_function("in-memory", bench_count_nodes_in_memory);
    group.finish();
}

pub fn filter_tags_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("FilterTags");
    let fx_db = fx_hash_id_db::from_file(TEST_OSM_FILE).expect("load db");
    group.throughput(Throughput::Elements(fx_db.nodes().len() as u64));
    group.bench_function("fx_hash_id_db", |b| bench_filter_tags(b, &fx_db));
    group.finish();
}

pub fn distance_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Distance");
    let fx_db = fx_hash_id_db::from_file(TEST_OSM_FILE).expect("load db");
    group.throughput(Throughput::Elements(fx_db.nodes().len() as u64));
    group.bench_function("fx_hash_id_db", |b| bench_distance(b, &fx_db));
    group.finish();
}

// fn bench_count_nodes_xml_rs() -> usize {
//     rustonosmium::xml_rs_reader::scan_nodes(
//         TEST_OSM_FILE,
//         |_node, acc| {
//             //println!("Node: {:?}", _node);
//             acc + 1
//         },
//         0,
//     )
//     .expect("scanned")
// }

fn bench_count_nodes_quick_xml() -> usize {
    rustonosmium::quick_xml_reader::scan_nodes(
        TEST_OSM_FILE,
        |_node, acc| {
            //println!("Node: {:?}", _node);
            acc + 1
        },
        0,
    )
    .expect("scanned")
}

fn bench_count_nodes_in_memory(bencher: &mut Bencher) {
    let db = fx_hash_id_db::from_file(TEST_OSM_FILE).expect("load db");
    bencher.iter(|| db.nodes().len())
}

fn bench_filter_tags(bencher: &mut Bencher, db: &impl OsmDatabase) {
    bencher.iter_with_large_drop(|| db.nodes_with_tag("seamark:type", Some("light_minor")))
}

fn bench_distance(bencher: &mut Bencher, db: &impl OsmDatabase) {
    let pos: Position = "(54.5, -8.3)".parse().expect("pos");
    let dist = Length::new::<kilometer>(100.0);
    bencher.iter_with_large_drop(|| db.nodes_in_radius(pos, dist))
}

// fn bench_filter_tags<F, D>(bencher: &mut Bencher, db_loader: F)
// where
//     D: OsmDatabase,
//     F: Fn(&'static str) -> D,
// {
//     let db = db_loader(TEST_OSM_FILE);
//     bencher.iter_with_large_drop(|| db.nodes_with_tag("seamark:type", Some("light_minor")))
// }

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

use criterion::{
    black_box,
    criterion_group,
    criterion_main,
    Bencher,
    Criterion,
    SamplingMode,
    Throughput,
};

use rustonosmium::{databases::*, osm_data::*};
use uom::si::{f64::*, length::kilometer};

const TEST_OSM_FILE: &str = "/Users/lkroll/Programming/Sailing/test-data/world.osm";

struct DBSet {
    fx_db: fx_hash_id_db::FxHashIdDb,
    index_db: rtree_indexed_db::RTreeIndexedDb,
    disk_db: temp_disk_db::TempDiskDb,
    index_disk_db: rtree_indexed_disk_db::RTreeIndexedDiskDb,
}

fn load_db_timed<F, T>(label: &str, f: F) -> T
where
    F: FnOnce() -> T,
{
    let start = std::time::Instant::now();
    let db = f();
    let elapsed = start.elapsed();
    let elapsed_sec = elapsed.as_millis() as f64 / 1000.0;
    println!("Loading {} took {}s", label, elapsed_sec);
    db
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let fx_db = load_db_timed("fx_hash_id_db", || {
        fx_hash_id_db::from_file(TEST_OSM_FILE).expect("load db")
    });
    let index_db = load_db_timed("rtree_indexed_db", || {
        rtree_indexed_db::from_file(TEST_OSM_FILE).expect("load db")
    });
    let disk_db = load_db_timed("temp_disk_db", || {
        temp_disk_db::from_file(TEST_OSM_FILE).expect("load db")
    });
    let index_disk_db = load_db_timed("rtree_indexed_disk_db", || {
        rtree_indexed_disk_db::from_file(TEST_OSM_FILE).expect("load db")
    });
    let databases = DBSet {
        fx_db,
        index_db,
        disk_db,
        index_disk_db,
    };
    count_nodes_benchmark(c, &databases);
    filter_tags_benchmark(c, &databases);
    distance_benchmark(c, &databases);
}

fn count_nodes_benchmark(c: &mut Criterion, databases: &DBSet) {
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
    group.bench_function("in-memory", |b| {
        bench_count_nodes_in_memory(b, &databases.fx_db)
    });
    group.finish();
}

fn filter_tags_benchmark(c: &mut Criterion, databases: &DBSet) {
    let mut group = c.benchmark_group("FilterTags");
    group.throughput(Throughput::Elements(databases.fx_db.nodes().len() as u64));
    group.bench_function("fx_hash_id_db", |b| bench_filter_tags(b, &databases.fx_db));
    group.bench_function("rtree_indexed_db", |b| {
        bench_filter_tags(b, &databases.index_db)
    });
    group.bench_function("temp_disk_db", |b| {
        bench_filter_tags_owned(b, &databases.disk_db)
    });
    group.bench_function("rtree_indexed_disk_db", |b| {
        bench_filter_tags_owned(b, &databases.index_disk_db)
    });
    group.finish();
}

fn distance_benchmark(c: &mut Criterion, databases: &DBSet) {
    let mut group = c.benchmark_group("Distance");
    group.throughput(Throughput::Elements(databases.fx_db.nodes().len() as u64));
    group.bench_function("fx_hash_id_db", |b| bench_distance(b, &databases.fx_db));
    group.bench_function("rtree_indexed_db", |b| {
        bench_distance(b, &databases.index_db)
    });
    group.bench_function("temp_disk_db", |b| {
        bench_distance_owned(b, &databases.disk_db)
    });
    group.bench_function("rtree_indexed_disk_db", |b| {
        bench_distance_owned(b, &databases.index_disk_db)
    });
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

fn bench_count_nodes_in_memory(bencher: &mut Bencher, db: &impl OsmDatabase) {
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

fn bench_filter_tags_owned(bencher: &mut Bencher, db: &impl OwnedOsmDatabase) {
    bencher.iter_with_large_drop(|| db.nodes_with_tag("seamark:type", Some("light_minor")))
}

fn bench_distance_owned(bencher: &mut Bencher, db: &impl OwnedOsmDatabase) {
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

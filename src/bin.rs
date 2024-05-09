use can2k_geo::{abbreviations::meters, GeoError};
use pretty_duration::pretty_duration;
use rayon::prelude::*;
use rustonosmium::{shapefile_db::Polygon, *};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::channel,
        Arc,
        Mutex,
    },
    thread,
    time::{Duration, Instant},
};

pub fn main() {
    let mut read_db = shapefile_db::ShapefileReadDb::open(
        "/Users/lkroll/Programming/Sailing/test-data/land-polygons-complete-4326/land_polygons.shp",
    )
    .expect("Could not open file");
    let mut write_db = read_db
        .writer_with_same_table_schema("./test.shp")
        .expect("Could not open file");
    let mut total_poly = 0usize;
    println!("Counting polygons...");
    for poly_res in read_db.read_polygons(Some("FID")) {
        let _poly = poly_res.expect("Error reading polygon");
        total_poly += 1;
        //write_db.write_polygon(&poly).expect("Error during write");
    }
    println!("Got a total of  {total_poly} polygons to convert");

    let in_progress = Arc::new(AtomicUsize::new(0));
    let simplified = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let eliminated = Arc::new(AtomicUsize::new(0));
    let errors: Arc<Mutex<Vec<GeoError>>> = Arc::new(Mutex::new(Vec::new()));
    let (write_sender, write_receiver) = channel();
    let writer_handle = {
        let completed = completed.clone();
        let in_progress = in_progress.clone();
        thread::spawn(move || {
            while let Ok(poly) = write_receiver.recv() {
                //println!("Writing poly");
                write_db.write_polygon(&poly).expect("Error during write");
                completed.fetch_add(1, Ordering::Relaxed);
                in_progress.fetch_sub(1, Ordering::Relaxed);
            }
            println!("Channel died");
        })
    };
    let finished = Arc::new(AtomicBool::new(false));
    let status_handle = {
        let in_progress = in_progress.clone();
        let simplified = simplified.clone();
        let completed = completed.clone();
        let eliminated = eliminated.clone();
        let finished = finished.clone();
        thread::spawn(move || {
            let start = Instant::now();
            while !finished.load(Ordering::Relaxed) {
                let elapsed = start.elapsed();
                let completed_poly = completed.load(Ordering::Relaxed);
                let completed_percentage = ((completed_poly as f64) / (total_poly as f64)) * 100.0;
                let average_time_per_poly_ns = if completed_poly > 0 {
                    elapsed.as_nanos() / (completed_poly as u128)
                } else {
                    1
                };
                let remaining = total_poly - completed_poly;
                let remaining_time =
                    Duration::from_nanos((average_time_per_poly_ns as u64) * (remaining as u64));
                println!(
                "Status: in_progress={}, simplified={}, eliminated={}, completed={} / {total_poly} ({:.2}%) ETA ~{}",
                in_progress.load(Ordering::Relaxed),
                simplified.load(Ordering::Relaxed),
                eliminated.load(Ordering::Relaxed),
                completed_poly,
                completed_percentage,
                pretty_duration(&remaining_time, None)
            );
                thread::sleep(Duration::from_secs(1));
            }
            println!(
                "Final Status: in_progress={}, simplified={}, eliminated={}, completed={} / {total_poly}",
                in_progress.load(Ordering::Relaxed),
                simplified.load(Ordering::Relaxed),
                eliminated.load(Ordering::Relaxed),
                completed.load(Ordering::Relaxed)
            );
        })
    };
    println!("Starting conversion");
    let errors_copy = errors.clone();
    let in_progress_map = in_progress.clone();
    read_db
        .read_polygons(Some("FID"))
        .par_bridge()
        .map(move |poly_res| {
            //println!("Converting poly");
            let poly = poly_res.expect("Error reading polygon");
            in_progress_map.fetch_add(1, Ordering::Relaxed);
            // TODO: Temporarily skip small polys.
            if false {
                // poly.outer_ring.len() < 1_000_000 {
                None
            } else {
                match simplify_poly(&poly) {
                    Ok(simple_poly) => simple_poly,
                    Err(e) => {
                        let mut guard = errors_copy.lock().expect("errors mutex is broken");
                        guard.push(e);
                        // Keep the original.
                        Some(poly)
                    }
                }
            }
        })
        .for_each_with(write_sender, |sender, poly| {
            if let Some(poly) = poly {
                sender.send(poly).expect("Send error");
                simplified.fetch_add(1, Ordering::Relaxed);
            } else {
                eliminated.fetch_add(1, Ordering::Relaxed);
                completed.fetch_add(1, Ordering::Relaxed);
                in_progress.fetch_sub(1, Ordering::Relaxed);
            }
        });
    println!("Waiting for write to complete");
    writer_handle.join().expect("Writer didn't finish");
    finished.store(true, Ordering::Relaxed);
    status_handle.join().expect("Status didn't finish");
    //println!("Rewrote {} polygons.", num_poly.fetch());
    println!("Done");
    let guard = errors.lock().expect("errors mutex is broken");
    println!("Errors:");
    for error in guard.iter() {
        eprintln!("{error}");
    }
}

fn simplify_poly(poly: &Polygon) -> Result<Option<Polygon>, GeoError> {
    if let Some(outer_ring) =
        utils::simplify::simplify_polygon_on_ellipsoid(&poly.outer_ring, meters(10000.0))?
    {
        let inner_ring = poly
            .inner_ring
            .as_ref()
            .and_then(|inner_ring| {
                utils::simplify::simplify_polygon_on_ellipsoid(inner_ring, meters(10000.0))
                    .transpose()
            })
            .transpose()?;
        Ok(Some(Polygon {
            id: poly.id,
            outer_ring,
            inner_ring,
        }))
    } else {
        Ok(None)
    }
}

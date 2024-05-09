use async_recursion::async_recursion;
use async_scoped::TokioScope;
use can2k_geo::{
    abbreviations::*,
    geodesic::{self, GeodesicDistance},
    intersect,
    points::GeoPoint,
    projection,
    GeoError,
};
use std::{future::Future, sync::RwLock};
use tokio::sync::mpsc;

// TODO: Make this parallelise, and use haversine as approximation before calculating exact.
//       If wikipedia is right an 0.5% error bound holds that'd be at most 100km...

struct BufferedComputePool<Args, Res> {
    max_outstanding_work: usize,
    outstanding_work: usize,
    result_sender: mpsc::Sender<Res>,
    result_queue: mpsc::Receiver<Res>,
    compute_fun: fn(Args) -> Res,
}

impl<Args, Res> BufferedComputePool<Args, Res>
where
    Args: 'static + Send,
    Res: 'static + Send,
{
    fn new(compute_fun: fn(Args) -> Res, max_outstanding_work: usize) -> Self {
        let (sender, receiver) = mpsc::channel(max_outstanding_work);
        Self {
            max_outstanding_work,
            outstanding_work: 0,
            result_sender: sender,
            result_queue: receiver,
            compute_fun,
        }
    }

    /// Submit a new calculation for `args` and return the results of any computation
    /// that have completed in the meantime.
    /// This will block if `outstanding_work == max_outstanding_work`.`
    async fn submit_and_receive(&mut self, args: Args) -> Vec<Res> {
        let f = self.compute_fun;
        let channel = self.result_sender.clone();
        self.outstanding_work += 1;
        tokio::spawn(async move {
            let res = f(args);
            let res = channel.send(res).await;
            if let Err(e) = res {
                eprintln!("Error on compute thread: {e}");
            }
        });
        let mut res = Vec::new();
        if self.outstanding_work >= self.max_outstanding_work {
            let num_results = self
                .result_queue
                .recv_many(&mut res, self.outstanding_work)
                .await;
            self.outstanding_work -= num_results;
        }
        res
    }

    /// Waits for the remaining work items to be completed and returns them.
    async fn wait_for_completion(&mut self) -> Vec<Res> {
        let mut res = Vec::new();
        while self.outstanding_work > 0 {
            let num_results = self
                .result_queue
                .recv_many(&mut res, self.outstanding_work)
                .await;
            self.outstanding_work -= num_results;
        }
        res
    }

    /// Consumes the rest of the results on an async thread, just so the channels don't error out.
    fn ignore_remaining(mut self) {
        tokio::spawn(async move {
            self.wait_for_completion().await;
        });
    }
}

/// 30 means it's going to do about 900 comparisons. That's likely worth parallelising.
const PARALLEL_ELIMINATE_THRESHOLD: usize = 30;

/// That is at least 500 point to line distances per thread on 2 threads.
const PARALLEL_SIMPLIFY_THRESHOLD: usize = 1000;

static RUNTIME: RwLock<Option<tokio::runtime::Runtime>> = RwLock::new(None);

fn with_runtime<F, R, Fut>(f: F) -> R
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = R>,
{
    let runtime_handle = if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle
    } else {
        let guard = RUNTIME.read().expect("read lock");
        if let Some(ref rt) = *guard {
            rt.handle().clone()
        } else {
            drop(guard);
            let mut guard = RUNTIME.write().expect("write lock");
            if let Some(ref rt) = *guard {
                rt.handle().clone()
            } else {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("runtime");
                let handle = rt.handle().clone();
                *guard = Some(rt);
                handle
            }
        }
    };
    runtime_handle.block_on(async { f().await })
}

fn should_eliminate_polygon(
    polygon: &[GeoPoint<Degrees>],
    max_distance: Meters,
) -> Result<bool, GeoError> {
    // TODO: Not worth it like this, maybe convert to per point and line if necessary.
    // if polygon.len() >= PARALLEL_ELIMINATE_THRESHOLD {
    //     with_runtime(|| should_eliminate_polygon_parallel(polygon, max_distance))
    // } else {
    should_eliminate_polygon_sequential(polygon, max_distance)
    //}
}

fn should_eliminate_polygon_sequential(
    polygon: &[GeoPoint<Degrees>],
    max_distance: Meters,
) -> Result<bool, GeoError> {
    let mut eliminate_polygon: bool = true;
    'outer: for p1 in polygon.iter() {
        for p2 in polygon.iter() {
            let distance = geodesic::WGS84.distance(*p1, *p2)?;
            if distance >= max_distance {
                eliminate_polygon = false;
                // As soon as we find one pair, we are done.
                break 'outer;
            }
        }
    }
    Ok(eliminate_polygon)
}
async fn should_eliminate_polygon_parallel(
    polygon: &[GeoPoint<Degrees>],
    max_distance: Meters,
) -> Result<bool, GeoError> {
    let mut eliminate_polygon: bool = true;
    struct DistanceArgs {
        start: GeoPoint<Degrees>,
        end: GeoPoint<Degrees>,
    }
    fn calc_distance(args: DistanceArgs) -> Result<Meters, GeoError> {
        geodesic::WGS84.distance(args.start, args.end)
    }
    let mut pool = BufferedComputePool::new(calc_distance, 30);
    'outer: for p1 in polygon.iter() {
        for p2 in polygon.iter() {
            let distance_args = DistanceArgs {
                start: *p1,
                end: *p2,
            };
            let res = pool.submit_and_receive(distance_args).await;
            for distance_res in res {
                let distance = distance_res?;
                if distance >= max_distance {
                    eliminate_polygon = false;
                    // As soon as we find one pair, we are done.
                    break 'outer;
                }
            }
        }
    }
    if eliminate_polygon {
        let res = pool.wait_for_completion().await;
        for distance_res in res {
            let distance = distance_res?;
            if distance >= max_distance {
                eliminate_polygon = false;
            }
        }
    } else {
        pool.ignore_remaining();
    }
    Ok(eliminate_polygon)
}

/// Reduce the number of points on `polygon` using Douglas–Peucker algorithm,
/// such that the error is not larger than `max_distance`.
///
/// If an entire polygon is smaller than this distance in the longest dimension,
///  then just eliminate it, returning `None`.
pub fn simplify_polygon_on_ellipsoid(
    polygon: &[GeoPoint<Degrees>],
    max_distance: Meters,
) -> Result<Option<Vec<GeoPoint<Degrees>>>, GeoError> {
    // Try to eliminate shapes that are small by taking pair-wise distance
    // and comparing to `max_distance`.
    let eliminate_polygon = should_eliminate_polygon(polygon, max_distance)?;
    if eliminate_polygon {
        return Ok(None);
    }
    if polygon.len() <= 4 {
        // Do not simplify a shape with less than 5 points, because it will degenerate the shape.
        Ok(Some(polygon.to_vec()))
    } else {
        if polygon.len() >= PARALLEL_SIMPLIFY_THRESHOLD {
            with_runtime(async || {
                // Simplify two sub parts, so it cannot reduce below 4 points.
                let middle_index = polygon.len() / 2;
                // Duplicate the middle index so we don't miss opportunities for simplification.
                let left = &polygon[0..=middle_index];
                let right = &polygon[middle_index..];
                // Only unsafe would be to `forget` the result. It *is* Drop-safe.
                let (_, mut results) = unsafe {
                    TokioScope::scope_and_collect(|scope| {
                        scope.spawn(simplify_polyline_on_ellipsoid_parallel(left, max_distance));
                        scope.spawn(simplify_polyline_on_ellipsoid_parallel(right, max_distance));
                    })
                    .await
                };
                assert_eq!(2, results.len());
                let mut right_simplified = results.pop().unwrap().expect("task result")?;
                let mut left_simplified = results.pop().unwrap().expect("task result")?;
                // We shouldn't have eliminated the duplicated point.
                assert_eq!(left_simplified.last(), right_simplified.first());
                // Remove the duplicate value now.
                left_simplified.pop();
                left_simplified.append(&mut right_simplified);
                Ok(Some(left_simplified))
            })
        } else {
            // Simplify two sub parts, so it cannot reduce below 4 points.
            let middle_index = polygon.len() / 2;
            // Duplicate the middle index so we don't miss opportunities for simplification.
            let left = &polygon[0..=middle_index];
            let right = &polygon[middle_index..];
            let mut left_simplified =
                simplify_polyline_on_ellipsoid_sequential(left, max_distance)?;
            let mut right_simplified =
                simplify_polyline_on_ellipsoid_sequential(right, max_distance)?;
            // We shouldn't have eliminated the duplicated point.
            assert_eq!(left_simplified.last(), right_simplified.first());
            // Remove the duplicate value now.
            left_simplified.pop();
            left_simplified.append(&mut right_simplified);
            Ok(Some(left_simplified))
        }
    }
}

/// Reduce the number of points on `polyline` using Douglas–Peucker algorithm,
/// such that the error is not larger than `max_distance`.
pub fn simplify_polyline_on_ellipsoid(
    polyline: &[GeoPoint<Degrees>],
    max_distance: Meters,
) -> Result<Vec<GeoPoint<Degrees>>, GeoError> {
    if polyline.len() >= PARALLEL_SIMPLIFY_THRESHOLD {
        with_runtime(|| simplify_polyline_on_ellipsoid_parallel(polyline, max_distance))
    } else {
        simplify_polyline_on_ellipsoid_sequential(polyline, max_distance)
    }
}

fn simplify_polyline_on_ellipsoid_sequential(
    polyline: &[GeoPoint<Degrees>],
    max_distance: Meters,
) -> Result<Vec<GeoPoint<Degrees>>, GeoError> {
    //println!("Trying to simplify {:?}", polyline);
    if polyline.len() < 3 {
        // Can't reduce this any further.
        return Ok(polyline.to_vec());
    }
    let mut current_max_distance: Option<Meters> = None;
    let mut current_max_distance_point: Option<usize> = None;
    let start = polyline.first().unwrap();
    let end = polyline.last().unwrap();
    for (index, point) in polyline.iter().enumerate().take(polyline.len() - 1).skip(1) {
        let distance = match intersect::find_shortest_distance_between_line_and_point(
            &geodesic::WGS84,
            *start,
            *end,
            *point,
        ) {
            Ok(d) => d,
            Err(GeoError::ErrorWithCode(e))
                if e.code == projection::gnomonic::OVER_THE_HORIZON_ERROR.code
                    || e.code == intersect::NO_CONVERGENCE.code =>
            {
                meters(f64::INFINITY)
            }
            Err(e) => return Err(e),
        };
        if let Some(current_max) = current_max_distance {
            if current_max < distance {
                current_max_distance = Some(distance);
                current_max_distance_point = Some(index);
            }
        } else {
            current_max_distance = Some(distance);
            current_max_distance_point = Some(index);
        }
    }
    // These must have been set.
    assert!(current_max_distance.is_some());
    assert!(current_max_distance_point.is_some());
    // println!("current_max_distance={current_max_distance:?} vs {max_distance}");
    if current_max_distance.unwrap() <= max_distance {
        // We can discard all points in this range without violating the error.
        Ok(vec![*start, *end])
    } else {
        // Recurse with the left and right sub-ranges.
        let left = &polyline[0..current_max_distance_point.unwrap()];
        let right = &polyline[current_max_distance_point.unwrap()..];
        let mut left_simplified = simplify_polyline_on_ellipsoid_sequential(left, max_distance)?;
        let mut right_simplified = simplify_polyline_on_ellipsoid_sequential(right, max_distance)?;
        left_simplified.append(&mut right_simplified);
        Ok(left_simplified)
    }
}

#[async_recursion]
async fn simplify_polyline_on_ellipsoid_parallel(
    polyline: &[GeoPoint<Degrees>],
    max_distance: Meters,
) -> Result<Vec<GeoPoint<Degrees>>, GeoError> {
    //println!("Trying to simplify {:?}", polyline);
    if polyline.len() < 3 {
        // Can't reduce this any further.
        return Ok(polyline.to_vec());
    }
    let polyline = if polyline.len() > PARALLEL_SIMPLIFY_THRESHOLD {
        // Recurse first, to improve parallelism.
        // Then simplify the remaining points again properly.
        let middle_index = polyline.len() / 2;
        // Duplicate the middle index so we don't miss opportunities for simplification.
        let left = &polyline[0..=middle_index];
        let right = &polyline[middle_index..];
        // Only unsafe would be to `forget` the result. It *is* Drop-safe.
        let (_, mut results) = unsafe {
            TokioScope::scope_and_collect(|scope| {
                scope.spawn(simplify_polyline_on_ellipsoid_parallel(left, max_distance));
                scope.spawn(simplify_polyline_on_ellipsoid_parallel(right, max_distance));
            })
            .await
        };
        assert_eq!(2, results.len());
        let mut right_simplified = results.pop().unwrap().expect("task result")?;
        let mut left_simplified = results.pop().unwrap().expect("task result")?;
        // We shouldn't have eliminated the duplicated point.
        assert_eq!(left_simplified.last(), right_simplified.first());
        // Remove the duplicate value now.
        left_simplified.pop();
        left_simplified.append(&mut right_simplified);
        left_simplified
    } else {
        polyline.to_vec()
    };
    let mut current_max_distance: Option<Meters> = None;
    let mut current_max_distance_point: Option<usize> = None;
    let start = polyline.first().unwrap();
    let end = polyline.last().unwrap();
    for (index, point) in polyline.iter().enumerate().take(polyline.len() - 1).skip(1) {
        let distance = match intersect::find_shortest_distance_between_line_and_point(
            &geodesic::WGS84,
            *start,
            *end,
            *point,
        ) {
            Ok(d) => d,
            Err(GeoError::ErrorWithCode(e))
                if e.code == projection::gnomonic::OVER_THE_HORIZON_ERROR.code
                    || e.code == intersect::NO_CONVERGENCE.code =>
            {
                meters(f64::INFINITY)
            }
            Err(e) => return Err(e),
        };
        if let Some(current_max) = current_max_distance {
            if current_max < distance {
                current_max_distance = Some(distance);
                current_max_distance_point = Some(index);
            }
        } else {
            current_max_distance = Some(distance);
            current_max_distance_point = Some(index);
        }
    }
    // These must have been set.
    assert!(current_max_distance.is_some());
    assert!(current_max_distance_point.is_some());
    // println!("current_max_distance={current_max_distance:?} vs {max_distance}");
    if current_max_distance.unwrap() <= max_distance {
        // We can discard all points in this range without violating the error.
        Ok(vec![*start, *end])
    } else {
        // Recurse with the left and right sub-ranges.
        let left = &polyline[0..current_max_distance_point.unwrap()];
        let right = &polyline[current_max_distance_point.unwrap()..];
        let (mut left_simplified, mut right_simplified) = if (left.len()
            >= PARALLEL_SIMPLIFY_THRESHOLD)
            || (right.len() >= PARALLEL_ELIMINATE_THRESHOLD)
        {
            // Only unsafe would be to `forget` the result. It *is* Drop-safe.
            let (_, mut results) = unsafe {
                TokioScope::scope_and_collect(|scope| {
                    scope.spawn(simplify_polyline_on_ellipsoid_parallel(left, max_distance));
                    scope.spawn(simplify_polyline_on_ellipsoid_parallel(right, max_distance));
                })
                .await
            };
            assert_eq!(2, results.len());
            let right_simplified = results.pop().unwrap().expect("task result")?;
            let left_simplified = results.pop().unwrap().expect("task result")?;
            (left_simplified, right_simplified)
        } else {
            let left_simplified_f = simplify_polyline_on_ellipsoid_parallel(left, max_distance);
            let right_simplified_f = simplify_polyline_on_ellipsoid_parallel(right, max_distance);
            (left_simplified_f.await?, right_simplified_f.await?)
        };
        left_simplified.append(&mut right_simplified);
        Ok(left_simplified)
    }
}

#[cfg(test)]
mod tests {
    use can2k_geo::geodesic::WGS84;
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_simplify_polyline() {
        let p1 = coord(1.0, 12.0);
        let p2 = coord(1.0, 21.0);
        let minimal_line = vec![p1, p2];
        // No matter how much error we allow, a minimal line cannot be further simplified.
        assert_eq!(
            minimal_line,
            simplify_polyline_on_ellipsoid(&minimal_line, meters(f64::INFINITY)).unwrap()
        );
        let p3 = coord(1.0, 18.0);
        // P3 isn't necessarily inline...shortest distance between p1 and p2 does not follow
        // the parallel.
        assert_eq!(
            minimal_line,
            simplify_polyline_on_ellipsoid(&[p1, p3, p2], meters(1000.0)).unwrap()
        );
        // Adding p3 twice makes no difference.
        assert_eq!(
            minimal_line,
            simplify_polyline_on_ellipsoid(&[p1, p3, p3, p2], meters(1000.0)).unwrap()
        );
        let p4 = coord(12.0, 17.0);
        let longer_line = vec![p1, p4, p3, p2];
        // Far away point p4 cannot be eliminated
        assert_eq!(
            longer_line,
            simplify_polyline_on_ellipsoid(&longer_line, meters(1000.0)).unwrap()
        );
    }

    #[test]
    fn test_simplify_large_polyline() {
        let mut polyline = Vec::new();
        // Build a long diagonal line.
        for v in 1..40 {
            polyline.push(coord(v as f64 * 0.01, v as f64 * 0.01));
        }
        // The line should completely eliminate, since they are all inline(ish).
        let expected = vec![coord(0.01, 0.01), coord(0.39, 0.39)];
        assert_eq!(
            expected,
            simplify_polyline_on_ellipsoid(&polyline, meters(1000.0)).unwrap()
        );
    }

    #[test]
    fn test_simplify_large_polygon() {
        let mut polyline = Vec::new();
        // Build a long diagonal line.
        for v in 1..20 {
            polyline.push(coord(v as f64 * 0.01, v as f64 * 0.01));
        }
        // Build another to complete a large triangle.
        for (lat, lon) in (0..20).rev().zip(20..40) {
            polyline.push(coord(lat as f64 * 0.01, lon as f64 * 0.01));
        }
        // Should keep the endpoints of the triangle.
        let expected = vec![coord(0.01, 0.01), coord(0.19, 0.2), coord(0.0, 0.39)];
        assert_eq!(
            Some(expected),
            simplify_polygon_on_ellipsoid(&polyline, meters(1000.0)).unwrap()
        );

        let super_short_polyline = polyline
            .into_iter()
            .map(|c| c.map(|a| a * 0.001))
            .collect_vec();
        // This should execute in parallel.
        assert_eq!(
            None,
            simplify_polygon_on_ellipsoid(&super_short_polyline, meters(1000.0)).unwrap()
        );
        assert!(
            should_eliminate_polygon_sequential(&super_short_polyline, meters(1000.0)).unwrap()
        );
    }

    #[test]
    fn test_simplify_very_large_polygon() {
        let mut polyline = Vec::new();
        // Build a long diagonal line.
        for v in 1..600 {
            polyline.push(coord(v as f64 * 0.01, v as f64 * 0.01));
        }
        // Build another to complete a large triangle.
        for (lat, lon) in (0..600).rev().zip(600..1200) {
            polyline.push(coord(lat as f64 * 0.01, lon as f64 * 0.01));
        }
        // Should keep the endpoints of the triangle.
        let expected = vec![coord(0.01, 0.01), coord(5.99, 6.0), coord(0.0, 11.99)];
        assert_eq!(
            Some(expected),
            simplify_polygon_on_ellipsoid(&polyline, meters(1000.0)).unwrap()
        );
    }

    #[test]
    fn test_simplify_polygon() {
        let p1 = coord(1.0, 12.0);
        let p2 = coord(1.0, 21.0);
        let p3 = coord(0.0, 17.0);
        let minimal_polygon = vec![p1, p2, p3];
        // "small" polygons will be eliminated.
        assert_eq!(
            None,
            simplify_polygon_on_ellipsoid(&minimal_polygon, meters(f64::INFINITY)).unwrap()
        );
        // Minimal polygon will not be further simplified.
        assert_eq!(
            Some(minimal_polygon.clone()),
            simplify_polygon_on_ellipsoid(
                &minimal_polygon,
                // This is greater than the distance from p2 to the line connecting p1 and p3
                WGS84.distance(p1, p2).unwrap()
            )
            .unwrap()
        );
        let p4 = coord(0.5, 13.0);
        let minimal_polygon4 = vec![p1, p2, p3, p4];
        // "small" polygons will be eliminated.
        assert_eq!(
            None,
            simplify_polygon_on_ellipsoid(&minimal_polygon4, meters(f64::INFINITY)).unwrap()
        );
        // Minimal polygon will not be further simplified.
        assert_eq!(
            Some(minimal_polygon4.clone()),
            simplify_polygon_on_ellipsoid(
                &minimal_polygon4,
                // This is greater than the distance from p2 to the line connecting p1 and p3
                WGS84.distance(p1, p2).unwrap()
            )
            .unwrap()
        );
        // This is almost inline with p1 and p2.
        let p5 = coord(1.0, 18.0);
        assert_eq!(
            Some(minimal_polygon4.clone()),
            simplify_polygon_on_ellipsoid(&[p1, p5, p2, p3, p4], meters(1000.0)).unwrap()
        );
        let p6 = coord(12.0, 17.0);
        let complicated_polygon = vec![p1, p6, p5, p2, p3, p4];
        assert_eq!(
            Some(complicated_polygon.clone()),
            simplify_polygon_on_ellipsoid(&complicated_polygon, meters(1000.0)).unwrap()
        );
    }
}

use super::*;
use approx::{abs_diff_eq, AbsDiffEq};
use std::{cmp::Ordering, fmt};

type FloatSize = f64;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub x: FloatSize,
    pub y: FloatSize,
}

impl AsRef<Point> for Point {
    fn as_ref(&self) -> &Point {
        self
    }
}

impl PartialOrd for Point {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let x_res = self.x.partial_cmp(&other.x)?;
        let y_res = self.y.partial_cmp(&other.y)?;
        if x_res == y_res {
            Some(x_res)
        } else {
            match (x_res, y_res) {
                (Ordering::Equal, y) => Some(y),
                (x, Ordering::Equal) => Some(x),
                _ => None,
            }
        }
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "・({},{})", self.x, self.y)
    }
}

impl AbsDiffEq for Point {
    type Epsilon = f64;

    fn default_epsilon() -> Self::Epsilon {
        f64::EPSILON
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        self.x.abs_diff_eq(&other.x, epsilon) && self.y.abs_diff_eq(&other.y, epsilon)
    }
}

impl Intersecting for Point {
    type IntersectionShape = Point;

    fn intersects(&self, other: &Point) -> bool {
        self == other
    }

    fn intersection(&self, other: &Point) -> Option<Self::IntersectionShape> {
        if self == other {
            Some(*self)
        } else {
            None
        }
    }

    fn contains(&self, other: &Point) -> bool {
        abs_diff_eq!(self, other)
    }
}

impl Intersecting<Rectangle> for Point {
    type IntersectionShape = Point;

    fn intersects(&self, other: &Rectangle) -> bool {
        other.intersects(self)
    }

    fn intersection(&self, other: &Rectangle) -> Option<Self::IntersectionShape> {
        other.intersection(self)
    }

    fn contains(&self, other: &Rectangle) -> bool {
        // only possible for a zero-sized rectangle
        abs_diff_eq!(self, &other.low_corner) && abs_diff_eq!(self, &other.high_corner)
    }
}

impl Extending<Rectangle> for Point {
    fn extend(&self, geometry: Rectangle) -> Rectangle {
        if geometry.intersects(self) {
            // already included
            geometry
        } else {
            let low_x = (geometry.low_corner.x).min(self.x);
            let low_y = (geometry.low_corner.y).min(self.y);
            let high_x = (geometry.high_corner.x).max(self.x);
            let high_y = (geometry.high_corner.y).max(self.y);
            let r = Rectangle {
                low_corner: Point { x: low_x, y: low_y },
                high_corner: Point {
                    x: high_x,
                    y: high_y,
                },
            };
            if cfg!(test) {
                r.assert_legal();
            }
            r
        }
    }
}

impl Bounding for Point {
    fn bound_all<'a>(mut entries: impl Iterator<Item = &'a Self>) -> Rectangle {
        let first = entries.next().expect("Don't bound empty iterators!");
        let mut min_x = first.x;
        let mut min_y = first.y;
        let mut max_x = first.x;
        let mut max_y = first.y;
        for p in entries {
            min_x = min_x.min(p.x);
            min_y = min_y.min(p.y);
            max_x = max_x.max(p.x);
            max_y = max_y.max(p.y);
        }
        let low_corner = Point { x: min_x, y: min_y };
        let high_corner = Point { x: max_x, y: max_y };
        let r = Rectangle {
            low_corner,
            high_corner,
        };
        if cfg!(test) {
            r.assert_legal();
        }
        r
    }

    fn find_split<T>(entries: Vec<T>, min_fill: usize, max_fill: usize) -> (Vec<T>, Vec<T>)
    where
        T: AsRef<Self>,
    {
        // From R*-tree paper
        //
        // Algorithm Split
        // S1 Invoke ChooseSplitAxis to determine the axis,
        //  perpendicular to which the split is performed
        // S2 Invoke ChooseSplitIndex to determine the best
        //  distribution into two groups along that axis
        // S3 Distribute the entries into two groups
        //
        // Algorithm ChooseSplitAxis
        // CSA1 For each axis
        //          Sort the entries by the lower then by the upper
        //          value of their rectangles and determine all
        //          distributions as described above. Compute S, the
        //          sum of all margin-values of the different
        //          distributions
        //      end
        let (chosen_sorted, chosen_distributions) = {
            let mut x_sorted: Vec<(usize, &T)> = entries.iter().enumerate().collect();
            x_sorted.sort_by(|l_entry, r_entry| {
                let l: &Point = l_entry.1.as_ref();
                let r: &Point = r_entry.1.as_ref();
                (l.x)
                    .partial_cmp(&r.x)
                    .expect("All coordinates must be comparable!")
            });
            let mut y_sorted = x_sorted.clone();
            y_sorted.sort_by(|l_entry, r_entry| {
                let l: &Point = l_entry.1.as_ref();
                let r: &Point = r_entry.1.as_ref();
                (l.y)
                    .partial_cmp(&r.y)
                    .expect("All coordinates must be comparable!")
            });
            choose_split_axis(x_sorted, y_sorted, min_fill, max_fill)
        };
        let split_index = choose_split_index(chosen_distributions, min_fill);
        // drop the immutable reference to entries
        let chosen_sorted_indices: Vec<usize> = chosen_sorted.into_iter().map(|e| e.0).collect();
        split_at_index(entries, chosen_sorted_indices, split_index)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rectangle {
    pub low_corner: Point,
    pub high_corner: Point,
}

impl Rectangle {
    pub fn margin_length(&self) -> f64 {
        (self.high_corner.x - self.low_corner.x).abs()
            * (self.high_corner.y - self.low_corner.y).abs()
    }

    pub fn assert_legal(&self) {
        assert!(
            self.low_corner <= self.high_corner,
            "Rectangle {} is illegal!",
            self
        );
    }
}

impl fmt::Display for Rectangle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}|{}]", self.low_corner, self.high_corner)
    }
}

impl Intersecting<Point> for Rectangle {
    type IntersectionShape = Point;

    fn intersects(&self, other: &Point) -> bool {
        &self.low_corner <= other && other <= &self.high_corner
    }

    fn intersection(&self, other: &Point) -> Option<Self::IntersectionShape> {
        if self.intersects(other) {
            Some(*other)
        } else {
            None
        }
    }

    fn contains(&self, other: &Point) -> bool {
        self.low_corner <= *other && *other <= self.high_corner
    }
}

impl Intersecting for Rectangle {
    type IntersectionShape = Rectangle;

    fn intersects(&self, other: &Rectangle) -> bool {
        self.low_corner.x <= other.high_corner.x
            && other.low_corner.x <= self.high_corner.x
            && self.low_corner.y <= other.high_corner.y
            && other.low_corner.y <= self.high_corner.y
    }

    fn intersection(&self, other: &Rectangle) -> Option<Self::IntersectionShape> {
        let low_x = (self.low_corner.x).max(other.low_corner.x);
        let high_x = (self.high_corner.x).min(other.high_corner.x);
        let low_y = (self.low_corner.y).max(other.low_corner.y);
        let high_y = (self.high_corner.y).min(other.high_corner.y);

        if (low_x < high_x) && (low_y < high_y) {
            let r = Rectangle {
                low_corner: Point { x: low_x, y: low_y },
                high_corner: Point {
                    x: high_x,
                    y: high_y,
                },
            };
            if cfg!(test) {
                r.assert_legal();
            }
            Some(r)
        } else {
            // Rectangles do not overlap, or overlap has an area of zero (edge/corner overlap)
            None
        }
    }

    fn contains(&self, other: &Rectangle) -> bool {
        self.low_corner <= other.low_corner && other.high_corner <= self.high_corner
    }
}

impl HasArea for Rectangle {
    fn area(&self) -> f64 {
        (self.high_corner.x - self.low_corner.x).abs()
            * (self.high_corner.y - self.low_corner.y).abs()
    }
}

impl Bounding for Rectangle {
    fn bound_all<'a>(mut entries: impl Iterator<Item = &'a Self>) -> Rectangle {
        let first = entries.next().expect("Don't bound empty iterators!");
        let mut min_x = first.low_corner.x;
        let mut min_y = first.low_corner.y;
        let mut max_x = first.high_corner.x;
        let mut max_y = first.high_corner.y;
        for r in entries {
            min_x = min_x.min(r.low_corner.x);
            min_y = min_y.min(r.low_corner.y);
            max_x = max_x.max(r.high_corner.x);
            max_y = max_y.max(r.high_corner.y);
        }
        let low_corner = Point { x: min_x, y: min_y };
        let high_corner = Point { x: max_x, y: max_y };
        let r = Rectangle {
            low_corner,
            high_corner,
        };
        if cfg!(test) {
            r.assert_legal();
        }
        r
    }

    fn find_split<T>(entries: Vec<T>, min_fill: usize, max_fill: usize) -> (Vec<T>, Vec<T>)
    where
        T: AsRef<Self>,
    {
        // From R*-tree paper
        //
        // Algorithm Split
        // S1 Invoke ChooseSplitAxis to determine the axis,
        //  perpendicular to which the split is performed
        // S2 Invoke ChooseSplitIndex to determine the best
        //  distribution into two groups along that axis
        // S3 Distribute the entries into two groups
        //
        // Algorithm ChooseSplitAxis
        // CSA1 For each axis
        //          Sort the entries by the lower then by the upper
        //          value of their rectangles and determine all
        //          distributions as described above. Compute S, the
        //          sum of all margin-values of the different
        //          distributions
        //      end
        let (chosen_sorted, chosen_distributions) = {
            let mut x_sorted: Vec<(usize, &T)> = entries.iter().enumerate().collect();
            x_sorted.sort_by(|l_entry, r_entry| {
                let l: &Rectangle = l_entry.1.as_ref();
                let r: &Rectangle = r_entry.1.as_ref();
                let primary = (l.low_corner.x)
                    .partial_cmp(&r.low_corner.x)
                    .expect("All coordinates must be comparable!");
                primary.then_with(|| {
                    (l.high_corner.x)
                        .partial_cmp(&r.high_corner.x)
                        .expect("All coordinates must be comparable!")
                })
            });
            let mut y_sorted = x_sorted.clone();
            y_sorted.sort_by(|l_entry, r_entry| {
                let l: &Rectangle = l_entry.1.as_ref();
                let r: &Rectangle = r_entry.1.as_ref();
                let primary = (l.low_corner.y)
                    .partial_cmp(&r.low_corner.y)
                    .expect("All coordinates must be comparable!");
                primary.then_with(|| {
                    (l.high_corner.y)
                        .partial_cmp(&r.high_corner.y)
                        .expect("All coordinates must be comparable!")
                })
            });
            choose_split_axis(x_sorted, y_sorted, min_fill, max_fill)
        };
        let split_index = choose_split_index(chosen_distributions, min_fill);
        // drop the immutable reference to entries
        let chosen_sorted_indices: Vec<usize> = chosen_sorted.into_iter().map(|e| e.0).collect();
        split_at_index(entries, chosen_sorted_indices, split_index)
    }
}

fn bounding_distributions<T, B>(
    sorted: &[(usize, &T)],
    min_fill: usize,
    max_fill: usize,
) -> Vec<(Rectangle, Rectangle)>
where
    T: AsRef<B>,
    B: Bounding,
{
    let num_distributions = max_fill - 2 * min_fill + 2;
    let mut result = Vec::with_capacity(num_distributions);
    for k in 1..=num_distributions {
        let split_index = min_fill - 1 + k;
        let left_bounding_box =
            B::bound_all(sorted[0..split_index].iter().map(|entry| entry.1.as_ref()));
        let right_bounding_box =
            B::bound_all(sorted[split_index..].iter().map(|entry| entry.1.as_ref()));
        result.push((left_bounding_box, right_bounding_box));
    }
    result
}

fn choose_split_axis<'a, T, B>(
    x_sorted: Vec<(usize, &'a T)>,
    y_sorted: Vec<(usize, &'a T)>,
    min_fill: usize,
    max_fill: usize,
) -> (Vec<(usize, &'a T)>, Vec<(Rectangle, Rectangle)>)
where
    T: AsRef<B>,
    B: Bounding,
{
    let x_distributions: Vec<(Rectangle, Rectangle)> =
        bounding_distributions(x_sorted.as_slice(), min_fill, max_fill);
    let y_distributions: Vec<(Rectangle, Rectangle)> =
        bounding_distributions(y_sorted.as_slice(), min_fill, max_fill);
    let margin_x_sum: f64 = x_distributions
        .iter()
        .map(|(l, r)| l.margin_length() + r.margin_length())
        .sum();
    let margin_y_sum: f64 = y_distributions
        .iter()
        .map(|(l, r)| l.margin_length() + r.margin_length())
        .sum();
    // CSA2 Choose the axis with the minimum S as split axis
    if margin_x_sum < margin_y_sum {
        (x_sorted, x_distributions)
    } else {
        (y_sorted, y_distributions)
    }
}

fn choose_split_index(chosen_distributions: Vec<(Rectangle, Rectangle)>, min_fill: usize) -> usize {
    //
    // Algorithm ChooseSplitIndex
    // CSI1 Along the chosen split axis, choose the
    //  distribution with the minimum overlap-value
    //  Resolve ties by choosing the distribution with
    //  minimum area-value
    let mut min_index = 0;
    let mut min_overlap: f64 = f64::INFINITY;
    let mut min_area: Option<f64> = None; // not always needed
    for (index, (left, right)) in chosen_distributions.iter().enumerate() {
        let overlap = left.intersection(right).map(|r| r.area()).unwrap_or(0.0);
        if overlap < min_overlap {
            min_index = index;
            min_overlap = overlap;
            min_area = None;
        } else if (overlap - min_overlap).abs() < f64::EPSILON {
            let area = left.area() + right.area();
            let other_area = if let Some(a) = min_area {
                a
            } else {
                let (min_l, min_r) = chosen_distributions[min_index];
                let a = min_l.area() + min_r.area();
                min_area = Some(a);
                a
            };
            if area < other_area {
                min_index = index;
                min_overlap = overlap;
                min_area = Some(area);
            }
        }
    }
    let k = min_index + 1;
    // split index
    min_fill - 1 + k
}
fn split_at_index<T>(
    entries: Vec<T>,
    chosen_sorted_indices: Vec<usize>,
    split_index: usize,
) -> (Vec<T>, Vec<T>) {
    let mut pickable_entries: Vec<Option<T>> = entries.into_iter().map(Some).collect();
    let mut left: Vec<T> = Vec::with_capacity(split_index);
    for index in chosen_sorted_indices[0..split_index].iter() {
        left.push(pickable_entries[*index].take().unwrap()); // must exists unless there are duplicate indices (which would be a bug)
    }
    let mut right: Vec<T> = Vec::with_capacity(pickable_entries.len() - split_index);
    for index in chosen_sorted_indices[split_index..].iter() {
        right.push(pickable_entries[*index].take().unwrap()); // must exists unless there are duplicate indices (which would be a bug)
    }
    (left, right)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_comparison() {
        let p1 = Point { x: 0.0, y: 0.0 };
        #[allow(clippy::eq_op)]
        assert_eq!(p1, p1);
        let p2 = Point { x: -1.0, y: -1.0 };
        assert_ne!(p1, p2);
        assert!(p2 < p1);
        let p3 = Point { x: 1.0, y: 1.0 };
        assert!(p1 < p3);
        assert!(p2 < p3);
        let p4 = Point { x: 1.0, y: 0.0 };
        assert!(p2 <= p4);
        assert!(p4 <= p3);
    }

    #[test]
    fn test_rectangle_rectangle_intersection() {
        let r1 = Rectangle {
            low_corner: Point { x: -1.0, y: -1.0 },
            high_corner: Point { x: 1.0, y: 1.0 },
        };
        let r2 = Rectangle {
            low_corner: Point { x: -2.0, y: -2.0 },
            high_corner: Point { x: 2.0, y: 2.0 },
        };
        assert!(r1.intersects(&r2));
        assert!(r2.intersects(&r1));
        let r3 = Rectangle {
            low_corner: Point { x: 2.0, y: 2.0 },
            high_corner: Point { x: 3.0, y: 3.0 },
        };
        assert!(!r1.intersects(&r3));
        assert!(!r3.intersects(&r1));
        let r4 = Rectangle {
            low_corner: Point { x: 0.0, y: 0.0 },
            high_corner: Point { x: 3.0, y: 3.0 },
        };
        assert!(r1.intersects(&r4));
        assert!(r4.intersects(&r1));
        let r5 = Rectangle {
            low_corner: Point { x: -3.0, y: -3.0 },
            high_corner: Point { x: 0.0, y: 0.0 },
        };
        assert!(r1.intersects(&r5));
        assert!(r5.intersects(&r1));
        let r6 = Rectangle {
            low_corner: Point { x: -3.0, y: -0.5 },
            high_corner: Point { x: 3.0, y: 0.5 },
        };
        assert!(r1.intersects(&r6));
        assert!(r6.intersects(&r1));
        let r7 = Rectangle {
            low_corner: Point { x: -0.5, y: -3.0 },
            high_corner: Point { x: 0.5, y: 3.0 },
        };
        assert!(r1.intersects(&r7));
        assert!(r7.intersects(&r1));
        let r8 = Rectangle {
            low_corner: Point { x: 1.0, y: -3.0 },
            high_corner: Point { x: 3.0, y: 3.0 },
        };
        assert!(r1.intersects(&r8));
        assert!(r8.intersects(&r1));
    }

    #[test]
    fn test_rectangle_point_intersection() {
        let r = Rectangle {
            low_corner: Point { x: -1.0, y: -1.0 },
            high_corner: Point { x: 1.0, y: 1.0 },
        };
        let p1 = Point { x: 0.0, y: 0.0 };
        assert!(r.intersects(&p1));
        assert!(p1.intersects(&r));

        let p2 = Point { x: 3.0, y: 3.0 };
        assert!(!r.intersects(&p2));
        assert!(!p2.intersects(&r));
        let p3 = Point { x: -3.0, y: -3.0 };
        assert!(!r.intersects(&p3));
        assert!(!p3.intersects(&r));
        let p4 = Point { x: -3.0, y: 3.0 };
        assert!(!r.intersects(&p4));
        assert!(!p4.intersects(&r));
        let p5 = Point { x: 3.0, y: -3.0 };
        assert!(!r.intersects(&p5));
        assert!(!p5.intersects(&r));

        let p6 = Point { x: 1.0, y: 0.0 };
        assert!(r.intersects(&p6));
        assert!(p6.intersects(&r));
        let p7 = Point { x: -1.0, y: -1.0 };
        assert!(r.intersects(&p7));
        assert!(p7.intersects(&r));
    }

    #[test]
    fn test_bounding_boxes_points() {
        let mut points: Vec<Point> = Vec::new();
        points.push(Point { x: 0.0, y: 0.0 });
        points.push(Point { x: 1.0, y: 1.0 });
        {
            let bbox = Point::bound_all(points.iter());
            for point in points.iter() {
                assert!(bbox.contains(point));
            }
        }
        points.push(Point { x: -1.0, y: -1.0 });
        {
            let bbox = Point::bound_all(points.iter());
            for point in points.iter() {
                assert!(bbox.contains(point));
            }
        }
        points.push(Point { x: 1.0, y: -1.0 });
        {
            let bbox = Point::bound_all(points.iter());
            for point in points.iter() {
                assert!(bbox.contains(point));
            }
        }
        points.push(Point { x: -1.0, y: 1.0 });
        {
            let bbox = Point::bound_all(points.iter());
            for point in points.iter() {
                assert!(bbox.contains(point));
            }
        }
    }

    #[test]
    fn test_bounding_boxes_rectangles() {
        let mut rectangles: Vec<Rectangle> = Vec::new();
        rectangles.push(Rectangle {
            low_corner: Point { x: 0.0, y: 0.0 },
            high_corner: Point { x: 1.0, y: 1.0 },
        });
        {
            let bbox = Rectangle::bound_all(rectangles.iter());
            for rectangle in rectangles.iter() {
                assert!(
                    bbox.contains(rectangle),
                    "bound {} does not contain {}",
                    bbox,
                    rectangle
                );
            }
        }
        rectangles.push(Rectangle {
            low_corner: Point { x: -1.0, y: -1.0 },
            high_corner: Point { x: 0.0, y: 0.0 },
        });
        {
            let bbox = Rectangle::bound_all(rectangles.iter());
            for rectangle in rectangles.iter() {
                assert!(
                    bbox.contains(rectangle),
                    "bound {} does not contain {}",
                    bbox,
                    rectangle
                );
            }
        }
        rectangles.push(Rectangle {
            low_corner: Point { x: 0.0, y: -1.0 },
            high_corner: Point { x: 1.0, y: 0.0 },
        });
        {
            let bbox = Rectangle::bound_all(rectangles.iter());
            for rectangle in rectangles.iter() {
                assert!(
                    bbox.contains(rectangle),
                    "bound {} does not contain {}",
                    bbox,
                    rectangle
                );
            }
        }
        rectangles.push(Rectangle {
            low_corner: Point { x: -1.0, y: 0.0 },
            high_corner: Point { x: 0.0, y: 1.0 },
        });
        {
            let bbox = Rectangle::bound_all(rectangles.iter());
            for rectangle in rectangles.iter() {
                assert!(
                    bbox.contains(rectangle),
                    "bound {} does not contain {}",
                    bbox,
                    rectangle
                );
            }
        }
    }
}

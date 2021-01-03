use super::*;
use crate::osm_data::{Latitude, Position};
use approx::abs_diff_eq;
use planar::*;
use uom::si::{angle::degree, f64::*, length::kilometer};

/// A course directly north (0º)
pub const NORTH: f64 = 0.0;
/// A course directly north (0º)
#[inline(always)]
pub fn north_deg() -> Angle {
    Angle::new::<degree>(NORTH)
}
// Can't do this until uom makes new const fn
//pub const NORTH: Angle = Angle::new::<degree>(0.0);

/// A course directly east (90º)
pub const EAST: f64 = 90.0;
/// A course directly east (90º)
#[inline(always)]
pub fn east_deg() -> Angle {
    Angle::new::<degree>(EAST)
}
// Can't do this until uom makes new const fn
//pub const EAST: Angle = Angle::new::<degree>(90.0);

/// A course directly south (180º)
pub const SOUTH: f64 = 180.0;
/// A course directly south (180º)
#[inline(always)]
pub fn south_deg() -> Angle {
    Angle::new::<degree>(SOUTH)
}
// Can't do this until uom makes new const fn
//pub const SOUTH: Angle = Angle::new::<degree>(180.0);

/// A course directly west (270º)
pub const WEST: f64 = 270.0;
/// A course directly west (270º)
#[inline(always)]
pub fn west_deg() -> Angle {
    Angle::new::<degree>(WEST)
}
// Can't do this until uom makes new const fn
//pub const WEST: Angle = Angle::new::<degree>(270.0);

const EARTH_CIRCUMFERENCE_MERIDIONAL_KM: f64 = 40007.86;
/// Maximum length of any side of a bounding box
///
/// Bounding boxes around circles with radii greater than this number will wrap both poles,
/// making them useless for filtering.
///
/// Use [EARTH_BOX](EARTH_BOX) if you need something that implements `Intersecting<Rectangle>`,
/// without filtering.
pub const MAX_BOUNDING_RADIUS_KM: f64 = EARTH_CIRCUMFERENCE_MERIDIONAL_KM / 4.0;
#[inline(always)]
pub fn max_bounding_radius() -> Length {
    Length::new::<kilometer>(MAX_BOUNDING_RADIUS_KM)
}
// Can't do this until uom makes new const fn
// pub const MAX_BOUNDING_RADIUS: Length = Length::new::<kilometer>(EARTH_CIRCUMFERENCE_MERIDIONAL_KM / 4.0);

// /// A ZST that implements no filtering for `Intersecting<Rectangle>`
// ///
// /// In other words `EarthBox.intersects(x)` returns `true` for any `x`
// #[derive(Debug, PartialEq, Eq, Clone, Copy)]
// pub struct EarthBox;
// impl Intersecting<Rectangle> for EarthBox {
//     type IntersectionShape = Rectangle;

//     fn intersects(&self, _other: &Rectangle) -> bool {
//         true
//     }

//     fn intersection(&self, other: &Rectangle) -> Option<Self::IntersectionShape> {
//         Some(*other)
//     }
// }
// /// See [EarthBox](EarthBox)
// pub const EARTH_BOX: EarthBox = EarthBox;

const ERROR: f64 = 1e-8;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum BoundingBox {
    Simple(Rectangle),
    EquatorialWrap(Rectangle),
    ArcticCap(Latitude),
    AntarcticCap(Latitude),
}

impl BoundingBox {
    pub fn from_circle(centre: Position, radius: Length) -> BoundingBox {
        let centre = centre.to_floats();
        // reasonably accurate bounding box
        let north = centre.travel_accurate(north_deg(), radius);
        let south = centre.travel_accurate(south_deg(), radius);

        if !abs_diff_eq!(north.lon, centre.lon, epsilon = ERROR) {
            // wrapped over the northpole
            assert!(
                abs_diff_eq!(south.lon, centre.lon, epsilon = ERROR),
                "BoundingBox wrapped over both poles. No filtering possible!"
            );
            return BoundingBox::ArcticCap(south.lat);
        }
        if !abs_diff_eq!(south.lon, centre.lon, epsilon = ERROR) {
            // wrapped over the southpole
            // (we already checked for both overlap above)
            return BoundingBox::AntarcticCap(north.lat);
        }
        let east = centre.travel_accurate(east_deg(), radius);
        let west = centre.travel_accurate(west_deg(), radius);
        let low_corner_x = west.lon.as_degrees();
        let low_corner_y = south.lat.as_degrees();
        let low_corner = Point {
            x: low_corner_x,
            y: low_corner_y,
        };
        let high_corner_x = east.lon.as_degrees();
        let high_corner_y = north.lat.as_degrees();
        let high_corner = Point {
            x: high_corner_x,
            y: high_corner_y,
        };
        let bounding_rectangle = Rectangle {
            low_corner,
            high_corner,
        };
        if high_corner_x < low_corner_x {
            BoundingBox::EquatorialWrap(bounding_rectangle)
        } else {
            BoundingBox::Simple(bounding_rectangle)
        }
    }
}

impl Intersecting<Rectangle> for BoundingBox {
    type IntersectionShape = BoundingBox;

    fn intersects(&self, other: &Rectangle) -> bool {
        match self {
            BoundingBox::Simple(r) => r.intersects(other),
            BoundingBox::EquatorialWrap(r) => {
                let y_overlap = r.low_corner.y <= other.high_corner.y && other.low_corner.y <= r.high_corner.y;
                // other can't wrap, so one sided tests are sufficient
                let x_overlap = r.low_corner.x <= other.high_corner.x || other.low_corner.x <= r.high_corner.x;

                x_overlap && y_overlap
            }
            BoundingBox::ArcticCap(lat) => other.high_corner.y >= lat.as_degrees(),
            BoundingBox::AntarcticCap(lat) => other.low_corner.y <= lat.as_degrees(),
        }
    }

    fn intersection(&self, _other: &Rectangle) -> Option<Self::IntersectionShape> {
        todo!("Implement me");
    }

    fn contains(&self, other: &Rectangle) -> bool {
        match self {
            BoundingBox::Simple(r) => r.contains(other),
            BoundingBox::EquatorialWrap(r) => {
                let y_contain = r.low_corner.y <= other.low_corner.y && other.high_corner.y <= r.high_corner.y;
                // other can't wrap, so one sided tests are sufficient
                let x_contain = r.low_corner.x <= other.low_corner.x || other.high_corner.x <= r.high_corner.x;

                x_contain && y_contain
            }
            BoundingBox::ArcticCap(lat) => other.low_corner.y >= lat.as_degrees(),
            BoundingBox::AntarcticCap(lat) => other.high_corner.y <= lat.as_degrees(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uom::si::angle::radian;

    #[test]
    fn test_rectangle_bounding_box_intersection() {
        let r1 = Rectangle {
            low_corner: Point { x: -1.0, y: -1.0 },
            high_corner: Point { x: 1.0, y: 1.0 },
        };
        let b1 = BoundingBox::Simple(r1);
        assert!(b1.intersects(&r1));
        let r2 = Rectangle {
            low_corner: Point { x: 2.0, y: 2.0 },
            high_corner: Point { x: 3.0, y: 3.0 },
        };
        assert!(!b1.intersects(&r2));
        let r3 = Rectangle {
            low_corner: Point { x: 2.0, y: 2.0 },
            high_corner: Point { x: 170.0, y: 80.0 },
        };
        let b2 = BoundingBox::EquatorialWrap(Rectangle {
            low_corner: Point { x: 160.0, y: 20.0 },
            high_corner: Point { x: -160.0, y: 60.0 },
        });
        assert!(b2.intersects(&r3));
        assert!(!b2.intersects(&r1));
        let b3 = BoundingBox::ArcticCap("70.0".parse().unwrap());
        assert!(b3.intersects(&r3));
        assert!(!b3.intersects(&r1));
        let r4 = Rectangle {
            low_corner: Point { x: 20.0, y: -80.0 },
            high_corner: Point { x: 60.0, y: 10.0 },
        };
        let b4 = BoundingBox::AntarcticCap("-70.0".parse().unwrap());
        assert!(b4.intersects(&r4));
        assert!(!b4.intersects(&r1));
    }

    #[test]
    fn test_bounding_radius() {
        let pos: Position = "(54.5, -8.3)".parse().expect("pos");
        let dist = Length::new::<kilometer>(100.0);
        assert_ne!(north_deg().get::<radian>(), south_deg().get::<radian>());
        let north = pos.travel_accurate(north_deg(), dist);
        let south = pos.travel_accurate(south_deg(), dist);
        println!("north={:?}, south={:?}", north, south);
        assert_ne!(north, south);
        let bounding_box = BoundingBox::from_circle(pos, dist);
        println!("Got bounding box: {:?}", bounding_box);
        assert!(matches!(bounding_box, BoundingBox::Simple(_)));
        let r1 = Rectangle {
            low_corner: Point { x: -10.0, y: 50.0 },
            high_corner: Point { x: -5.0, y: 60.0 },
        };
        assert!(bounding_box.intersects(&r1));
    }
}

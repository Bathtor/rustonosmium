//use rustc_hash::FxHashMap;
use approx::*;
use geographiclib_rs::{DirectGeodesic, Geodesic, InverseGeodesic};
use get_size::GetSize;
use std::{f64::consts::PI, fmt, num::ParseIntError, str::FromStr};
use uom::si::{
    angle::degree,
    f64::*,
    length::{kilometer, meter},
};

// <node id="106904" lat="51.5195553" lon="-0.0362329" version="5" timestamp="2020-06-15T19:23:35Z" changeset="86684855" uid="4948143" user="doublah">
//     <tag k="seamark:type" v="gate"/>
//     <tag k="waterway" v="lock_gate"/>
//   </node>

#[derive(Debug, Clone, GetSize)]
pub struct Node {
    pub id: i64,
    pub lat: Latitude,
    pub lon: Longitude,
    //pub tags: FxHashMap<String, String>,
    pub tags: Vec<Tag>,
}

impl Node {
    pub fn no_tags(id: i64, lat: Latitude, lon: Longitude) -> Self {
        Node {
            id,
            lat,
            lon,
            tags: Vec::new(),
        }
    }

    pub fn uninitialised() -> Self {
        Node {
            id: -1,
            lat: Latitude::NONE,
            lon: Longitude::NONE,
            tags: Vec::new(),
        }
    }

    pub fn position(&self) -> Position {
        Position {
            lat: self.lat,
            lon: self.lon,
        }
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Node {}

const PRECISITION_DECIMAL_PLACES: usize = 7;
const PRECISITION_FACTOR: f64 = 1e7;

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct Position {
    pub lat: Latitude,
    pub lon: Longitude,
}
impl Position {
    const EARTH_RADIUS_KM: f64 = 6371.009;

    pub fn to_floats(self) -> Self {
        Position {
            lat: self.lat.to_float(),
            lon: self.lon.to_float(),
        }
    }

    pub fn distance_approximate(&self, other: &Self) -> Length {
        let self_lat_rad = self.lat.as_radians();
        let self_lon_rad = self.lon.as_radians();
        let other_lat_rad = other.lat.as_radians();
        let other_lon_rad = other.lon.as_radians();

        //let dlat = self_lat_rad - other_lat_rad;
        let dlon = self_lon_rad - other_lon_rad;

        let cos_phi_1 = self_lat_rad.cos();
        let cos_phi_2 = other_lat_rad.cos();
        let sin_phi_1 = self_lat_rad.sin();
        let sin_phi_2 = other_lat_rad.sin();
        let cos_dlon = dlon.cos();
        let upper = (cos_phi_2 * dlon.sin()).powi(2)
            + (cos_phi_1 * sin_phi_2 - sin_phi_1 * cos_phi_2 * cos_dlon).powi(2);
        let lower = sin_phi_1 * sin_phi_2 + cos_phi_1 * cos_phi_2 * cos_dlon;
        let central_angle = upper.sqrt().atan2(lower);
        let dist_km = central_angle * Self::EARTH_RADIUS_KM;
        Length::new::<kilometer>(dist_km)
    }

    pub fn distance_accurate(&self, other: &Self) -> Length {
        let self_lat_deg = self.lat.as_degrees();
        let self_lon_deg = self.lon.as_degrees();
        let other_lat_deg = other.lat.as_degrees();
        let other_lon_deg = other.lon.as_degrees();

        let g = Geodesic::wgs84();
        let dist_m: f64 = g.inverse(self_lat_deg, self_lon_deg, other_lat_deg, other_lon_deg);

        Length::new::<meter>(dist_m)
    }

    pub fn travel_approximate(&self, course: Angle, distance: Length) -> Position {
        todo!("Fix me!");
        // let dr = distance.get::<kilometer>() / Self::EARTH_RADIUS_KM;
        // let lat_rad = self.lat.as_radians();
        // let lon_rad = self.lon.as_radians();
        // let course_rad = course.get::<radian>();
        // let sin_lat = lat_rad.sin();
        // let cos_lat = lat_rad.cos();
        // let cos_dr = dr.cos();
        // let sin_dr = dr.sin();
        // let lat2_rad = (sin_lat * cos_dr + cos_lat * sin_dr * course_rad.sin()).asin();
        // let lon2_rad = (course_rad.sin() * sin_dr * cos_lat).atan2(cos_dr - sin_lat * lat2_rad.sin());
        // let lon3_rad = (lon_rad + lon2_rad + PI) % (2.0 * PI) - PI;
        // let new_lat = Latitude::from_radians(lat2_rad);
        // let new_lon = Longitude::from_radians(lon3_rad);
        // Position {
        //     lat: new_lat,
        //     lon: new_lon,
        // }
    }

    /// Calculate a target position from here by travelleing along a `course` for a certain `distance`
    ///
    /// This method uses a more exact(and expensive) calculation than [travel_approximate](Position::travel_approximate).
    ///
    /// - `course` is the direction to travel in, given in [0º, 360º) (or something equivalent in different units)
    /// - `distance` is the length of the arc to cover; must be non-negative
    pub fn travel_accurate(&self, course: Angle, distance: Length) -> Position {
        let distance_m = distance.get::<meter>();
        assert!(distance_m >= 0.0);
        let course_deg = course.get::<degree>();
        assert!((0.0..360.0).contains(&course_deg));
        // convert to the [-180, 180]
        let azimuth = if course_deg <= 180.0 {
            course_deg
        } else {
            course_deg - 360.0
        };
        let lat_deg = self.lat.as_degrees();
        let lon_deg = self.lon.as_degrees();

        let g = Geodesic::wgs84();
        let (new_lat_deg, new_lon_deg) = g.direct(lat_deg, lon_deg, azimuth, distance_m);

        let new_lat = Latitude::from_degrees(new_lat_deg);
        let new_lon = Longitude::from_degrees(new_lon_deg);
        Position {
            lat: new_lat,
            lon: new_lon,
        }
    }
}

impl FromStr for Position {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let coords: Vec<&str> = s
            .trim_matches(|p| p == '(' || p == ')')
            .split(',')
            .collect();

        let lat: Latitude = coords[0].trim().parse()?;
        let lon: Longitude = coords[1].trim().parse()?;

        Ok(Position { lat, lon })
    }
}

impl AbsDiffEq for Position {
    type Epsilon = f64;

    fn default_epsilon() -> Self::Epsilon {
        f64::EPSILON
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        self.lat.abs_diff_eq(&other.lat, epsilon) && self.lon.abs_diff_eq(&other.lon, epsilon)
    }
}

#[derive(Debug, Clone, Copy, GetSize)]
pub enum Latitude {
    Decimal(i8, u32),
    Tight(i32),
    Float(f64),
}

impl Latitude {
    pub const NONE: Latitude = Latitude::Float(f64::NAN);

    pub fn from_radians(n: f64) -> Self {
        let deg = n * 180.0 / PI;
        assert!(
            (-90.0..=90.0).contains(&deg),
            "Latitude must be within [-90º, 90º], not {}º",
            deg
        );
        Latitude::Float(deg)
    }

    pub fn from_degrees(n: f64) -> Self {
        assert!(
            (-90.0..=90.0).contains(&n),
            "Latitude must be within [-90º, 90º], not {}º",
            n
        );
        Latitude::Float(n)
    }

    pub fn to_float(self) -> Self {
        match self {
            Latitude::Decimal(deg, frac) => {
                Latitude::Float((deg as f64) + (frac as f64) / PRECISITION_FACTOR)
            }
            Latitude::Tight(mult) => Latitude::Float((mult as f64) / PRECISITION_FACTOR),
            Latitude::Float(_) => self,
        }
    }

    pub fn to_decimal(self) -> Self {
        match self {
            Latitude::Decimal(_, _) => self,
            Latitude::Tight(mult) => {
                // round towards zero
                let deg = mult / PRECISITION_FACTOR as i32;
                let frac = mult - deg;
                Latitude::Decimal(deg as i8, frac as u32)
            }
            Latitude::Float(f) => {
                let deg = f.round();
                let frac = (f - deg) * PRECISITION_FACTOR;
                Latitude::Decimal(deg as i8, frac as u32)
            }
        }
    }

    pub fn to_tight(self) -> Self {
        match self {
            Latitude::Decimal(deg, frac) => {
                Latitude::Tight((deg as i32) * (PRECISITION_FACTOR as i32) + (frac as i32))
            }
            Latitude::Tight(_mult) => self,
            Latitude::Float(f) => Latitude::Tight((f * PRECISITION_FACTOR).round() as i32),
        }
    }

    pub fn as_radians(&self) -> f64 {
        if let Latitude::Float(n) = self.to_float() {
            n * PI / 180.0
        } else {
            unreachable!("Must be a Float instance now!");
        }
    }

    pub fn as_degrees(&self) -> f64 {
        if let Latitude::Float(n) = self.to_float() {
            n
        } else {
            unreachable!("Must be a Float instance now!");
        }
    }
}

impl FromStr for Latitude {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();

        let degrees: i8 = parts[0].parse()?;
        let fraction_str = parts[1];
        // we expect a fixed number of decimal places...must multiply if we don't get them
        let fac = PRECISITION_DECIMAL_PLACES - fraction_str.len();
        let fraction: u32 = fraction_str.parse()?;

        Ok(Latitude::Decimal(degrees, fraction * fac as u32))
    }
}

impl PartialEq for Latitude {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Latitude::Decimal(n1, f1) => match other {
                Latitude::Decimal(n2, f2) => n1 == n2 && f1 == f2,
                Latitude::Tight(_) => {
                    if let Latitude::Decimal(n2, f2) = other.to_decimal() {
                        *n1 == n2 && *f1 == f2
                    } else {
                        unreachable!("Must be decimal now!");
                    }
                }
                Latitude::Float(f2) => {
                    if let Latitude::Float(f1_float) = self.to_float() {
                        f1_float == *f2
                    } else {
                        unreachable!("Must be float now!");
                    }
                }
            },
            Latitude::Tight(n1) => match other {
                Latitude::Decimal(n2, f2) => {
                    if let Latitude::Decimal(n1, f1) = self.to_decimal() {
                        n1 == *n2 && f1 == *f2
                    } else {
                        unreachable!("Must be decimal now!");
                    }
                }
                Latitude::Tight(n2) => n1 == n2,
                Latitude::Float(f2) => {
                    if let Latitude::Float(f1_float) = self.to_float() {
                        f1_float == *f2
                    } else {
                        unreachable!("Must be float now!");
                    }
                }
            },
            Latitude::Float(f1) => {
                if let Latitude::Float(f2) = other.to_float() {
                    *f1 == f2
                } else {
                    unreachable!("Must be float now!");
                }
            }
        }
    }
}

impl AbsDiffEq for Latitude {
    type Epsilon = f64;

    fn default_epsilon() -> Self::Epsilon {
        f64::EPSILON
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        assert!(
            epsilon < 1.0 && 0.0 < epsilon,
            "Only use this with fractional epsilon!"
        );
        match self {
            Latitude::Float(f1) => {
                if let Latitude::Float(f2) = other.to_float() {
                    f1.abs_diff_eq(&f2, epsilon)
                } else {
                    unreachable!("Must be float now!");
                }
            }
            _ => match other {
                Latitude::Float(f2) => {
                    if let Latitude::Float(f1) = self.to_float() {
                        f1.abs_diff_eq(&f2, epsilon)
                    } else {
                        unreachable!("Must be float now!");
                    }
                }
                _ => self == other, // just use normal comparisons for integer variants
            },
        }
    }
}

impl fmt::Display for Latitude {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Latitude::Float(num) = self.to_float() {
            let dir = if num > 0.0 { "N" } else { "S" };
            write!(f, "{}º{}", num.abs(), dir)
        } else {
            unreachable!("Must be float now!");
        }
    }
}

#[derive(Debug, Clone, Copy, GetSize)]
pub enum Longitude {
    Decimal(i16, u32),
    Tight(i32),
    Float(f64),
}

impl Longitude {
    pub const NONE: Longitude = Longitude::Float(f64::NAN);

    pub fn from_radians(n: f64) -> Self {
        let deg = n * 180.0 / PI;
        assert!(
            (-180.0..=180.0).contains(&deg),
            "Longitude must be within [-180º, 180º], not {}º",
            deg
        );
        Longitude::Float(deg)
    }

    pub fn from_degrees(n: f64) -> Self {
        assert!(
            (-180.0..=180.0).contains(&n),
            "Longitude must be within [-180º, 180º], not {}º",
            n
        );
        Longitude::Float(n)
    }

    pub fn to_float(self) -> Self {
        match self {
            Longitude::Decimal(deg, frac) => {
                Longitude::Float((deg as f64) + (frac as f64) / PRECISITION_FACTOR)
            }
            Longitude::Tight(mult) => Longitude::Float((mult as f64) / PRECISITION_FACTOR),
            Longitude::Float(_) => self,
        }
    }

    pub fn to_decimal(self) -> Self {
        match self {
            Longitude::Decimal(_, _) => self,
            Longitude::Tight(mult) => {
                // round towards zero
                let deg = mult / PRECISITION_FACTOR as i32;
                let frac = mult - deg;
                Longitude::Decimal(deg as i16, frac as u32)
            }
            Longitude::Float(f) => {
                let deg = f.round();
                let frac = (f - deg) * PRECISITION_FACTOR;
                Longitude::Decimal(deg as i16, frac as u32)
            }
        }
    }

    pub fn to_tight(self) -> Self {
        match self {
            Longitude::Decimal(deg, frac) => {
                Longitude::Tight((deg as i32) * (PRECISITION_FACTOR as i32) + (frac as i32))
            }
            Longitude::Tight(mult) => self,
            Longitude::Float(f) => Longitude::Tight((f * PRECISITION_FACTOR).round() as i32),
        }
    }

    pub fn as_radians(&self) -> f64 {
        if let Longitude::Float(n) = self.to_float() {
            n * std::f64::consts::PI / 180.0
        } else {
            panic!("Must be a Float instance now!");
        }
    }

    pub fn as_degrees(&self) -> f64 {
        if let Longitude::Float(n) = self.to_float() {
            n
        } else {
            unreachable!("Must be a Float instance now!");
        }
    }
}

impl FromStr for Longitude {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();

        let degrees: i16 = parts[0].parse()?;
        let fraction_str = parts[1];
        // we expect a fixed number of decimal places...must multiply if we don't get them
        let fac = PRECISITION_DECIMAL_PLACES - fraction_str.len();
        let fraction: u32 = fraction_str.parse()?;

        Ok(Longitude::Decimal(degrees, fraction * fac as u32))
    }
}

impl PartialEq for Longitude {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Longitude::Decimal(n1, f1) => match other {
                Longitude::Decimal(n2, f2) => n1 == n2 && f1 == f2,
                Longitude::Tight(_) => {
                    if let Longitude::Decimal(n2, f2) = other.to_decimal() {
                        *n1 == n2 && *f1 == f2
                    } else {
                        unreachable!("Must be decimal now!");
                    }
                }
                Longitude::Float(f2) => {
                    if let Longitude::Float(f1_float) = self.to_float() {
                        f1_float == *f2
                    } else {
                        unreachable!("Must be float now!");
                    }
                }
            },
            Longitude::Tight(n1) => match other {
                Longitude::Decimal(n2, f2) => {
                    if let Longitude::Decimal(n1, f1) = self.to_decimal() {
                        n1 == *n2 && f1 == *f2
                    } else {
                        unreachable!("Must be decimal now!");
                    }
                }
                Longitude::Tight(n2) => n1 == n2,
                Longitude::Float(f2) => {
                    if let Longitude::Float(f1_float) = self.to_float() {
                        f1_float == *f2
                    } else {
                        unreachable!("Must be float now!");
                    }
                }
            },
            Longitude::Float(f1) => {
                if let Longitude::Float(f2) = other.to_float() {
                    *f1 == f2
                } else {
                    unreachable!("Must be float now!");
                }
            }
        }
    }
}

impl AbsDiffEq for Longitude {
    type Epsilon = f64;

    fn default_epsilon() -> Self::Epsilon {
        f64::EPSILON
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        assert!(
            epsilon < 1.0 && 0.0 < epsilon,
            "Only use this with fractional epsilon!"
        );
        match self {
            Longitude::Float(f1) => {
                if let Longitude::Float(f2) = other.to_float() {
                    f1.abs_diff_eq(&f2, epsilon)
                } else {
                    unreachable!("Must be float now!");
                }
            }
            _ => match other {
                Longitude::Float(f2) => {
                    if let Longitude::Float(f1) = self.to_float() {
                        f1.abs_diff_eq(&f2, epsilon)
                    } else {
                        unreachable!("Must be float now!");
                    }
                }
                _ => self == other, // just use normal comparisons for integer variants
            },
        }
    }
}

impl fmt::Display for Longitude {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Longitude::Float(num) = self.to_float() {
            let dir = if num > 0.0 { "E" } else { "W" };
            write!(f, "{}º{}", num.abs(), dir)
        } else {
            unreachable!("Must be float now!");
        }
    }
}

#[derive(Debug, Clone, GetSize)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use uom::si::angle::degree;

    #[test]
    fn test_approx_distance() {
        let pos1: Position = "(45.79, 3.97)".parse().unwrap();
        let pos2: Position = "(-32.0, -122.7)".parse().unwrap();

        let dist = pos1.distance_approximate(&pos2);
        let dist_km = dist.get::<kilometer>().round() as i64;
        let diff = (dist_km - 15239i64).abs();
        assert!(diff < 200); // < 1% difference
    }

    #[test]
    fn test_approx_travel() {
        //Input (lat, long) = (45, 0), angle = 0, distance = radius * deg2rad(90) => (45, 180) (as I said before)
        {
            let pos: Position = "(45.0, 0.0)".parse().unwrap();
            let distance = Length::new::<kilometer>(Position::EARTH_RADIUS_KM * 0.5 * PI);
            //let result = pos.travel_approximate(Angle::new::<degree>(0.0), distance);
            let expected: Position = "(45.0, 180.0)".parse().unwrap();
            //assert_abs_diff_eq!(result, expected, epsilon = 0.5);
            let result2 = pos.travel_accurate(Angle::new::<degree>(0.0), distance);
            assert_abs_diff_eq!(result2, expected, epsilon = 0.5);
        }
        //Input (lat, long) = (0, 0), angle = 90, distance = radius * deg2rad(90) => (0, 90) (as expected - start at equator, travel east by 90 longitude)
        {
            let pos: Position = "(0.0, 0.0)".parse().unwrap();
            let distance = Length::new::<kilometer>(Position::EARTH_RADIUS_KM * 0.5 * PI);
            //let result = pos.travel_approximate(Angle::new::<degree>(90.0), distance);
            let expected: Position = "(0.0, 90.0)".parse().unwrap();
            //assert_abs_diff_eq!(result, expected, epsilon = 0.5);
            let result2 = pos.travel_accurate(Angle::new::<degree>(90.0), distance);
            assert_abs_diff_eq!(result2, expected, epsilon = 0.5);
        }
        //Input (lat, long) = (54, 29), angle = 36, distance = radius * deg2rad(360) => (54, 29) (as expected - start at any random position and go full-circle in any direction)
        {
            let pos: Position = "(54.0, 29.0)".parse().unwrap();
            let distance = Length::new::<kilometer>(Position::EARTH_RADIUS_KM * 2.0 * PI);
            //let result = pos.travel_approximate(Angle::new::<degree>(36.0), distance);
            //assert_abs_diff_eq!(result, pos, epsilon = 0.5);
            let result2 = pos.travel_accurate(Angle::new::<degree>(36.0), distance);
            assert_abs_diff_eq!(result2, pos, epsilon = 0.5);
        }
        //Interesting case: input (lat, long) = (30, 0), everything else same. => (0, 90) (we expected (30, 90)? - not starting at equator, travel by 90 degrees to North)
        {
            let pos: Position = "(30.0, 0.0)".parse().unwrap();
            let distance = Length::new::<kilometer>(Position::EARTH_RADIUS_KM * 0.5 * PI);
            //let result = pos.travel_approximate(Angle::new::<degree>(90.0), distance);
            let expected: Position = "(0.0, 90.0)".parse().unwrap();
            //assert_abs_diff_eq!(result, expected, epsilon = 0.5);
            let result2 = pos.travel_accurate(Angle::new::<degree>(90.0), distance);
            assert_abs_diff_eq!(result2, expected, epsilon = 0.5);
        }
    }
}

//use rustc_hash::FxHashMap;
use geographiclib_rs::{Geodesic, InverseGeodesic};
use std::{num::ParseIntError, str::FromStr};
use uom::si::{
    f64::*,
    length::{kilometer, meter},
};

// <node id="106904" lat="51.5195553" lon="-0.0362329" version="5" timestamp="2020-06-15T19:23:35Z" changeset="86684855" uid="4948143" user="doublah">
//     <tag k="seamark:type" v="gate"/>
//     <tag k="waterway" v="lock_gate"/>
//   </node>

#[derive(Debug, Clone)]
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

const PRECISITION_FACTOR: f64 = 1e10;

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct Position {
    lat: Latitude,
    lon: Longitude,
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
        let upper =
            (cos_phi_2 * dlon.sin()).powi(2) + (cos_phi_1 * sin_phi_2 - sin_phi_1 * cos_phi_2 * cos_dlon).powi(2);
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
}

impl FromStr for Position {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let coords: Vec<&str> = s.trim_matches(|p| p == '(' || p == ')').split(',').collect();

        let lat: Latitude = coords[0].trim().parse()?;
        let lon: Longitude = coords[1].trim().parse()?;

        Ok(Position { lat, lon })
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Latitude {
    Decimal(i8, u32),
    Tight(i32),
    Float(f64),
}

impl Latitude {
    pub const NONE: Latitude = Latitude::Float(f64::NAN);

    pub fn to_float(self) -> Self {
        match self {
            Latitude::Decimal(deg, frac) => Latitude::Float((deg as f64) + (frac as f64) / PRECISITION_FACTOR),
            Latitude::Tight(mult) => Latitude::Float((mult as f64) / PRECISITION_FACTOR),
            Latitude::Float(_) => self,
        }
    }

    pub fn as_radians(&self) -> f64 {
        if let Latitude::Float(n) = self.to_float() {
            n * std::f64::consts::PI / 180.0
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
        let fraction: u32 = parts[1].parse()?;

        Ok(Latitude::Decimal(degrees, fraction))
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Longitude {
    Decimal(i16, u32),
    Tight(i32),
    Float(f64),
}

impl Longitude {
    pub const NONE: Longitude = Longitude::Float(f64::NAN);

    pub fn to_float(self) -> Self {
        match self {
            Longitude::Decimal(deg, frac) => Longitude::Float((deg as f64) + (frac as f64) / PRECISITION_FACTOR),
            Longitude::Tight(mult) => Longitude::Float((mult as f64) / PRECISITION_FACTOR),
            Longitude::Float(_) => self,
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
        let fraction: u32 = parts[1].parse()?;

        Ok(Longitude::Decimal(degrees, fraction))
    }
}

#[derive(Debug, Clone)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_approx_distance() {
        let pos1: Position = "(45.79, 3.97)".parse().unwrap();
        let pos2: Position = "(-32.0, -122.7)".parse().unwrap();

        let dist = pos1.distance_approximate(&pos2);
        let dist_km = dist.get::<kilometer>().round() as i64;
        let diff = (dist_km - 15239i64).abs();
        assert!(diff < 200); // < 1% difference
    }
}

#![feature(associated_type_defaults)]
#![allow(clippy::type_complexity)]
use std::{error::Error, fmt, fs::File, io::BufReader, path::Path};

pub mod databases;
pub mod geometry;
pub mod osm_data;
pub mod quick_xml_reader;
pub mod xml_rs_reader;
pub use databases::{OsmDatabase, OwnedOsmDatabase};

const BUFFER_SIZE: usize = 80 * 1024 * 1024; // 80 MB

#[derive(Debug)]
pub enum ScanError {
    IOError(std::io::Error),
    XmlReaderError(xml::reader::Error),
    QuickXmlError(quick_xml::Error),
    NumberParseError(std::num::ParseIntError),
    Other(String),
}

impl fmt::Display for ScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "A OSM Scanning error occurred")
    }
}

impl Error for ScanError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ScanError::IOError(e) => Some(e),
            ScanError::XmlReaderError(e) => Some(e),
            ScanError::QuickXmlError(e) => Some(e),
            ScanError::NumberParseError(e) => Some(e),
            ScanError::Other(_) => None,
        }
    }
}

impl From<std::io::Error> for ScanError {
    fn from(e: std::io::Error) -> Self {
        ScanError::IOError(e)
    }
}
impl From<xml::reader::Error> for ScanError {
    fn from(e: xml::reader::Error) -> Self {
        ScanError::XmlReaderError(e)
    }
}
impl From<quick_xml::Error> for ScanError {
    fn from(e: quick_xml::Error) -> Self {
        ScanError::QuickXmlError(e)
    }
}
impl From<std::num::ParseIntError> for ScanError {
    fn from(e: std::num::ParseIntError) -> Self {
        ScanError::NumberParseError(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use osm_data::Position;
    use std::time::Instant;
    use uom::si::{f64::*, length::kilometer};

    pub const TEST_OSM_FILE: &str = "/Users/lkroll/Programming/Sailing/test-data/world.osm";

    // #[test]
    // fn test_scan_nodes_xml_rs() {
    //     let total = xml_rs_reader::scan_nodes(
    //         TEST_OSM_FILE,
    //         |_node, acc| {
    //             //println!("Node: {:?}", _node);
    //             acc + 1
    //         },
    //         0,
    //     )
    //     .expect("scanned");
    //     assert_eq!(1346516, total);
    // }

    #[test]
    fn test_scan_nodes_quick_xml() {
        let total = quick_xml_reader::scan_nodes(
            TEST_OSM_FILE,
            |_node, acc| {
                //println!("Node: {:?}", _node);
                acc + 1
            },
            0,
        )
        .expect("scanned");
        assert_eq!(1346516, total);
    }

    pub fn test_filter_tags(db: impl OsmDatabase) {
        let res = db.nodes_with_tag("seamark:type", Some("light_minor"));
        assert_eq!(20762, res.len());
    }

    pub fn test_filter_tags_owned(db: impl OwnedOsmDatabase) {
        let res = db.nodes_with_tag("seamark:type", Some("light_minor"));
        assert_eq!(20762, res.len());
    }

    pub fn test_nodes_in_distance(db: impl OsmDatabase) {
        let pos: Position = "(54.5, -8.3)".parse().expect("pos");
        let dist = Length::new::<kilometer>(100.0);
        let nodes = db.nodes_in_radius(pos, dist);
        assert_eq!(540, nodes.len());
    }

    pub fn test_nodes_in_distance_owned(db: impl OwnedOsmDatabase) {
        let pos: Position = "(54.5, -8.3)".parse().expect("pos");
        let dist = Length::new::<kilometer>(100.0);
        let nodes = db.nodes_in_radius(pos, dist);
        assert_eq!(540, nodes.len());
    }
}

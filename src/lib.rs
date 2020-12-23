use std::{error::Error, fmt, fs::File, io::BufReader, path::Path};

pub mod databases;
pub mod osm_data;
pub mod quick_xml_reader;
pub mod xml_rs_reader;
pub use databases::OsmDatabase;

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

    use std::time::Instant;

    const TEST_OSM_FILE: &str = "/Users/lkroll/Programming/Sailing/test-data/world.osm";

    #[test]
    fn test_scan_nodes_xml_rs() {
        let total = xml_rs_reader::scan_nodes(
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

    #[test]
    fn test_filter_tags_fx_hash_id_db() {
        let db = databases::fx_hash_id_db::from_file(TEST_OSM_FILE).expect("load db");
        let start = Instant::now();
        test_filter_tags(db);
        let elap = start.elapsed();
        println!("elapsed: {}ms", elap.as_millis());
    }

    fn test_filter_tags(db: impl OsmDatabase) {
        let res = db.nodes_with_tag("seamark:type", Some("light_minor"));
        assert_eq!(20762, res.len());
    }

    // #[test]
    // fn test_all_in_memory() {
    //     let mut nodes: FxHashMap<i64, osm_data::Node> = FxHashMap::default();
    //     quick_xml_reader::scan_nodes(
    //         TEST_OSM_FILE,
    //         |node, _acc| {
    //             //println!("Node: {:?}", _node);
    //             nodes.insert(node.id, node);
    //         },
    //         (),
    //     )
    //     .expect("scanned");
    //     assert_eq!(1346516, nodes.len());
    //     let mem_size = std::mem::size_of::<osm_data::Node>();
    //     println!(
    //         "Size of a node: {} (* 1346516 = {})",
    //         mem_size,
    //         mem_size * 1346516
    //     );
    //     loop {
    //         println!("Size of nodes: {}", std::mem::size_of_val(&nodes));
    //         std::thread::sleep(std::time::Duration::from_millis(1000));
    //         //nodes.pop().expect("node");
    //     }
    // }
}

use uom::si::f64::*;

use crate::osm_data::*;

pub mod fx_hash_id_db;
mod indices;
pub mod rtree_indexed_db;
pub mod rtree_indexed_disk_db;
pub mod temp_disk_db;

pub trait OsmDatabase {
    fn node_by_id(&self, id: i64) -> Option<&Node>;
    fn nodes_in_radius(&self, pos: Position, radius: Length) -> Vec<&Node>;
    fn nodes_with_tag(&self, key: &str, value: Option<&str>) -> Vec<&Node>;
    fn nodes(&self) -> Vec<&Node>;
}

pub trait OwnedOsmDatabase {
    fn node_by_id(&self, id: i64) -> Option<Node>;
    fn nodes_in_radius(&self, pos: Position, radius: Length) -> Vec<Node>;
    fn nodes_with_tag(&self, key: &str, value: Option<&str>) -> Vec<Node>;
    // Not a good idea to load them all into memory
    //fn nodes(&self) -> Vec<Node>;
}

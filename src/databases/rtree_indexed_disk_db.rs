use super::*;
use crate::{
    geometry::{planar::*, spherical::*},
    ScanError,
};
use bytes::{Buf, BufMut};
use indices::r_tree::on_disk::*;
use rtree_indexed_db::NodeLocation;
use std::path::Path;
use temp_disk_db::TempDiskDb;

use uom::si::{f64::*, length::kilometer};

pub fn from_file<P>(path: P) -> Result<RTreeIndexedDiskDb, ScanError>
where
    P: AsRef<Path>,
{
    let db = temp_disk_db::from_file(path)?;
    Ok(RTreeIndexedDiskDb::with_db(db))
}

impl FixedSizeSerde for NodeLocation {
    const SIZE: usize = Point::SIZE + 8;

    fn ser_into(&self, mut buf: impl BufMut) {
        self.location.ser_into(&mut buf);
        buf.put_i64(self.node_id);
    }

    fn deser_from(mut buf: impl Buf) -> Self {
        let location = Point::deser_from(&mut buf);
        let node_id = buf.get_i64();
        NodeLocation { location, node_id }
    }
}

pub struct RTreeIndexedDiskDb {
    disk_db: TempDiskDb,
    index: RTree<NodeLocation>,
}

impl RTreeIndexedDiskDb {
    fn with_db(disk_db: TempDiskDb) -> Self {
        let mut index = with_capacity(disk_db.len());
        disk_db
            .scan_nodes()
            .map(|node| {
                let node = node.into_osm();
                let location = Point {
                    x: node.lon.as_degrees(),
                    y: node.lat.as_degrees(),
                };
                NodeLocation {
                    location,
                    node_id: node.id,
                }
            })
            .for_each(|location| index.insert(location));
        if cfg!(test) {
            index.assert_invariants();
            index.print_stats();
        }
        RTreeIndexedDiskDb { disk_db, index }
    }
}

impl OwnedOsmDatabase for RTreeIndexedDiskDb {
    fn node_by_id(&self, id: i64) -> Option<Node> {
        self.disk_db.node_by_id(id)
    }

    fn nodes_in_radius(&self, pos: Position, radius: Length) -> Vec<Node> {
        let radius_km = radius.get::<kilometer>();
        let error_radius_km = radius_km * 1.1;
        let error_radius = Length::new::<kilometer>(error_radius_km);
        let bounding_box = BoundingBox::from_circle(pos, error_radius);
        let pre_selection = self.index.search(&bounding_box);
        //println!("Found {} candidates in index", pre_selection.len());
        let nodes = pre_selection.into_iter().map(|location| {
            self.disk_db
                .node_by_id(location.node_id)
                .expect("Nodes in index that aren't in directory")
        });
        nodes
            .filter(|n| n.position().distance_accurate(&pos) < radius)
            .collect()
    }

    fn nodes_with_tag(&self, key_pattern: &str, value_pattern: Option<&str>) -> Vec<Node> {
        self.disk_db.nodes_with_tag(key_pattern, value_pattern)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[ignore = "slow test"]
    #[test]
    fn test_filter_tags() {
        let db = from_file(crate::tests::TEST_OSM_FILE).expect("load db");
        crate::tests::test_filter_tags_owned(db);
    }

    #[ignore = "slow test"]
    #[test]
    fn test_nodes_in_distance() {
        let db = from_file(crate::tests::TEST_OSM_FILE).expect("load db");
        crate::tests::test_nodes_in_distance_owned(db);
    }

    #[test]
    fn test_ser_deser() {
        let mut buf: Vec<u8> = Vec::new();
        let p = Point { x: 10.0, y: -25.0 };
        let l = NodeLocation {
            location: p,
            node_id: 42,
        };
        l.ser_into(&mut buf);
        let deser = NodeLocation::deser_from(buf.as_slice());
        assert_eq!(p, l.location);
        assert_eq!(42, deser.node_id);
    }
}

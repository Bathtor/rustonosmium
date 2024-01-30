use super::*;
use crate::{
    geometry::{planar::*, spherical::*, Bounding, Extending},
    ScanError,
};
use indices::r_tree::in_memory::*;
use rustc_hash::FxHashMap;
use std::path::Path;

use uom::si::{f64::*, length::kilometer};

pub fn from_file<P>(path: P) -> Result<RTreeIndexedDb, ScanError>
where
    P: AsRef<Path>,
{
    let mut nodes: FxHashMap<i64, Node> = FxHashMap::default();
    crate::quick_xml_reader::scan_nodes(
        path,
        |node, _acc| {
            nodes.insert(node.id, node);
        },
        (),
    )?;
    Ok(RTreeIndexedDb::with_nodes(nodes))
}

#[derive(Debug, Clone)]
pub(super) struct NodeLocation {
    pub(super) location: Point,
    pub(super) node_id: i64,
}

impl Extending<Rectangle> for NodeLocation {
    fn extend(&self, geometry: Rectangle) -> Rectangle {
        self.location.extend(geometry)
    }
}

impl AsRef<Point> for NodeLocation {
    fn as_ref(&self) -> &Point {
        &self.location
    }
}

impl Bounding for NodeLocation {
    type SplitUse = Point;

    fn bound_all<'a>(entries: impl Iterator<Item = &'a Self>) -> Rectangle {
        Point::bound_all(entries.map(|e| &e.location))
    }

    fn find_split<T>(entries: Vec<T>, min_fill: usize, max_fill: usize) -> (Vec<T>, Vec<T>)
    where
        T: AsRef<Self::SplitUse>,
    {
        Point::find_split(entries, min_fill, max_fill)
    }
}

pub struct RTreeIndexedDb {
    nodes: FxHashMap<i64, Node>,
    index: RTree<NodeLocation>,
}

impl RTreeIndexedDb {
    fn with_nodes(nodes: FxHashMap<i64, Node>) -> Self {
        let mut index = with_capacity(nodes.len());
        nodes
            .values()
            .map(|node| {
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
        }
        RTreeIndexedDb { nodes, index }
    }
}

impl OsmDatabase for RTreeIndexedDb {
    fn node_by_id(&self, id: i64) -> Option<&Node> {
        self.nodes.get(&id)
    }

    fn nodes_in_radius(&self, pos: Position, radius: Length) -> Vec<&Node> {
        let radius_km = radius.get::<kilometer>();
        let error_radius_km = radius_km * 1.1;
        let error_radius = Length::new::<kilometer>(error_radius_km);
        let bounding_box = BoundingBox::from_circle(pos, error_radius);
        let pre_selection = self.index.search(&bounding_box);
        //println!("Found {} candidates in index", pre_selection.len());
        let nodes = pre_selection.into_iter().map(|location| {
            self.nodes
                .get(&location.node_id)
                .expect("Nodes in index that aren't in directory")
        });
        nodes
            .filter(|n| n.position().distance_accurate(&pos) < radius)
            .collect()
    }

    fn nodes_with_tag(&self, key_pattern: &str, value_pattern: Option<&str>) -> Vec<&Node> {
        let filter_fun: Box<dyn Fn(&Node) -> bool> = if let Some(vp) = value_pattern {
            let vp = vp.to_string();
            let c = move |n: &Node| {
                n.tags
                    .iter()
                    .any(|t| t.key.contains(key_pattern) && t.value.contains(&vp))
            };
            Box::new(c)
        } else {
            let c = move |n: &Node| n.tags.iter().any(|t| t.key.contains(key_pattern));
            Box::new(c)
        };
        self.nodes.values().filter(|n| filter_fun(n)).collect()
    }

    fn nodes(&self) -> Vec<&Node> {
        self.nodes.values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[ignore = "slow test"]
    #[test]
    fn test_filter_tags() {
        let db = from_file(crate::tests::TEST_OSM_FILE).expect("load db");
        crate::tests::test_filter_tags(db);
    }

    #[ignore = "slow test"]
    #[test]
    fn test_nodes_in_distance() {
        let db = from_file(crate::tests::TEST_OSM_FILE).expect("load db");
        crate::tests::test_nodes_in_distance(db);
    }
}

use super::*;
use crate::ScanError;
use rustc_hash::FxHashMap;
use std::path::Path;

use uom::si::{f64::*, length::kilometer};

pub fn from_file<P>(path: P) -> Result<FxHashIdDb, ScanError>
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
    Ok(FxHashIdDb { nodes })
}

pub struct FxHashIdDb {
    nodes: FxHashMap<i64, Node>,
}

impl OsmDatabase for FxHashIdDb {
    fn node_by_id(&self, id: i64) -> Option<&Node> {
        self.nodes.get(&id)
    }

    fn nodes_in_radius(&self, pos: Position, radius: Length) -> Vec<&Node> {
        let radius_km = radius.get::<kilometer>();
        let error_radius_km = radius_km * 1.1;
        let error_radius = Length::new::<kilometer>(error_radius_km);
        self.nodes
            .values()
            .filter(|n| n.position().distance_approximate(&pos) < error_radius)
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

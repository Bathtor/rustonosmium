use std::{
    collections::{BTreeMap, HashSet},
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    time::Instant,
};

use itertools::Itertools;
use osmpbf::{Element, ElementReader, IndexedReader, RelMemberType};

use crate::{
    osm_data::{Latitude, Longitude, Node},
    ScanError,
};

#[derive(Clone, Debug, PartialEq)]
pub struct Way<T> {
    pub id: i64,
    pub nodes: Vec<T>,
}

impl<T> Way<T> {
    pub fn map_nodes<F, R>(self, mapper: F) -> Result<Way<R>, ScanError>
    where
        F: Fn(T) -> Option<R>,
    {
        let mut new_nodes: Vec<R> = Vec::new();
        for node in self.nodes {
            if let Some(new_node) = mapper(node) {
                new_nodes.push(new_node);
            } else {
                return Err(ScanError::MissingNode);
            }
        }
        Ok(Way {
            id: self.id,
            nodes: new_nodes,
        })
    }
}

impl<T: PartialEq> Way<T> {
    pub fn is_closed(&self) -> bool {
        self.nodes.first() == self.nodes.last()
    }
}

pub type ResolvedWay = Way<Node>;

impl<'a> From<&osmpbf::elements::Node<'a>> for Node {
    fn from(node: &osmpbf::elements::Node<'a>) -> Self {
        Node {
            id: node.id(),
            lat: Latitude::Float(node.lat()),
            lon: Longitude::Float(node.lon()),
            tags: Vec::new(),
        }
    }
}

impl<'a> From<&osmpbf::dense::DenseNode<'a>> for Node {
    fn from(node: &osmpbf::dense::DenseNode<'a>) -> Self {
        Node {
            id: node.id(),
            lat: Latitude::Float(node.lat()),
            lon: Longitude::Float(node.lon()),
            tags: Vec::new(),
        }
    }
}

pub struct SingleFileDatabase {
    path: Box<PathBuf>,
}

impl SingleFileDatabase {
    pub fn from_path<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        let owned = Box::new(path.as_ref().to_owned());
        Self { path: owned }
    }

    fn indexed_reader(&self) -> Result<IndexedReader<File>, ScanError> {
        let reader = IndexedReader::from_path(self.path.as_ref())?;
        Ok(reader)
    }

    fn element_reader(&self) -> Result<ElementReader<BufReader<File>>, ScanError> {
        let reader = ElementReader::from_path(self.path.as_ref())?;
        Ok(reader)
    }

    fn get_way_ids_for_multipolygon_matching<F>(
        &self,
        matcher: F,
    ) -> Result<HashSet<i64>, ScanError>
    where
        F: Fn(&osmpbf::Relation<'_>) -> bool + Send + Sync + 'static,
    {
        let reader = self.element_reader()?;
        reader
            .par_map_reduce(
                |element| {
                    match element {
                        Element::Relation(rel) => {
                            if rel.tags().contains(&("type", "multipolygon")) && matcher(&rel) {
                                let mut way_ids = HashSet::new();
                                for member in rel.members() {
                                    if member.member_type == RelMemberType::Way {
                                        way_ids.insert(member.member_id);
                                    }
                                }
                                way_ids
                            } else {
                                HashSet::new()
                            }
                        }
                        _ => HashSet::new(), // ignore others
                    }
                },
                HashSet::new,
                |mut left, right| {
                    left.extend(right);
                    left
                },
            )
            .map_err(Into::into)
    }

    pub fn coastlines(
        &self,
    ) -> Result<impl Iterator<Item = Result<ResolvedWay, ScanError>>, ScanError> {
        let mut reader = self.indexed_reader()?;
        let mut nodes: BTreeMap<i64, Node> = BTreeMap::new();
        let mut ways: BTreeMap<i64, Way<i64>> = BTreeMap::new();

        log::info!("Collecting required way ids...");
        let start = Instant::now();
        let mut required_way_ids = self.get_way_ids_for_multipolygon_matching(|rel| {
            rel.tags().any(|tag| tag.0 == "natural" && tag.1 == "beach")
        })?;
        required_way_ids.insert(961680619);
        log::info!(
            "Finished collecting required way ids in {}ms",
            start.elapsed().as_millis()
        );

        log::info!("Collecting natural coastline ways...");
        let start = Instant::now();
        reader.read_ways_and_deps(
            |way| {
                required_way_ids.contains(&way.id())
                    || way.tags().any(|(key, value)| {
                        key == "natural" && (value == "coastline" || value == "beach")
                    })
            },
            |element| match element {
                Element::Way(way) => {
                    let way_obj = Way {
                        id: way.id(),
                        nodes: way.refs().collect(),
                    };

                    if way.tags().contains(&("area", "yes")) {
                        assert_eq!(
                            way_obj.nodes.first(),
                            way_obj.nodes.last(),
                            "Way was marked as area, but was not closed"
                        );
                    }
                    ways.insert(way.id(), way_obj);
                }
                Element::Node(node) => {
                    nodes.insert(node.id(), node.into());
                }
                Element::DenseNode(node) => {
                    nodes.insert(node.id(), node.into());
                }
                Element::Relation(_) => unreachable!("Should not occur"),
            },
        )?;
        log::info!(
            "Finished collecting ways in {}ms",
            start.elapsed().as_millis()
        );

        let start = Instant::now();
        log::info!("Assemblying multipolygon ways...");
        enum ReducerState {
            Empty,
            Single {
                way_ids: HashSet<i64>,
                assembled_way: Way<i64>,
            },
            Combined {
                assembled_areas: Vec<Way<i64>>,
                ways_used_in_areas: HashSet<i64>,
            },
        }
        // let mut assembled_areas: Vec<Way<i64>> = Vec::new();
        // let mut ways_used_in_areas: HashSet<i64> = HashSet::new();

        let reader = self.element_reader()?;
        let final_reduced_state = reader.par_map_reduce(
            |element| {
                match element {
                    Element::Relation(rel) => {
                        if rel.tags().contains(&("type", "multipolygon"))
                            && rel.tags().any(|tag| tag.0 == "natural" && tag.1 == "beach")
                        {
                            let mut way_ids = HashSet::new();
                            log::info!("Assemblying multipolygon: id={}", rel.id());
                            let mut nodes: Vec<i64> = Vec::new();
                            for member in rel.members() {
                                if member.member_type == RelMemberType::Way
                                    && member.role().unwrap_or("outer") == "outer"
                                {
                                    if let Some(way) = ways.get(&member.member_id) {
                                        way_ids.insert(way.id);
                                        nodes.extend(way.nodes.iter().cloned());
                                    } else {
                                        log::warn!(
                                            "Missing way ({}) in relation ({}).",
                                            member.member_id,
                                            rel.id()
                                        );
                                    }
                                }
                            }
                            if nodes.is_empty() {
                                log::warn!("Multipolygon {} resulted in empty way!", rel.id());
                                ReducerState::Empty
                            } else {
                                let assembled_way = Way {
                                    id: rel.id(),
                                    nodes,
                                };
                                ReducerState::Single {
                                    way_ids,
                                    assembled_way,
                                }
                            }
                        } else {
                            ReducerState::Empty
                        }
                    }
                    _ => ReducerState::Empty, // ignore others
                }
            },
            || ReducerState::Empty,
            |left, right| match (left, right) {
                (r, ReducerState::Empty) => r,
                (ReducerState::Empty, r) => r,
                (
                    ReducerState::Combined {
                        assembled_areas: areas1,
                        ways_used_in_areas: ways1,
                    },
                    ReducerState::Combined {
                        assembled_areas: areas2,
                        ways_used_in_areas: ways2,
                    },
                ) => {
                    let mut assembled_areas = areas1;
                    assembled_areas.extend(areas2);
                    let mut ways_used_in_areas = ways1;
                    ways_used_in_areas.extend(ways2);
                    ReducerState::Combined {
                        assembled_areas,
                        ways_used_in_areas,
                    }
                }
                (
                    ReducerState::Combined {
                        mut assembled_areas,
                        mut ways_used_in_areas,
                    },
                    ReducerState::Single {
                        way_ids: ways2,
                        assembled_way,
                    },
                ) => {
                    assembled_areas.push(assembled_way);
                    ways_used_in_areas.extend(ways2);
                    ReducerState::Combined {
                        assembled_areas,
                        ways_used_in_areas,
                    }
                }
                (
                    ReducerState::Single {
                        way_ids: ways2,
                        assembled_way,
                    },
                    ReducerState::Combined {
                        mut assembled_areas,
                        mut ways_used_in_areas,
                    },
                ) => {
                    assembled_areas.push(assembled_way);
                    ways_used_in_areas.extend(ways2);
                    ReducerState::Combined {
                        assembled_areas,
                        ways_used_in_areas,
                    }
                }
                (
                    ReducerState::Single {
                        way_ids: ways1,
                        assembled_way: area1,
                    },
                    ReducerState::Single {
                        way_ids: ways2,
                        assembled_way: area2,
                    },
                ) => {
                    let mut ways_used_in_areas = ways1;
                    ways_used_in_areas.extend(ways2);
                    ReducerState::Combined {
                        assembled_areas: vec![area1, area2],
                        ways_used_in_areas,
                    }
                }
            },
        )?;
        // reader.for_each(|element| match element {
        //     Element::Relation(rel) => {
        //         if rel.tags().contains(&("type", "multipolygon"))
        //             && rel.tags().any(|tag| tag.0 == "natural")
        //         {
        //             log::info!("Assemblying multipolygon: id={}", rel.id());
        //             let mut nodes: Vec<i64> = Vec::new();
        //             for member in rel.members() {
        //                 if member.member_type == RelMemberType::Way
        //                     && member.role().unwrap_or("outer") == "outer"
        //                 {
        //                     if let Some(way) = ways.get(&member.member_id) {
        //                         ways_used_in_areas.insert(way.id);
        //                         nodes.extend(way.nodes.iter().cloned());
        //                     } else {
        //                         log::warn!(
        //                             "Missing way ({}) in relation ({}).",
        //                             member.member_id,
        //                             rel.id()
        //                         );
        //                     }
        //                 }
        //             }
        //             assembled_areas.push(Way {
        //                 id: rel.id(),
        //                 nodes,
        //             });
        //         }
        //     }
        //     _ => (), // ignore others
        // })?;
        log::info!(
            "Finished assembling multipolygon ways in {}ms",
            start.elapsed().as_millis()
        );

        log::info!("Resolving all nodes...");
        let collected_ways: Vec<Result<Way<Node>, ScanError>> = if let ReducerState::Combined {
            assembled_areas,
            ways_used_in_areas,
        } = final_reduced_state
        {
            ways.retain(|id, _| !ways_used_in_areas.contains(id));
            let resolved_ways = assembled_areas
                .into_iter()
                .chain(ways.into_values())
                .map(move |w| w.map_nodes(|node_id| nodes.get(&node_id).cloned()));

            resolved_ways.collect()
        } else {
            log::warn!("Reducer was not Combined!");
            let resolved_ways = ways
                .into_values()
                .map(move |w| w.map_nodes(|node_id| nodes.get(&node_id).cloned()));

            resolved_ways.collect()
        };
        Ok(collected_ways.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use get_size::GetSize;
    use itertools::Itertools;
    use std::{
        cmp,
        collections::{hash_map, BTreeMap, BTreeSet, HashMap, HashSet},
    };

    use osmpbf::{Element, ElementReader, IndexedReader};

    use crate::osm_data::{Latitude, Longitude, Node};

    #[ignore = "slow test"]
    #[test]
    fn test_coastlines() {
        let db = SingleFileDatabase::from_path(crate::tests::TEST_OSM_PBF_FILE);
        let num_coastlines = db.coastlines().unwrap().count();
        assert_eq!(num_coastlines, 595);
    }

    #[ignore = "experiment only"]
    #[test]
    fn test_ways() {
        let mut reader = IndexedReader::from_path(crate::tests::TEST_OSM_PBF_FILE).unwrap();

        let mut ways_in_coastlines = 0u64;
        let mut nodes_in_coastlines = 0u64;

        reader
            .read_ways_and_deps(
                |way| {
                    way.tags()
                        .any(|(key, value)| key == "natural" && value == "coastline")
                },
                |element| match element {
                    Element::Way(_way) => {
                        ways_in_coastlines += 1;
                        // expected_node_ids.clear();
                        // expected_node_ids.extend(way.refs());
                    }
                    Element::Node(_node) => {
                        nodes_in_coastlines += 1;
                        // assert!(expected_node_ids.contains(&node.id()));
                    }
                    Element::DenseNode(_node) => {
                        nodes_in_coastlines += 1;
                        // assert!(expected_node_ids.contains(&node.id()));
                    }
                    Element::Relation(_) => unreachable!("Should not occur"),
                },
            )
            .unwrap();
        println!("#Ways: {ways_in_coastlines}");
        println!("#Nodes: {nodes_in_coastlines}");
    }

    #[ignore = "experiment only"]
    #[test]
    fn test_scan() {
        let reader = ElementReader::from_path(crate::tests::TEST_OSM_PBF_FILE).unwrap();
        let mut ways = 0u64;
        let mut nodes = 0u64;
        let mut min_node_id = i64::MAX;
        let mut max_node_id = i64::MIN;
        let mut relations = 0u64;
        //let mut keys: HashSet<String> = HashSet::new();
        let mut natural_values: HashSet<String> = HashSet::new();
        let mut coastlines = 0u64;
        let mut coastlines_with_inline_position = 0u64;

        let mut relation_values: HashSet<String> = HashSet::new();

        //let mut node_lookup: HashMap<i64, Node> = HashMap::new();
        let mut node_lookup: BTreeMap<i64, Node> = BTreeMap::new();

        // Increment the counter by one for each way.
        reader
            .for_each(|element| match element {
                Element::Way(way) => {
                    if way.id() == 715427103 {
                        println!("Special way had tags={:?}", way.tags().collect_vec());
                    }
                    ways += 1;
                    let mut is_coastline = false;
                    for tag in way.tags() {
                        //keys.insert(tag.0.to_string());
                        if tag.0 == "natural" {
                            if !natural_values.contains(tag.1) {
                                natural_values.insert(tag.1.to_string());
                            }
                            if tag.1 == "coastline" {
                                is_coastline = true;
                            }
                        }
                    }
                    if is_coastline {
                        coastlines += 1;
                        if way.node_locations().next().is_some() {
                            coastlines_with_inline_position += 1;
                        }
                        // println!("num nodes={}", way.refs().len());
                        // println!(
                        //     "tags={}",
                        //     way.tags().map(|(k, v)| format!("{k} -> {v}")).join(", ")
                        // );
                    }
                }
                Element::DenseNode(node) => {
                    nodes += 1;
                    max_node_id = cmp::max(node.id(), max_node_id);
                    min_node_id = cmp::min(node.id(), min_node_id);
                    // for tag in node.tags() {
                    //     keys.insert(tag.0.to_string());
                    // }
                    let owned_node = Node {
                        id: node.id(),
                        lat: Latitude::Float(node.lat()),
                        lon: Longitude::Float(node.lon()),
                        tags: Vec::new(),
                    };
                    node_lookup.insert(node.id(), owned_node);
                }
                Element::Node(node) => {
                    nodes += 1;
                    max_node_id = cmp::max(node.id(), max_node_id);
                    min_node_id = cmp::min(node.id(), min_node_id);
                    // for tag in node.tags() {
                    //     keys.insert(tag.0.to_string());
                    // }
                    let owned_node = Node {
                        id: node.id(),
                        lat: Latitude::Float(node.lat()),
                        lon: Longitude::Float(node.lon()),
                        tags: Vec::new(),
                    };
                    node_lookup.insert(node.id(), owned_node);
                }
                Element::Relation(relation) => {
                    relations += 1;
                    for tag in relation.tags() {
                        if tag.0 == "natural" {
                            relation_values.insert(format!("{}={}", tag.0, tag.1));
                        }
                    }
                }
            })
            .unwrap();
        println!("#ways = {ways}");
        println!(
            "#nodes = {nodes}, max = {max_node_id}, min = {min_node_id}, range={}",
            max_node_id - min_node_id
        );
        println!("#relations = {relations}");
        println!("relation values = {relation_values:?}");
        println!(
            "Coastlines with inline position = {coastlines_with_inline_position} / {coastlines}"
        );
        println!("natural values: {natural_values:?}");
        println!("Nodes size: {}", node_lookup.get_size());
    }
}

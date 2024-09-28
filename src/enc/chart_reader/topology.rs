use super::*;
use crate::fail;
use data_types::DataStructure;
use snafu::ResultExt;

/// Defines how to resolve the relationships between features of the chart.
///
/// Charts can be represented at multiple levels of topology,
/// indicating how connected the individual features are.
/// Every level requires slightly different resolution, represented by one implementation of
/// this trait.
///
/// The topology in use for a particular file is found in the
/// [[DataSetStructureInformation::data_structure]] field.
pub trait TopologyResolver {
    /// Resolve the spatial features in `feature_record` and put them into `feature_info`'s
    /// `spatial_data` field.
    fn resolve_feature(
        &self,
        spatial_feature_index: &HashMap<RecordName, SpatialRecord>,
        feature_record: FeatureRecord,
        feature_info: &mut FeatureInfo,
    ) -> Result<()>;
}

pub fn resolver_for_data_structure(
    data_structure: &DataStructure,
) -> Result<&'static dyn TopologyResolver> {
    match data_structure {
        DataStructure::CartographicSpaghetti => Ok(&SPAGHETTI_RESOLVER),
        DataStructure::ChainNode => Ok(&CHAIN_NODE_RESOLVER),
        DataStructure::PlanarGraph => UnsupportedSnafu {
            description: "Planar graph topology",
        }
        .fail(),

        DataStructure::FullTopology => UnsupportedSnafu {
            description: "Full topology",
        }
        .fail(),
        _ => FormatViolationSnafu {
            description: format!("Expected topology level, but got {data_structure:?}"),
        }
        .fail(),
    }
}

/// A set of isolated nodes and edges.
/// Edges do not reference nodes.
/// Feature objects must not share spatial objects.
/// Point representations are coded as isolated nodes.
/// Line representations are coded as connected series of edges.
/// Area representations are coded as closing loops of edges.
struct SpaghettiResolver;
impl TopologyResolver for SpaghettiResolver {
    fn resolve_feature(
        &self,
        spatial_feature_index: &HashMap<RecordName, SpatialRecord>,
        feature_record: FeatureRecord,
        feature_info: &mut FeatureInfo,
    ) -> Result<()> {
        for pointer in feature_record.spatial_pointers {
            let spatial_record = spatial_feature_index
                .get(&pointer.name)
                .cloned() // Not ideal, but can optimize later, if necessary.
                .ok_or_else(|| EncError::pointer_lookup_failed(pointer.name))?;
            EncError::assert(
                || spatial_record.pointers.is_empty(),
                || {
                    format!(
                        "Spatial to spatial pointers are not supported in Cartographic Spaghetti"
                    )
                },
            )?;
            let data = extract_spatial_array(
                feature_record.field.object_geometric_primitive,
                spatial_record.data,
                false, // empty data is not allowed
            )?;
            feature_info
                .spatial_data
                .merge(data, pointer.usage_indicator)?;
        }
        EncError::assert(
            || !feature_info.spatial_data.is_empty(),
            || {
                // Should be printing the record here, but it's not available anymore.
                format!("Expected a feature with spatial data resolved, but no data was resolved for: {:?}", feature_info)
            },
        )?;
        Ok(())
    }
}
const SPAGHETTI_RESOLVER: SpaghettiResolver = SpaghettiResolver;

/// A set of nodes and edges.
/// Each edge must reference a connected node as its beginning and end (they may be the same node).
/// The geometry of the referenced node is not part of the edge (see clause 4.7.2).
/// Vector objects may be shared.
/// Point representations are coded as nodes (isolated or connected).
/// Line representations are coded as series of edges and connected nodes.
/// Area representations are coded as closing loops of edges starting and ending at a common connected node.
struct ChainNodeResolver;
impl TopologyResolver for ChainNodeResolver {
    fn resolve_feature(
        &self,
        spatial_feature_index: &HashMap<RecordName, SpatialRecord>,
        feature_record: FeatureRecord,
        feature_info: &mut FeatureInfo,
    ) -> Result<()> {
        let num_spatial_records = feature_record.spatial_pointers.len();
        for pointer in feature_record.spatial_pointers {
            let spatial_record = spatial_feature_index
                .get(&pointer.name)
                .cloned() // Not ideal, but can optimize later, if necessary.
                .ok_or_else(|| EncError::pointer_lookup_failed(pointer.name))?;
            // TODO: Interpret mask.
            let resolved_pointers_by_topology = {
                let mut resolved_pointers_by_topology = HashMap::new();
                for sub_pointer in spatial_record.pointers.iter() {
                    let resolved = spatial_feature_index
                        .get(&sub_pointer.name)
                        .cloned()
                        .ok_or_else(|| EncError::pointer_lookup_failed(pointer.name))?;
                    let existing = resolved_pointers_by_topology.insert(
                        sub_pointer
                            .topology_indicator
                            .expect("Pointers without topology are not supported"),
                        resolved,
                    );
                    assert!(
                        existing.is_none(),
                        "Multiple pointers of the same type ({:?}) are not (yet) supported",
                        sub_pointer.topology_indicator
                    );
                }
                resolved_pointers_by_topology
            };
            let mut data = extract_spatial_array(
                feature_record.field.object_geometric_primitive,
                spatial_record.data,
                true, // In Chain node it is ok for data to be empty, since the connected nodes themselves also form a simple edge.
            )
            .map_err(Box::new)
            .with_context(|_| WithExtraContextSnafu {
                context: format!(
                    "Info: {:?},\nresolved pointers: {:#?}n\nNum Pointers: {num_spatial_records}",
                    feature_info, resolved_pointers_by_topology
                ),
            })?;
            match data {
                SpatialDataSegment::Area(ref mut vertices) => {
                    EncError::assert(
                        || resolved_pointers_by_topology.len() == 2,
                        || {
                            format!("An area should only have a beginning and end node references, which should be the same node. Found: {:?}", resolved_pointers_by_topology)
                        },
                    )?;
                    let beginning = resolved_pointers_by_topology
                        .get(&TopologyIndicator::BeginningNode)
                        .with_context(|| FormatViolationSnafu {
                            description: "Area should have a beginning node".to_string(),
                        })?;
                    let end = resolved_pointers_by_topology
                        .get(&TopologyIndicator::EndNode)
                        .with_context(|| FormatViolationSnafu {
                            description: "Area should have an end node".to_string(),
                        })?;
                    vertices.insert(0, beginning.clone().expect_point()?);
                    vertices.push(end.clone().expect_point()?);
                }
                SpatialDataSegment::Line(ref mut vertices) => {
                    EncError::assert(
                        || pointer.usage_indicator.is_none(),
                        || {
                            format!(
                                "Usage indicator {:?} is not allowed for Line features.",
                                pointer.usage_indicator
                            )
                        },
                    )?;
                    EncError::assert(
                        || resolved_pointers_by_topology.len() == 2,
                        || {
                            format!("A line should only have a beginning and end node references. Found: {:?}", resolved_pointers_by_topology)
                        },
                    )?;
                    let beginning = resolved_pointers_by_topology
                        .get(&TopologyIndicator::BeginningNode)
                        .with_context(|| FormatViolationSnafu {
                            description: "Line should have a beginning node".to_string(),
                        })?;
                    let end = resolved_pointers_by_topology
                        .get(&TopologyIndicator::EndNode)
                        .with_context(|| FormatViolationSnafu {
                            description: "Line should have an end node".to_string(),
                        })?;
                    vertices.insert(0, beginning.clone().expect_point()?);
                    vertices.push(end.clone().expect_point()?);
                }
                _ => EncError::assert(
                    || resolved_pointers_by_topology.is_empty(),
                    || {
                        format!(
                            "No connected topology expected for Point/Points: {:?}\n{:?}",
                            feature_info, resolved_pointers_by_topology
                        )
                    },
                )?,
            }
            if let Some(Orientation::Reverse) = pointer.orientation {
                data.reverse();
            }
            feature_info
                .spatial_data
                .merge(data, pointer.usage_indicator)?;
        }
        EncError::assert(
            || {
                !feature_info.spatial_data.is_empty()
                    // Some records may only aggregate other records without any spatial data of their own.
                    || !feature_info.unresolved_feature_references.is_empty()
            },
            || {
                // Should be printing the record here, but it's not available anymore.
                format!("Expected a feature with spatial data resolved, but no data was resolved for: {:?}", feature_info)
            },
        )?;
        // TODO: Hard to assert right now.
        // EncError::assert(
        //     || {
        //         feature_info.spatial_data.first().unwrap().
        //     },
        //     || {
        //         format!("For areas the beginning and end node should be the same, but got: {beginning:?} != {end:?}")
        //     },
        // )?;
        Ok(())
    }
}
const CHAIN_NODE_RESOLVER: ChainNodeResolver = ChainNodeResolver;

/*
 *
 * 2.2.1.3 Planar graph
 * A set of nodes and edges. A chain-node set where edges must not cross and may touch only at the connected nodes. Vector objects may be shared, with the restriction that touching edges always share connected nodes and adjacent areas always share the edges forming their common boundary. Duplication of coincident geometry is prohibited. The planar graph model is illustrated in figure 2.4.
 *
 * 2.2.1.4 Full topology
 * A set of nodes, edges and faces. A planar graph with defined faces. The universe is partitioned into a set of mutually exclusive and collectively exhaustive faces. Isolated nodes may reference their containing faces and edges must reference the faces to their right and left. Point representations are coded as nodes (isolated or connected). Line representations are coded as series of edges and connected nodes. Area representations are coded as faces. Duplication of coincident geometry is prohibited. The full topology data model is illustrated in figure 2.5.
 */

/// Simple extraction, without considering pointers.
///
/// When `allow_empty_data = true` Line and Area primitives will return empty vector
/// s when `data = None`.
fn extract_spatial_array(
    primitive: Option<ObjectGeometricPrimitive>,
    data: Option<SpatialArray>,
    allow_empty_data: bool,
) -> Result<SpatialDataSegment> {
    let data = match primitive {
        Some(ObjectGeometricPrimitive::Area) => {
            let mut area_coords = Vec::new();
            match data {
                Some(SpatialArray::Coordinates(coords)) => {
                    area_coords.extend(coords);
                }
                Some(_) => {
                    fail!(FormatViolationSnafu {
                        description: format!(
                            "Expected an area of coordinates, but got data: {:?}",
                            data
                        ),
                    });
                }
                None => {
                    ensure!(
                        allow_empty_data,
                        FormatViolationSnafu {
                            description: "Expected an area of coordinates, but got None"
                                .to_string(),
                        }
                    );
                }
            }
            SpatialDataSegment::Area(area_coords)
        }
        Some(ObjectGeometricPrimitive::Line) => {
            let mut line_coords = Vec::new();
            match data {
                Some(SpatialArray::Coordinates(coords)) => {
                    line_coords.extend(coords);
                }
                Some(_) => {
                    fail!(FormatViolationSnafu {
                        description: format!(
                            "Expected a line of coordinates, but got data: {:?}",
                            data
                        ),
                    });
                }
                None => {
                    ensure!(
                        allow_empty_data,
                        FormatViolationSnafu {
                            description: "Expected a line of coordinates, but got None".to_string(),
                        }
                    );
                }
            }
            SpatialDataSegment::Line(line_coords)
        }
        Some(ObjectGeometricPrimitive::Point) => match data {
            Some(SpatialArray::Coordinates(coords)) => {
                let num_coords = coords.len();
                EncError::assert(
                    || num_coords == 1,
                    || format!("Expected a single point, but got {}", num_coords),
                )?;
                SpatialDataSegment::Point(coords[0])
            }
            Some(SpatialArray::Soundings(soundings)) => SpatialDataSegment::Points(soundings),
            None => {
                fail!(FormatViolationSnafu {
                    description: format!("Expected a point, but got None"),
                });
            }
        },
        None => {
            fail!(FormatViolationSnafu {
                description: "Expected some primitive, but got None".to_string()
            });
        }
    };
    Ok(data)
}

use crate::fail;

use super::*;
use data_fields::*;
use reader::{DdrRecord, DrRecord};
use std::{collections::HashSet, sync::LazyLock};
use topology::resolver_for_data_structure;

mod topology;

const DATA_SET_ID_TAG: &str = "DSID";
const DATA_SET_STRUCTURE_INFO_TAG: &str = "DSSI";
const DATA_SET_PARAMETER_TAG: &str = "DSPM";
const FEATURE_RECORD_ID_TAG: &str = "FRID";
const VECTOR_RECORD_ID_TAG: &str = "VRID";

static METADATA_TAGS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    vec![
        DATA_SET_ID_TAG,
        DATA_SET_STRUCTURE_INFO_TAG,
        DATA_SET_PARAMETER_TAG,
    ]
    .into_iter()
    .collect()
});

pub struct ChartReader {
    generic_reader: Generic8211FileReader,
    lazy_index: Option<RecordIndex>,
}
impl ChartReader {
    pub fn with(generic_reader: Generic8211FileReader) -> Result<Self> {
        EncError::assert(
            || {
                generic_reader
                    .ddr_record
                    .directory
                    .iter()
                    .any(|entry| entry.tag == DATA_SET_ID_TAG)
            },
            || {
                format!(
                    "Expected entries with tag '{DATA_SET_ID_TAG}' but found none in [{}]",
                    generic_reader
                        .ddr_record
                        .directory
                        .iter()
                        .map(|e| &e.tag)
                        .join(", ")
                )
            },
        )?;
        Ok(Self {
            generic_reader,
            lazy_index: None,
        })
    }

    fn index(&mut self) -> Result<&RecordIndex> {
        match self.lazy_index {
            Some(ref index) => Ok(index),
            None => {
                let mut records = Vec::new();
                for record_res in self.generic_reader.data_record_headers()? {
                    records.push(record_res?);
                }
                self.lazy_index = Some(RecordIndex::build_from(
                    &self.generic_reader.ddr_record,
                    records,
                )?);
                Ok(self.lazy_index.as_ref().unwrap())
            }
        }
    }

    /// Return the chart's metadata.
    pub fn metadata(&mut self) -> Result<ChartMetadata> {
        let matching_offsets = self.index()?.offsets_matching(|record| {
            record
                .directory
                .iter()
                .any(|entry| METADATA_TAGS.contains(entry.tag.as_str()))
        });
        let mut identification: Option<DataSetIdentificationField> = None;
        let mut structure: Option<DataSetStructureInformation> = None;
        let mut parameters: Option<DataSetParameterField> = None;
        for record_res in self
            .generic_reader
            .data_records_at_offsets(matching_offsets)?
        {
            let record = record_res?;
            let matching_values = record.take_fields_with_tag_in(&*METADATA_TAGS);
            assert!(!matching_values.is_empty(), "We only selected records with fields in {:?} but we got no records that  match any of them", METADATA_TAGS);

            for matching_value in matching_values {
                match matching_value {
                    DataField::DataSetIdentification(dsid) => {
                        put_if_absent(&mut identification, *dsid)?;
                    }
                    DataField::DataStructure(dssi) => {
                        put_if_absent(&mut structure, *dssi)?;
                    }
                    DataField::DataSetParameter(dspm) => {
                        put_if_absent(&mut parameters, *dspm)?;
                    }
                    value => panic!("We only selected records with fields in {:?} but we got a records that doesn't match any of them: {:?}", METADATA_TAGS, value),
                }
            }
        }
        Ok(ChartMetadata {
            identification: identification.with_context(|| FormatViolationSnafu {
                description: "Chart data must contain DSID field!".to_string(),
            })?,
            structure: structure.with_context(|| FormatViolationSnafu {
                description: "Chart data must contain DSSI field!".to_string(),
            })?,
            parameters: parameters.with_context(|| FormatViolationSnafu {
                description: "Chart data must contain DSPM field!".to_string(),
            })?,
        })
    }

    /// Return all features, but without any pointers resolved.
    ///
    /// That means the [[FeatureInfo::spatial_data]] field is always empty.
    pub fn feature_records_unresolved(
        &mut self,
    ) -> Result<impl Iterator<Item = Result<FeatureInfo>>> {
        let raw_it = self.feature_records_raw()?;
        let it =
            raw_it.map(|res| res.map(|feature_record| FeatureInfo::from_record(&feature_record)));
        Ok(it)
    }

    /// Return all features, with any pointers resolved.
    pub fn feature_records_resolved(
        &mut self,
    ) -> Result<impl Iterator<Item = Result<FeatureInfo>>> {
        let metadata = self.metadata()?;

        let mut spatial_feature_index: HashMap<RecordName, SpatialRecord> = HashMap::new();
        for record_res in self.spatial_records()? {
            let record = record_res?;
            spatial_feature_index.insert(record.name, record);
        }

        let topology_resolver = resolver_for_data_structure(&metadata.structure.data_structure)?;
        let raw_it = self.feature_records_raw()?;
        let it = raw_it.map(move |res| {
            res.and_then(|feature_record| {
                let mut feature_info = FeatureInfo::from_record(&feature_record);
                topology_resolver.resolve_feature(
                    &spatial_feature_index,
                    feature_record,
                    &mut feature_info,
                )?;

                #[cfg(test)]
                {
                    // println!("Feature: {feature_info:#?}");
                    feature_info.spatial_data.validate();
                }

                Ok(feature_info)
            })
        });
        Ok(it)
    }

    fn feature_records_raw(&mut self) -> Result<impl Iterator<Item = Result<FeatureRecord>>> {
        let matching_offsets = self.index()?.offsets_matching(|record| {
            record
                .directory
                .iter()
                .any(|entry| entry.tag == FEATURE_RECORD_ID_TAG)
        });
        self.generic_reader
            .data_records_at_offsets(matching_offsets)
            .map(|iter| {
                iter.map(|record_res| {
                    record_res.and_then(|record| {
                        let mut feature_opt = None;
                        for matching_value in record.fields {
                            match matching_value {
                                DataField::FeatureRecordId(field) => {
                                    feature_opt = Some(FeatureRecord::new(field));
                                }
                                DataField::FeatureObjectId(field) => {
                                    feature_opt.as_mut().unwrap().id = Some(field);
                                }
                                DataField::Array(array) => match array {
                                    DataArray::FeatureAttribute(attrs) => {
                                        feature_opt.as_mut().unwrap().attributes.extend(
                                            attrs.values.into_iter().map(|attr| {
                                                let label = (*LABEL_VALUE_LOOKUP)
                                                    .get(&attr.label)
                                                    .cloned()
                                                    .unwrap_or_default();
                                                Attribute {
                                                    label,
                                                    value: AttributeValue::from_str_and_label(
                                                        attr.value, label,
                                                    )
                                                    .unwrap_or_else(|e| {
                                                        eprint!(
                                                        "Error parsing attribute value ({}): {}",
                                                        label, e
                                                    );
                                                        AttributeValue::Invalid
                                                    }),
                                                }
                                            }),
                                        );
                                    }
                                    DataArray::FeatureToFeatureObjectPointer(pointers) => {
                                        feature_opt
                                            .as_mut()
                                            .unwrap()
                                            .object_pointers
                                            .extend(pointers.values);
                                    }
                                    DataArray::FeatureToSpatialRecordPointer(pointers) => {
                                        feature_opt
                                            .as_mut()
                                            .unwrap()
                                            .spatial_pointers
                                            .extend(pointers.values);
                                    }
                                    _ => (), // Ignore other attributes
                                },
                                _ => (), // Ignore other attributes
                            }
                        }
                        feature_opt.with_context(|| FormatViolationSnafu {
                            description: format!(
                                "Every selected record must contain a {FEATURE_RECORD_ID_TAG}"
                            ),
                        })
                    })
                })
            })
    }

    /// Return all the spatial records in the data set.
    ///
    /// Not connected to features, pointers are unresolved.
    pub fn spatial_records(&mut self) -> Result<impl Iterator<Item = Result<SpatialRecord>>> {
        let metadata = self.metadata()?;
        let matching_offsets = self.index()?.offsets_matching(|record| {
            record
                .directory
                .iter()
                .any(|entry| entry.tag == VECTOR_RECORD_ID_TAG)
        });
        self.generic_reader
            .data_records_at_offsets(matching_offsets)
            .map(move |iter| {
                iter.map(move |res| {
                    res.and_then(|record| {
                        let mut vrid: Option<VectorRecordIdField> = None;
                        let mut attributes: Vec<Attribute> = Vec::new();
                        let mut pointers: Vec<VectorRecordPointer> = Vec::new();
                        let mut data: Option<SpatialArray> = None;
                        for field in record.fields {
                            match field {
                                DataField::VectorRecordId(record) => {
                                    put_if_absent(&mut vrid, record)?;
                                }
                                DataField::Array(DataArray::VectorAttribute(attrs)) => {
                                    attributes.extend(attrs.values.into_iter().map(|attr| {
                                        let label = (*LABEL_VALUE_LOOKUP)
                                            .get(&attr.label)
                                            .cloned()
                                            .unwrap_or_default();
                                        Attribute {
                                            label,
                                            value: AttributeValue::from_str_and_label(
                                                attr.value, label,
                                            )
                                            .unwrap_or_else(|e| {
                                                eprint!(
                                                    "Error parsing attribute value ({}): {}",
                                                    label, e
                                                );
                                                AttributeValue::Invalid
                                            }),
                                        }
                                    }));
                                }
                                DataField::Array(DataArray::Coordinate(coords)) => {
                                    let mapped = metadata.scale_coordinates(&coords.values);
                                    put_if_absent(&mut data, SpatialArray::Coordinates(mapped))?;
                                }
                                DataField::Array(DataArray::Sounding(soundings)) => {
                                    let mapped = metadata.scale_soundings(&soundings.values);
                                    put_if_absent(&mut data, SpatialArray::Soundings(mapped))?;
                                }
                                DataField::Array(DataArray::VectorRecordPointer(ptrs)) => {
                                    pointers.extend(ptrs.values);
                                }
                                _ => (), // Ignore other attributes
                            }
                        }
                        let vrid = vrid.with_context(|| FormatViolationSnafu {
                            description: format!(
                                "Vector record must contain {VECTOR_RECORD_ID_TAG} field!"
                            ),
                        })?;
                        let record = SpatialRecord {
                            name: vrid.unique_name(),
                            record_version: vrid.record_version,
                            record_update_instruction: vrid.record_update_instruction,
                            attributes,
                            pointers,
                            data,
                        };
                        Ok(record)
                    })
                })
            })
    }
}

fn put_if_absent<T>(target: &mut Option<T>, value: T) -> Result<()>
where
    T: fmt::Debug,
{
    ensure!(
        target.is_none(),
        FormatViolationSnafu {
            description: format!(
                "Expected a single value of type {}, but got both {:?} and {:?}",
                std::any::type_name::<T>(),
                target,
                value
            )
        }
    );
    *target = Some(value);
    Ok(())
}

struct RecordIndex {
    record_offsets: Vec<u64>,
    /// The record headers without any fields.
    records: Vec<DrRecord>,
}
impl RecordIndex {
    fn build_from(ddr_record: &DdrRecord, records: Vec<DrRecord>) -> Result<Self> {
        let mut record_offsets = Vec::with_capacity(records.len());
        let mut current_offset = ddr_record.leader.record_length as u64;
        for record in records.iter() {
            record_offsets.push(current_offset);
            current_offset += record.leader.record_length as u64;
        }
        Ok(Self {
            record_offsets,
            records,
        })
    }

    fn offsets_matching(&self, predicate: impl Fn(&DrRecord) -> bool) -> Vec<u64> {
        let positions = self
            .records
            .iter()
            .enumerate()
            .filter(|(_index, record)| predicate(record))
            .map(|(index, _record)| index)
            .collect_vec();
        positions
            .iter()
            .map(|&pos| unsafe { self.record_offsets.get_unchecked(pos).clone() })
            .collect()
    }
}

const FEET_TO_METERS: f64 = 0.3048f64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DepthUnits {
    Meters,
    FathomsAndFeet,
    Feet,
    FathomsAndFractions,
}
impl DepthUnits {
    fn from_u8(data: u8) -> Self {
        match data {
            1 => Self::Meters,
            2 => Self::FathomsAndFeet,
            3 => Self::Feet,
            4 => Self::FathomsAndFractions,
            _ => panic!("Illegal depth value {data}"),
        }
    }

    fn scaling_factor_to_meters(&self, multiplier: u32) -> f64 {
        match self {
            Self::Meters => 1.0 / (multiplier as f64),
            Self::Feet => FEET_TO_METERS / (multiplier as f64),
            _ => unimplemented!("{:?} depth unit is not supported", self),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PositionalAccuracyUnits {
    Meters,
    DegreesOfArc,
    Millimeters,
    Feet,
    Cables,
}
impl PositionalAccuracyUnits {
    fn from_u8(data: u8) -> Self {
        match data {
            1 => Self::Meters,
            2 => Self::DegreesOfArc,
            3 => Self::Millimeters,
            4 => Self::Feet,
            5 => Self::Cables,
            _ => panic!("Illegal coordinate unit value {data}"),
        }
    }
}

#[derive(Debug)]
pub struct ChartMetadata {
    // TODO: Add some accessors that turn this garbage into some intelligble values.
    pub identification: DataSetIdentificationField,
    pub structure: DataSetStructureInformation,
    pub parameters: DataSetParameterField,
}
impl ChartMetadata {
    pub fn depth_units(&self) -> DepthUnits {
        DepthUnits::from_u8(self.parameters.units_of_depth)
    }

    pub fn coordinate_units(&self) -> &CoordinateUnits {
        &self.parameters.coordinate_units
    }

    pub fn positional_units(&self) -> PositionalAccuracyUnits {
        PositionalAccuracyUnits::from_u8(self.parameters.units_of_positional_accuracy)
    }

    pub fn scale_coordinate(&self, coordinate: &Coordinate2D) -> GeoPoint {
        let coordinate_factor = self
            .coordinate_units()
            .scaling_factor_to_degrees(self.parameters.coordinate_multiplication_factor);
        self.scale_coordinate_with_factor(coordinate, coordinate_factor)
    }

    pub fn scale_coordinates(&self, coordinates: &[Coordinate2D]) -> Vec<GeoPoint> {
        let coordinate_factor = self
            .coordinate_units()
            .scaling_factor_to_degrees(self.parameters.coordinate_multiplication_factor);
        coordinates
            .iter()
            .map(|coordinate| self.scale_coordinate_with_factor(coordinate, coordinate_factor))
            .collect()
    }

    fn scale_coordinate_with_factor(&self, coordinate: &Coordinate2D, factor: f64) -> GeoPoint {
        let lat = Degrees::new(coordinate.y_coordinate as f64 * factor);
        let lon = Degrees::new(coordinate.x_coordinate as f64 * factor);
        GeoPoint { lat, lon }
    }

    pub fn scale_sounding(&self, sounding: &Sounding3D) -> Sounding {
        let location = self.scale_coordinate(&Coordinate2D {
            x_coordinate: sounding.x_coordinate,
            y_coordinate: sounding.y_coordinate,
        });
        let depth_factor = self
            .depth_units()
            .scaling_factor_to_meters(self.parameters.sounding_multiplication_factor);
        let depth = Meter::new(sounding.sounding_value as f64 * depth_factor);
        Sounding { location, depth }
    }

    pub fn scale_soundings(&self, soundings: &[Sounding3D]) -> Vec<Sounding> {
        let depth_factor = self
            .depth_units()
            .scaling_factor_to_meters(self.parameters.sounding_multiplication_factor);
        let coordinate_factor = self
            .coordinate_units()
            .scaling_factor_to_degrees(self.parameters.coordinate_multiplication_factor);
        soundings
            .iter()
            .map(|sounding| {
                let location = self.scale_coordinate_with_factor(
                    &Coordinate2D {
                        x_coordinate: sounding.x_coordinate,
                        y_coordinate: sounding.y_coordinate,
                    },
                    coordinate_factor,
                );
                let depth = Meter::new(sounding.sounding_value as f64 * depth_factor);
                Sounding { location, depth }
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum AttributeValue {
    Lookup { values: Vec<&'static str> },
    Float(f64),
    Integer(i64),
    Text(String),
    Invalid,
}
impl AttributeValue {
    fn from_str_and_label(value: String, label: AttributeLabel) -> Result<Self> {
        let value = match label.attr_type {
            AttributeType::Enumerated => {
                let resolved = attribute_value_lookup(label.acronym, &value)?;
                AttributeValue::Lookup {
                    values: vec![resolved],
                }
            }
            AttributeType::List => {
                let mut values = Vec::new();
                for part in value.split(",") {
                    let resolved = attribute_value_lookup(label.acronym, part)?;
                    values.push(resolved)
                }
                AttributeValue::Lookup { values }
            }
            AttributeType::Float => {
                let num: f64 = value.parse().with_whatever_context::<_, _, EncError>(|_| {
                    format!("Could not parse {value} to f64.")
                })?;
                AttributeValue::Float(num)
            }
            AttributeType::Integer => {
                let num: i64 = value.parse().with_whatever_context::<_, _, EncError>(|_| {
                    format!("Could not parse {value} to i64.")
                })?;
                AttributeValue::Integer(num)
            }
            _ => AttributeValue::Text(value),
        };
        Ok(value)
    }
}
impl fmt::Display for AttributeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lookup { values } => write!(f, "{}", values.join(", ")),
            Self::Float(r) => write!(f, "{r}"),
            Self::Integer(i) => write!(f, "{i}"),
            Self::Text(s) => write!(f, "{s}"),
            Self::Invalid => write!(f, "<INVALID>"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub label: AttributeLabel,
    pub value: AttributeValue,
}
impl fmt::Display for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.label, self.value)
    }
}

#[derive(Debug, Clone)]
pub struct FeatureRecord {
    field: FeatureRecordIdField,
    id: Option<FeatureObjectIdField>,
    attributes: Vec<Attribute>,
    object_pointers: Vec<FeatureRecordToFeatureObjectPointer>,
    spatial_pointers: Vec<FeatureRecordToSpatialRecordPointer>,
}
impl FeatureRecord {
    fn new(field: FeatureRecordIdField) -> Self {
        Self {
            field,
            id: None,
            attributes: Default::default(),
            object_pointers: Default::default(),
            spatial_pointers: Default::default(),
        }
    }
}
impl fmt::Display for FeatureRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Feature {{\n name: {}\n id: {}\n attributes: [ \n{} \n ]\n {} object pointers and {} spatial pointers\n}}",
            self.field.unique_name(),
            self.id.as_ref().map(|id| format!("{id}")).unwrap_or("None".to_string()),
            self.attributes.iter().map(|a| format!("  {a}")).join("\n"),
            self.object_pointers.len(),
            self.spatial_pointers.len())
    }
}

/// This is a trimmed down version of [[FeatureRecord]] without the spatial pointers.
#[derive(Debug, Clone)]
pub struct FeatureInfo {
    pub name: RecordName,
    pub long_name: Option<LongRecordName>,
    pub group: u8,
    pub object_label: &'static ObjectClass,
    pub record_version: u16,
    pub record_update_instruction: UpdateInstruction,
    pub attributes: Vec<Attribute>,
    pub unresolved_feature_references: Vec<FeatureRecordToFeatureObjectPointer>,
    pub spatial_data: SpatialData,
}
impl FeatureInfo {
    fn from_record(record: &FeatureRecord) -> Self {
        let object_label = (*OBJECT_LOOKUP)
            .get(&record.field.object_label)
            .unwrap_or_default();
        Self {
            name: record.field.unique_name(),
            long_name: record.id.clone(),
            group: record.field.group,
            object_label,
            record_version: record.field.record_version,
            record_update_instruction: record.field.record_update_instruction,
            attributes: record.attributes.clone(),
            unresolved_feature_references: record.object_pointers.clone(),
            spatial_data: SpatialData::None, // Needs to be filled in after resolving pointers.
        }
    }
}
impl fmt::Display for FeatureInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Feature {{\n name: {}\n id: {}\n class: {}\n attributes: [ \n{} \n ]\n unresolved references: [ \n{} \n ]\n {}\n}}",
            self.name,
            self.long_name
                .as_ref()
                .map(|id| format!("{id}"))
                .unwrap_or("None".to_string()),
            self.object_label,
            self.attributes.iter().map(|a| format!("  {a}")).join("\n"),
            self.unresolved_feature_references.iter().map(|r| format!("  {r}")).join("\n"),
            self.spatial_data
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SpatialArray {
    Coordinates(Vec<GeoPoint>),
    Soundings(Vec<Sounding>),
}
impl SpatialArray {
    pub fn expect_point(self) -> Result<GeoPoint> {
        match self {
            SpatialArray::Coordinates(mut points) => {
                ensure!(
                    points.len() == 1,
                    FormatViolationSnafu {
                        description: format!("Expected a single point but got {}", points.len())
                    }
                );
                Ok(points.pop().unwrap())
            }
            SpatialArray::Soundings(_) => FormatViolationSnafu {
                description: "Expected a single point but got soundings".to_string(),
            }
            .fail(),
        }
    }
}
impl fmt::Display for SpatialArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Coordinates(points) => write!(
                f,
                "[{}]",
                points.iter().map(|point| point.to_string()).join(" -> ")
            ),
            Self::Soundings(soundings) => write!(
                f,
                "[{}]",
                soundings
                    .iter()
                    .map(|sounding| sounding.to_string())
                    .join(" -> ")
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpatialRecord {
    pub name: RecordName,
    pub record_version: u16,
    pub record_update_instruction: UpdateInstruction,
    pub attributes: Vec<Attribute>,
    pub pointers: Vec<VectorRecordPointer>,
    pub data: Option<SpatialArray>,
}
impl SpatialRecord {
    pub fn expect_point(self) -> Result<GeoPoint> {
        self.data
            .with_context(|| FormatViolationSnafu {
                description: "Expected a single point but got None".to_string(),
            })
            .and_then(SpatialArray::expect_point)
    }
}
impl fmt::Display for SpatialRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Spatial {{\n name: {}\n version: {}\n instruction: {}\n attributes: [ \n{} \n ]\n pointers: [{}]\n data: {}\n}}", 
            self.name,
            self.record_version,
            self.record_update_instruction,
            self.attributes.iter().map(|a| format!("  {a}")).join("\n"),
            self.pointers
                    .iter()
                    .map(|pointer| pointer.name.to_string())
                    .join(", "),
            self.data.as_ref().map(|d| d.to_string()).unwrap_or_else(|| "None".to_string()))
    }
}

/// A representation of a spatial feature.
#[derive(Debug, Clone)]
pub enum SpatialData {
    /// An area defined by a list of vertices representing the `outer_boundary` made up
    /// of straight line segments closing on itself.
    /// It may also provide a list of `inner_boundaries`, made up themselves of vertices.
    Area {
        outer_boundary: Vec<GeoPoint>,
        inner_boundaries: Vec<Vec<GeoPoint>>,
    },
    /// A line defined by a list of `vertices` that connect into straight line segments.
    Line { vertices: Vec<GeoPoint> },
    /// A single point given by a `location`.
    Point { location: GeoPoint },
    /// A collection of soundings.
    Points { soundings: Vec<Sounding> },
    /// No spatial data for this feature.
    None,
}
impl SpatialData {
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::None)
    }

    #[cfg(test)]
    fn validate(&self) {
        match self {
            Self::Area {
                outer_boundary,
                inner_boundaries,
            } => {
                // Very inefficient, but good enough for debug use.
                for (pos1, vertex1) in outer_boundary.iter().enumerate() {
                    for (pos2, vertex2) in outer_boundary.iter().enumerate() {
                        if pos1 == pos2
                            || (pos1 == 0 && pos2 == outer_boundary.len() - 1)
                            || (pos2 == 0 && pos1 == outer_boundary.len() - 1)
                        {
                            // TODO: Areas are closed, to the first and last vertex should be the same.
                            //       Might want to revisit that when unifying.
                            assert_eq!(
                                vertex1, vertex2,
                                "Expected the same vertices in area at ({pos1}, {pos2}): {self:#?}"
                            )
                        } else {
                            assert_ne!(
                                vertex1, vertex2,
                                "Duplicate vertex in area at ({pos1}, {pos2}): {self:#?}"
                            );
                        }
                    }
                }
                for inner_boundary in inner_boundaries.iter() {
                    for (pos1, vertex1) in inner_boundary.iter().enumerate() {
                        for (pos2, vertex2) in inner_boundary.iter().enumerate() {
                            if pos1 == pos2
                                || (pos1 == 0 && pos2 == inner_boundary.len() - 1)
                                || (pos2 == 0 && pos1 == inner_boundary.len() - 1)
                            {
                                // TODO: Areas are closed, to the first and last vertex should be the same.
                                //       Might want to revisit that when unifying.
                                assert_eq!(
                                    vertex1, vertex2,
                                    "Expected the same vertices in area at ({pos1}, {pos2}): {self:#?}"
                                );
                            } else {
                                assert_ne!(
                                    vertex1, vertex2,
                                    "Duplicate vertex in area at ({pos1}, {pos2}): {self:#?}"
                                );
                            }
                        }
                    }
                }
            }
            Self::Line { vertices } => {
                // Very inefficient, but good enough for debug use.
                for (pos1, vertex1) in vertices.iter().enumerate() {
                    for (pos2, vertex2) in vertices.iter().enumerate() {
                        if pos1 == pos2 {
                            assert_eq!(
                                vertex1, vertex2,
                                "Expected the same vertices in area at ({pos1}, {pos2}): {self:#?}"
                            )
                        } else if (pos1 == 0 && pos2 == vertices.len() - 1)
                            || (pos2 == 0 && pos1 == vertices.len() - 1)
                        {
                            // Some lines may be closed without describing an area, so this case is ok.
                        } else {
                            assert_ne!(
                                vertex1, vertex2,
                                "Duplicate vertex in area at ({pos1}, {pos2}): {self:#?}"
                            );
                        }
                    }
                }
            }
            Self::Points { soundings } => {
                // Very inefficient, but good enough for debug use.
                for (pos1, vertex1) in soundings.iter().enumerate() {
                    for (pos2, vertex2) in soundings.iter().enumerate() {
                        if pos1 != pos2 {
                            assert_ne!(
                                vertex1, vertex2,
                                "Duplicate sounding in points at ({pos1}, {pos2}): {self:#?}"
                            );
                        }
                    }
                }
            }
            Self::Point { .. } | Self::None => (), // Can't think of a way to mess this up
        }
    }

    fn merge(&mut self, segment: SpatialDataSegment, usage: Option<UsageIndicator>) -> Result<()> {
        match self {
            Self::Area {
                outer_boundary,
                inner_boundaries,
            } => {
                if let SpatialDataSegment::Area(vertices) = segment {
                    match usage {
                        Some(UsageIndicator::Exterior)
                        | Some(UsageIndicator::ExteriorTruncated)
                        | None => {
                            if vertices.first() == outer_boundary.last() {
                                // Cheaper to pop than to shift in vertices.
                                outer_boundary.pop();
                            }
                            outer_boundary.extend(vertices);
                        }
                        Some(UsageIndicator::Interior) => {
                            if let Some(current) = inner_boundaries.last_mut() {
                                if current.first() != current.last() {
                                    // Boundary is incomplete, keep pushing to it.
                                    if vertices.first() == current.last() {
                                        // Cheaper to pop than to shift in vertices.
                                        current.pop();
                                    }
                                    current.extend(vertices);
                                } else {
                                    // Start an new one.
                                    inner_boundaries.push(vertices);
                                }
                            } else {
                                // Start an new one.
                                inner_boundaries.push(vertices);
                            }
                        }
                    };
                } else {
                    fail!(FormatViolationSnafu {
                        description: format!("Expected an area segment, but got: {segment:?}")
                    });
                }
            }
            Self::Line { vertices } => {
                ensure!(
                    usage.is_none(),
                    FormatViolationSnafu {
                        description: format!("Usage is not supported for Line data: {usage:?}")
                    }
                );
                if let SpatialDataSegment::Line(new_vertices) = segment {
                    if new_vertices.first() == vertices.last() {
                        // Cheaper to pop than to shift in new_vertices.
                        vertices.pop();
                    }
                    vertices.extend(new_vertices);
                } else {
                    fail!(FormatViolationSnafu {
                        description: format!("Expected a line segment, but got: {segment:?}")
                    });
                }
            }
            Self::Points { soundings } => {
                ensure!(
                    usage.is_none(),
                    FormatViolationSnafu {
                        description: format!("Usage is not supported for Points data: {usage:?}")
                    }
                );
                if let SpatialDataSegment::Points(new_soundings) = segment {
                    soundings.extend(new_soundings);
                } else {
                    fail!(FormatViolationSnafu {
                        description: format!(
                            "Expected a segment of soundings, but got: {segment:?}"
                        )
                    });
                }
            }
            Self::Point { .. } => {
                fail!(FormatViolationSnafu {
                    description: format!("Point cannot be extended. Got: {segment:?}")
                });
            }
            Self::None => match segment {
                SpatialDataSegment::Area(vertices) => {
                    *self = match usage {
                        Some(UsageIndicator::Exterior)
                        | Some(UsageIndicator::ExteriorTruncated)
                        | None => SpatialData::Area {
                            outer_boundary: vertices,
                            inner_boundaries: Vec::new(),
                        },
                        Some(UsageIndicator::Interior) => SpatialData::Area {
                            outer_boundary: Vec::new(),
                            inner_boundaries: vec![vertices],
                        },
                    };
                }
                SpatialDataSegment::Line(vertices) => *self = Self::Line { vertices },
                SpatialDataSegment::Points(soundings) => {
                    *self = Self::Points { soundings };
                }
                SpatialDataSegment::Point(location) => {
                    *self = Self::Point { location };
                }
            },
        }
        Ok(())
    }
}
impl fmt::Display for SpatialData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Area {
                outer_boundary,
                inner_boundaries,
            } => {
                write!(
                    f,
                    "Area with {} vertices in outer boundary and {} inner boundaries with a total of {} vertices",
                    outer_boundary.len(),
                    inner_boundaries.len(),
                    inner_boundaries.iter().map(|b| b.len()).sum::<usize>()
                )
            }
            Self::Line { vertices } => write!(f, "Line with {} vertices", vertices.len()),
            Self::Point { location } => write!(f, "Point at {}", location),
            Self::Points { soundings } => write!(f, "Points with {} soundings", soundings.len()),
            Self::None => write!(f, "None"),
        }
    }
}

#[derive(Debug, Clone)]
enum SpatialDataSegment {
    Area(Vec<GeoPoint>),
    Line(Vec<GeoPoint>),
    Point(GeoPoint),
    Points(Vec<Sounding>),
}
impl SpatialDataSegment {
    fn reverse(&mut self) {
        match self {
            Self::Area(ref mut vertices) => vertices.reverse(),
            Self::Line(ref mut vertices) => vertices.reverse(),
            Self::Points(ref mut vertices) => vertices.reverse(),
            Self::Point(_) => (), // Nothing to do.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_DIRECTORY_FILE: &str = "/Users/lkroll/Programming/Sailing/test-data/20240816_U7Inland_Waddenzee_week 33_NL/ENC_ROOT/catalog.031";
    const TEST_CHART_FILE: &str = "/Users/lkroll/Programming/Sailing/test-data/20240816_U7Inland_Waddenzee_week 33_NL/ENC_ROOT/1R/7/1R7EMS01/1R7EMS01.000";

    #[test]
    fn read_chart() {
        println!("Path: {TEST_CHART_FILE}");
        let generic_reader = Generic8211FileReader::open(TEST_CHART_FILE).unwrap();
        let mut chart_reader = ChartReader::with(generic_reader).unwrap();
        let metadata = chart_reader.metadata().unwrap();
        println!("Metadata: {metadata:#?}");
        println!("Depth Unit: {:?}", metadata.depth_units());
        println!("Coordinate Unit: {:?}", metadata.coordinate_units());
        // println!("Features:");
        // for feature_res in chart_reader.feature_records_unresolved().unwrap() {
        //     match feature_res {
        //         Ok(feature) => println!("  {feature}"),
        //         Err(e) => {
        //             eprintln!("{e}\nBacktrace:\n{}", e.backtrace().unwrap());
        //             assert!(false, "Test failed");
        //         }
        //     }
        // }
        // println!("Spatial Records:");
        // for record_res in chart_reader.spatial_records().unwrap() {
        //     match record_res {
        //         Ok(record) => println!("  {record}"),
        //         Err(e) => {
        //             eprintln!("{e}\nBacktrace:\n{}", e.backtrace().unwrap());
        //             assert!(false, "Test failed");
        //         }
        //     }
        // }
        // println!("Resolved Records:");
        // for record_res in chart_reader.feature_records_resolved().unwrap() {
        //     match record_res {
        //         Ok(record) => println!("  {record}"),
        //         Err(e) => {
        //             eprintln!("{e}\nBacktrace:\n{}", e.backtrace().unwrap());
        //             assert!(false, "Test failed");
        //         }
        //     }
        // }
        let expected_feature_records = 5431;
        let expected_spatial_records = 14171;
        assert_eq!(
            chart_reader
                .feature_records_unresolved()
                .unwrap()
                .map(Result::unwrap)
                .count(),
            expected_feature_records
        );
        assert_eq!(
            chart_reader
                .feature_records_resolved()
                .unwrap()
                .map(Result::unwrap)
                .count(),
            expected_feature_records
        );
        assert_eq!(
            chart_reader
                .spatial_records()
                .unwrap()
                .map(Result::unwrap)
                .count(),
            expected_spatial_records
        );

        // let objects: HashSet<&ObjectClass> = chart_reader
        //     .feature_records_resolved()
        //     .unwrap()
        //     .map(Result::unwrap)
        //     .map(|feature| feature.object_label)
        //     .collect();
        // println!("All unique objects: {objects:#?}");
    }

    #[test]
    fn fail_on_wrong_file_type() {
        let generic_reader = Generic8211FileReader::open(TEST_DIRECTORY_FILE).unwrap();
        let chart_reader_res = ChartReader::with(generic_reader);
        assert!(
            chart_reader_res.is_err(),
            "Did not fail to produce a reader"
        );
    }
}

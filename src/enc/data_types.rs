use super::*;
use byteorder::{ReadBytesExt, LE};

struct RecordNameLookupEntry {
    binary_id: u8,
    ascii_code: [char; 2],
    name: &'static str,
}

const RECORD_NAMES: [RecordNameLookupEntry; 14] = [
    RecordNameLookupEntry {
        binary_id: 10,
        ascii_code: ['D', 'S'],
        name: "Data Set General Information",
    },
    RecordNameLookupEntry {
        binary_id: 20,
        ascii_code: ['D', 'P'],
        name: "Data Set Geographic Reference",
    },
    RecordNameLookupEntry {
        binary_id: 30,
        ascii_code: ['D', 'H'],
        name: "Data Set History",
    },
    RecordNameLookupEntry {
        binary_id: 40,
        ascii_code: ['D', 'A'],
        name: "Data Set Accuracy",
    },
    RecordNameLookupEntry {
        binary_id: 50, // This is actually not allowed, but happens to be free.
        ascii_code: ['C', 'D'],
        name: "Catalogue Directory",
    },
    RecordNameLookupEntry {
        binary_id: 60,
        ascii_code: ['C', 'R'],
        name: "Catalogue Cross Reference",
    },
    RecordNameLookupEntry {
        binary_id: 70,
        ascii_code: ['I', 'D'],
        name: "Data Dictionary Definition",
    },
    RecordNameLookupEntry {
        binary_id: 80,
        ascii_code: ['I', 'O'],
        name: "Data Dictionary Domain",
    },
    RecordNameLookupEntry {
        binary_id: 90,
        ascii_code: ['I', 'S'],
        name: "Data Dictionary Schema",
    },
    RecordNameLookupEntry {
        binary_id: 100,
        ascii_code: ['F', 'E'],
        name: "Feature",
    },
    RecordNameLookupEntry {
        binary_id: 110,
        ascii_code: ['V', 'I'],
        name: "Vector Isolated Node",
    },
    RecordNameLookupEntry {
        binary_id: 120,
        ascii_code: ['V', 'C'],
        name: "Vector Connected Node",
    },
    RecordNameLookupEntry {
        binary_id: 130,
        ascii_code: ['V', 'E'],
        name: "Vector Edge",
    },
    RecordNameLookupEntry {
        binary_id: 140,
        ascii_code: ['V', 'F'],
        name: "Vector Face",
    },
];

static RECORD_NAME_LOOKUP_BY_ID: LazyLock<BTreeMap<u8, &'static RecordNameLookupEntry>> =
    LazyLock::new(|| {
        RECORD_NAMES
            .iter()
            .map(|entry| (entry.binary_id, entry))
            .collect()
    });

/// A 5 byte unique "NAME" for a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordName {
    pub name: u8,
    pub identifier: u32,
}
impl RecordName {
    /// The 2 character ASCII representation of a record's "name" field.
    pub fn ascii_name(&self) -> &'static [char; 2] {
        &(*RECORD_NAME_LOOKUP_BY_ID)
            .get(&self.name)
            .expect("record name should exist")
            .ascii_code
    }

    /// The fully written out version of a record's "name" field.
    pub fn full_name(&self) -> &'static str {
        &(*RECORD_NAME_LOOKUP_BY_ID)
            .get(&self.name)
            .expect("record name should exist")
            .name
    }
}
impl FromFixedLengthBytes for RecordName {
    const LENGTH: usize = 5;

    fn from_bytes_unchecked(data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let name = Self {
            name: data[0],
            identifier: u32::from_le_bytes(data[1..].try_into().context(SlicingSnafu)?),
        };
        Ok(name)
    }
}
impl fmt::Display for RecordName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ascii_name = self.ascii_name();
        write!(f, "{}{}{}", ascii_name[0], ascii_name[1], self.identifier)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum S57Implementation {
    /// File is a S-57 ASCII implementation.
    Ascii,
    /// File is a S-57 binary implementation.
    Binary,
    Other([char; 3]),
}
impl FromFixedLengthBytes for S57Implementation {
    const LENGTH: usize = 3;

    fn from_bytes_unchecked(data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        match data {
            b"ASC" => Ok(Self::Ascii),
            b"BIN" => Ok(Self::Binary),
            _ => {
                let array: [u8; 3] = data.try_into().context(SlicingSnafu)?;
                Ok(Self::Other(array.map(char::from)))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ProductSpecification {
    /// ENC
    ElectronicNavigationalChart,
    /// ODD
    ObjectCatalogueDataDictionary,
    UnknownBytes(u8),
    UnknownStr(String),
}
impl FromBytes for ProductSpecification {
    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        match reader.read_u8()? {
            // Hard to find a reference for this, tbh. The standard says 1, but the example I got has 10.
            10 => Ok(Self::ElectronicNavigationalChart),
            2 => Ok(Self::ObjectCatalogueDataDictionary),
            b => Ok(Self::UnknownBytes(b)),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ApplicationProfileId {
    /// EN - ENC New
    EncNew,
    /// ER - ENC Revision
    EncRevision,
    /// DD - IHO Data dictionary
    DataDictionary,
    UnknownBytes(u8),
    UnknownStr(String),
}
impl FromBytes for ApplicationProfileId {
    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        match reader.read_u8()? {
            1 => Ok(Self::EncNew),
            2 => Ok(Self::EncRevision),
            3 => Ok(Self::DataDictionary),
            b => Ok(Self::UnknownBytes(b)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DataStructure {
    /// CS - Cartographic sphaghetti
    CartographicSpaghetti,
    /// CN - Chain-node
    ChainNode,
    /// PG - Planar graph
    PlanarGraph,
    /// FT - Full topology
    FullTopology,
    /// NO - Topology is not relevant
    Irrelevant,
    UnknownBytes(u8),
    UnknownStr(String),
}
impl FromBytes for DataStructure {
    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        match reader.read_u8()? {
            1 => Ok(Self::CartographicSpaghetti),
            2 => Ok(Self::ChainNode),
            3 => Ok(Self::PlanarGraph),
            4 => Ok(Self::FullTopology),
            255 => Ok(Self::Irrelevant),
            b => Ok(Self::UnknownBytes(b)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CoordinateUnits {
    /// LL - Latitude/Longitude
    LatitudeLongitude,
    /// EN - Easting/Northing
    EastingNorthing,
    /// UC - Units on the chart
    UnitsOnTheChart,
    UnknownBytes(u8),
    UnknownStr(String),
}
impl CoordinateUnits {
    pub(super) fn scaling_factor_to_degrees(&self, multiplier: u32) -> f64 {
        match self {
            // This is always specified in degrees.
            Self::LatitudeLongitude => 1.0 / (multiplier as f64),
            _ => unimplemented!("{:?} coordinate unit is not supported", self),
        }
    }
}
impl FromBytes for CoordinateUnits {
    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        match reader.read_u8()? {
            1 => Ok(Self::LatitudeLongitude),
            2 => Ok(Self::EastingNorthing),
            3 => Ok(Self::UnitsOnTheChart),
            b => Ok(Self::UnknownBytes(b)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UpdateInstruction {
    Insert,
    Delete,
    Modify,
}
impl FromBytes for UpdateInstruction {
    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        match reader.read_u8()? {
            1 => Ok(Self::Insert),
            2 => Ok(Self::Delete),
            3 => Ok(Self::Modify),
            b => FormatViolationSnafu {
                description: format!("Expected update instruction in {{1, 2, 3}} but got {b}"),
            }
            .fail(),
        }
    }
}
impl fmt::Display for UpdateInstruction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insert => write!(f, "INSERT"),
            Self::Delete => write!(f, "DELETE"),
            Self::Modify => write!(f, "MODIFY"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Orientation {
    Forward,
    Reverse,
}
impl FromByteOpt for Orientation {
    fn from_byte(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Forward),
            2 => Ok(Self::Reverse),
            x => FormatViolationSnafu {
                description: format!("Invalid value {x} for {}", std::any::type_name::<Self>()),
            }
            .fail(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UsageIndicator {
    Exterior,
    Interior,
    /// Exterior boundary truncated by the data limit.
    ExteriorTruncated,
}
impl FromByteOpt for UsageIndicator {
    fn from_byte(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Exterior),
            2 => Ok(Self::Interior),
            3 => Ok(Self::ExteriorTruncated),
            x => FormatViolationSnafu {
                description: format!("Invalid value {x} for {}", std::any::type_name::<Self>()),
            }
            .fail(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TopologyIndicator {
    BeginningNode,
    EndNode,
    LeftFace,
    RightFace,
    ContainingFace,
}
impl FromByteOpt for TopologyIndicator {
    fn from_byte(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::BeginningNode),
            2 => Ok(Self::EndNode),
            3 => Ok(Self::LeftFace),
            4 => Ok(Self::RightFace),
            5 => Ok(Self::ContainingFace),
            x => FormatViolationSnafu {
                description: format!("Invalid value {x} for {}", std::any::type_name::<Self>()),
            }
            .fail(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MaskingIndicator {
    Mask,
    Show,
}
impl FromByteOpt for MaskingIndicator {
    fn from_byte(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Mask),
            2 => Ok(Self::Show),
            x => FormatViolationSnafu {
                description: format!("Invalid value {x} for {}", std::any::type_name::<Self>()),
            }
            .fail(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RelationshipIndicator {
    Master,
    Slave,
    Peer,
    Other(u8),
}
impl fmt::Display for RelationshipIndicator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Master => write!(f, "Master"),
            Self::Slave => write!(f, "Slave"),
            Self::Peer => write!(f, "Peer"),
            Self::Other(_) => write!(f, "?"),
        }
    }
}
impl FromBytes for RelationshipIndicator {
    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        match reader.read_u8()? {
            1 => Ok(Self::Master),
            2 => Ok(Self::Slave),
            3 => Ok(Self::Peer),
            b => Ok(Self::Other(b)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ObjectGeometricPrimitive {
    Point,
    Line,
    Area,
}
impl FromByteOpt for ObjectGeometricPrimitive {
    fn from_byte(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Point),
            2 => Ok(Self::Line),
            3 => Ok(Self::Area),
            x => FormatViolationSnafu {
                description: format!("Invalid value {x} for {}", std::any::type_name::<Self>()),
            }
            .fail(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LongRecordName {
    pub producing_agency: u16,
    pub feature_identification_number: u32,
    pub feature_identification_subdivision: u16,
}
impl FromFixedLengthBytes for LongRecordName {
    const LENGTH: usize = 8;

    fn from_bytes_unchecked(mut data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let name = Self {
            producing_agency: data.read_u16::<LE>()?,
            feature_identification_number: data.read_u32::<LE>()?,
            feature_identification_subdivision: data.read_u16::<LE>()?,
        };
        Ok(name)
    }
}
impl fmt::Display for LongRecordName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}.{}",
            self.producing_agency,
            self.feature_identification_number,
            self.feature_identification_subdivision
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AttributeLabelDomain {
    General,
    National,
}

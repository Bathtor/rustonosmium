use super::*;
use byteorder::{ReadBytesExt, LE};
use data_types::*;
use reader::{DataDescriptiveField, DirectoryEntry};
use std::marker::PhantomData;

/// Records can be identified by a 5 byte unique "NAME" within each file/exchange set.
pub trait Record {
    fn unique_name(&self) -> RecordName;
}

#[derive(Clone, Debug)]
pub enum DataField {
    CatalogDirectory(Box<CatalogDirectoryField>),
    RecordIdentifier { id: usize },
    DataSetIdentification(Box<DataSetIdentificationField>),
    DataStructure(Box<DataSetStructureInformation>),
    DataSetParameter(Box<DataSetParameterField>),
    VectorRecordId(VectorRecordIdField),   // No Box, just 8 bytes
    FeatureRecordId(FeatureRecordIdField), // No Box, just 12 bytes
    FeatureObjectId(FeatureObjectIdField), // No Box, just 8 bytes.
    Array(DataArray),
    UnsupportedString(String),
    UnsupportedBytes(Box<[u8]>),
}

#[derive(Clone, Debug)]
pub enum DataArray {
    Sounding(ValueArray<Sounding3D>),
    Coordinate(ValueArray<Coordinate2D>),
    VectorRecordPointer(ValueArray<VectorRecordPointer>),
    FeatureToSpatialRecordPointer(ValueArray<FeatureRecordToSpatialRecordPointer>),
    FeatureToFeatureObjectPointer(ValueArray<FeatureRecordToFeatureObjectPointer>),
    FeatureAttribute(ValueArray<FeatureRecordAttribute>),
    VectorAttribute(ValueArray<VectorRecordAttribute>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum FieldEncoding {
    Binary,
    Ascii,
}

#[derive(Debug)]
pub(super) struct DataFieldReaderProducer {
    lexical_level: LexicalLevel,
    reader_type: DataFieldReaderType,
}
impl DataFieldReaderProducer {
    pub(super) fn for_data_descriptor(
        entry: &DirectoryEntry,
        descriptor: &DataDescriptiveField,
    ) -> Result<Self> {
        let lexical_level = LexicalLevel::for_character_set_indicator(
            descriptor.field_controls.truncated_escape_sequence,
        )?;
        let reader_type = DataFieldReaderType::for_data_descriptor(entry, descriptor);
        Ok(Self {
            lexical_level,
            reader_type,
        })
    }

    pub(super) fn read_with_ctx<R>(
        &self,
        ctx: DecodingContext,
        entry: &DirectoryEntry,
        reader: &mut R,
    ) -> Result<DataField>
    where
        R: BufRead + Seek,
    {
        let ctx_with_lexical_level = ctx.with_lexical_level(self.lexical_level);
        self.reader_type
            .read_with_ctx(ctx_with_lexical_level, entry, reader)
    }
}

#[derive(Debug)]
pub(super) enum DataFieldReaderType {
    CatalogDirectoryField,
    RecordIdentifier(FieldEncoding),
    DataSetIdentification,
    DataSetStructureInformation,
    DataSetParameter,
    VectorRecordId,
    FeatureRecordId,
    FeatureObjectId,
    Sounding3DArray,
    Coordinate2DArray,
    VectorRecordPointerArray,
    FeatureToSpatialRecordPointerArray,
    FeatureToObjectPointerArray,
    FeatureRecordAttributeArray(AttributeLabelDomain),
    VectorRecordAttributeArray,
    Fallback,
}
impl DataFieldReaderType {
    fn for_data_descriptor(entry: &DirectoryEntry, descriptor: &DataDescriptiveField) -> Self {
        match entry.tag.as_str() {
            CatalogDirectoryFieldReader::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    CatalogDirectoryFieldReader::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    CatalogDirectoryFieldReader::DESCRIPTOR
                );
                Self::CatalogDirectoryField
            }
            RecordIdentifierAsciiReader::TAG => match descriptor.format_controls.as_str() {
                RecordIdentifierAsciiReader::FORMAT => {
                    assert_eq!(
                        descriptor.array_descriptor,
                        RecordIdentifierAsciiReader::DESCRIPTOR
                    );
                    Self::RecordIdentifier(FieldEncoding::Ascii)
                }
                RecordIdentifierBinaryReader::FORMAT => {
                    assert_eq!(
                        descriptor.array_descriptor,
                        RecordIdentifierBinaryReader::DESCRIPTOR
                    );
                    Self::RecordIdentifier(FieldEncoding::Binary)
                }
                f => unimplemented!("Unknown format '{}' for tag={}", f, entry.tag),
            },
            DataSetIdentificationFieldBinaryReader::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    DataSetIdentificationFieldBinaryReader::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    DataSetIdentificationFieldBinaryReader::DESCRIPTOR
                );
                Self::DataSetIdentification
            }
            DataSetStructureInformationBinaryReader::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    DataSetStructureInformationBinaryReader::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    DataSetStructureInformationBinaryReader::DESCRIPTOR
                );
                Self::DataSetStructureInformation
            }
            DataSetParameterFieldBinaryReader::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    DataSetParameterFieldBinaryReader::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    DataSetParameterFieldBinaryReader::DESCRIPTOR
                );
                Self::DataSetParameter
            }
            VectorRecordIdFieldBinaryReader::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    VectorRecordIdFieldBinaryReader::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    VectorRecordIdFieldBinaryReader::DESCRIPTOR
                );
                Self::VectorRecordId
            }
            FeatureRecordIdBinaryFieldReader::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    FeatureRecordIdBinaryFieldReader::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    FeatureRecordIdBinaryFieldReader::DESCRIPTOR
                );
                Self::FeatureRecordId
            }
            FeatureObjectIdFieldBinaryReader::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    FeatureObjectIdFieldBinaryReader::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    FeatureObjectIdFieldBinaryReader::DESCRIPTOR
                );
                Self::FeatureObjectId
            }
            ValueArrayFixedLengthBinaryReader::<Sounding3D>::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    ValueArrayFixedLengthBinaryReader::<Sounding3D>::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    ValueArrayFixedLengthBinaryReader::<Sounding3D>::DESCRIPTOR
                );
                Self::Sounding3DArray
            }
            ValueArrayFixedLengthBinaryReader::<Coordinate2D>::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    ValueArrayFixedLengthBinaryReader::<Coordinate2D>::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    ValueArrayFixedLengthBinaryReader::<Coordinate2D>::DESCRIPTOR
                );
                Self::Coordinate2DArray
            }
            ValueArrayTerminatedBinaryReader::<VectorRecordPointer>::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    ValueArrayTerminatedBinaryReader::<VectorRecordPointer>::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    ValueArrayTerminatedBinaryReader::<VectorRecordPointer>::DESCRIPTOR
                );
                Self::VectorRecordPointerArray
            }
            ValueArrayTerminatedBinaryReader::<FeatureRecordToSpatialRecordPointer>::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    ValueArrayTerminatedBinaryReader::<FeatureRecordToSpatialRecordPointer>::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    ValueArrayTerminatedBinaryReader::<FeatureRecordToSpatialRecordPointer>::DESCRIPTOR
                );
                Self::FeatureToSpatialRecordPointerArray
            }
            ValueArrayTerminatedBinaryReader::<FeatureRecordToFeatureObjectPointer>::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    ValueArrayTerminatedBinaryReader::<FeatureRecordToFeatureObjectPointer>::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    ValueArrayTerminatedBinaryReader::<FeatureRecordToFeatureObjectPointer>::DESCRIPTOR
                );
                Self::FeatureToObjectPointerArray
            }
            ValueArrayTerminatedReaderBinaryReader::<FeatureRecordAttributeGeneralBinaryReader>::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    ValueArrayTerminatedReaderBinaryReader::<FeatureRecordAttributeGeneralBinaryReader>::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    ValueArrayTerminatedReaderBinaryReader::<FeatureRecordAttributeGeneralBinaryReader>::DESCRIPTOR
                );
                Self::FeatureRecordAttributeArray(AttributeLabelDomain::General)
            }
            ValueArrayTerminatedReaderBinaryReader::<FeatureRecordAttributeNationalBinaryReader>::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    ValueArrayTerminatedReaderBinaryReader::<FeatureRecordAttributeNationalBinaryReader>::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    ValueArrayTerminatedReaderBinaryReader::<FeatureRecordAttributeNationalBinaryReader>::DESCRIPTOR
                );
                Self::FeatureRecordAttributeArray(AttributeLabelDomain::National)
            }
            ValueArrayTerminatedReaderBinaryReader::<VectorRecordAttributeBinaryReader>::TAG => {
                assert_eq!(
                    descriptor.format_controls,
                    ValueArrayTerminatedReaderBinaryReader::<VectorRecordAttributeBinaryReader>::FORMAT
                );
                assert_eq!(
                    descriptor.array_descriptor,
                    ValueArrayTerminatedReaderBinaryReader::<VectorRecordAttributeBinaryReader>::DESCRIPTOR
                );
                Self::VectorRecordAttributeArray
            }
            _ => Self::Fallback,
        }
    }

    fn read_with_ctx<R>(
        &self,
        ctx: DecodingContext,
        entry: &DirectoryEntry,
        reader: &mut R,
    ) -> Result<DataField>
    where
        R: BufRead + Seek,
    {
        match self {
            Self::CatalogDirectoryField => {
                let field_reader = CatalogDirectoryFieldReader::new(ctx);
                field_reader.read_from_reader(reader)
            }
            Self::RecordIdentifier(FieldEncoding::Ascii) => {
                let field_reader = RecordIdentifierAsciiReader::new(ctx);
                field_reader.read_from_reader(reader)
            }
            Self::RecordIdentifier(FieldEncoding::Binary) => {
                let field_reader = RecordIdentifierBinaryReader::new(ctx);
                field_reader.read_from_reader(reader)
            }
            Self::DataSetIdentification => {
                let field_reader = DataSetIdentificationFieldBinaryReader::new(ctx);
                field_reader.read_from_reader(reader)
            }
            Self::DataSetStructureInformation => {
                let field_reader = DataSetStructureInformationBinaryReader::new(ctx);
                field_reader.read_from_reader(reader)
            }
            Self::DataSetParameter => {
                let field_reader = DataSetParameterFieldBinaryReader::new(ctx);
                field_reader.read_from_reader(reader)
            }
            Self::VectorRecordId => {
                let field_reader = VectorRecordIdFieldBinaryReader::new(ctx);
                field_reader.read_from_reader(reader)
            }
            Self::FeatureRecordId => {
                let field_reader = FeatureRecordIdBinaryFieldReader::new(ctx);
                field_reader.read_from_reader(reader)
            }
            Self::FeatureObjectId => {
                let field_reader = FeatureObjectIdFieldBinaryReader::new(ctx);
                field_reader.read_from_reader(reader)
            }
            Self::Sounding3DArray => {
                let value_reader: ValueArrayFixedLengthBinaryReader<Sounding3D> =
                    ValueArrayFixedLengthBinaryReader::with_size(entry.length, ctx)?;
                value_reader.read_from_reader(reader)
            }
            Self::Coordinate2DArray => {
                let value_reader: ValueArrayFixedLengthBinaryReader<Coordinate2D> =
                    ValueArrayFixedLengthBinaryReader::with_size(entry.length, ctx)?;
                value_reader.read_from_reader(reader)
            }
            Self::VectorRecordPointerArray => {
                let value_reader =
                    ValueArrayTerminatedBinaryReader::<VectorRecordPointer>::new(ctx);
                value_reader.read_from_reader(reader)
            }
            Self::FeatureToSpatialRecordPointerArray => {
                let value_reader = ValueArrayTerminatedBinaryReader::<
                    FeatureRecordToSpatialRecordPointer,
                >::new(ctx);
                value_reader.read_from_reader(reader)
            }
            Self::FeatureToObjectPointerArray => {
                let value_reader = ValueArrayTerminatedBinaryReader::<
                    FeatureRecordToFeatureObjectPointer,
                >::new(ctx);
                value_reader.read_from_reader(reader)
            }
            Self::FeatureRecordAttributeArray(AttributeLabelDomain::General) => {
                let value_reader = ValueArrayTerminatedReaderBinaryReader::<
                    FeatureRecordAttributeGeneralBinaryReader,
                >::new(
                    FeatureRecordAttributeGeneralBinaryReader::new(ctx.clone()),
                    entry.length,
                    ctx,
                );
                value_reader.read_from_reader(reader)
            }
            Self::FeatureRecordAttributeArray(AttributeLabelDomain::National) => {
                let value_reader = ValueArrayTerminatedReaderBinaryReader::<
                    FeatureRecordAttributeNationalBinaryReader,
                >::new(
                    FeatureRecordAttributeNationalBinaryReader::new(ctx.clone()),
                    entry.length,
                    ctx,
                );
                value_reader.read_from_reader(reader)
            }
            Self::VectorRecordAttributeArray => {
                let value_reader = ValueArrayTerminatedReaderBinaryReader::<
                    VectorRecordAttributeBinaryReader,
                >::new(
                    VectorRecordAttributeBinaryReader::new(ctx.clone()),
                    entry.length,
                    ctx,
                );
                value_reader.read_from_reader(reader)
            }
            Self::Fallback => {
                let field_reader = FallbackFieldReader::new(ctx);
                field_reader.read_from_reader(reader)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct CatalogDirectoryField {
    /// Must be "CD"
    pub record_name: &'static str,
    /// Record identification number.
    /// Range: 1 to 232-2
    pub record_id: u16,
    /// A string indicating a valid file name.
    pub file_name: String,
    /// A string indicating the long name of the file.
    pub file_long_name: String,
    /// A string indicating a valid volume label for the transfer media on which the file,
    /// indicated by the FILE subfield, is located.
    pub volume: String,
    pub implementation: S57Implementation,
    /// Southernmost latitude of data coverage contained in the file indicated by the FILE subfield.
    /// Degrees of arc, south is negative.
    pub south_lat: f64,
    /// Westernmost longitute of data coverage contained in the file indicated by the FILE subfield.
    /// Degrees of arc, west is negative.
    pub west_lon: f64,
    /// Northernmost latitude of data coverage contained in the file indicated by the FILE subfield.
    /// Degrees of arc, south is negative.
    pub north_lat: f64,
    /// Easternmost longitute of data coverage contained in the file indicated by the FILE subfield.
    /// Degrees of arc, west is negative.
    pub east_lon: f64,
    /// The Cyclic Redundancy Checksum for the file indicated by the FILE subfield (as hex).
    pub crc: String,
    /// A string of characters.
    pub comment: String,
}

trait FieldReader: ByteReader<Target = DataField> {
    const TAG: &'static str;
    const DESCRIPTOR: &'static str;
    const FORMAT: &'static str;
}

struct CatalogDirectoryFieldReader {
    ctx: DecodingContext,
}
impl CatalogDirectoryFieldReader {
    fn new(ctx: DecodingContext) -> Self {
        Self { ctx }
    }
}
impl FieldReader for CatalogDirectoryFieldReader {
    const TAG: &'static str = "CATD";
    const DESCRIPTOR: &'static str = "RCNM!RCID!FILE!LFIL!VOLM!IMPL!SLAT!WLON!NLAT!ELON!CRCS!COMT";
    const FORMAT: &'static str = "(A(2),I(10),3A,A(3),4R,2A)";
}
impl ByteReader for CatalogDirectoryFieldReader {
    type Target = DataField;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        reader.expect_ascii_str("CD")?;
        let entry = CatalogDirectoryField {
            record_name: "CD",
            record_id: reader.parse_number(10)?,
            file_name: reader.read_string_until_unit_terminator(&self.ctx)?,
            file_long_name: reader.read_string_until_unit_terminator(&self.ctx)?,
            volume: reader.read_string_until_unit_terminator(&self.ctx)?,
            implementation: S57Implementation::from_reader(reader)?,
            south_lat: reader.parse_number_until_unit_terminator(&self.ctx)?,
            west_lon: reader.parse_number_until_unit_terminator(&self.ctx)?,
            north_lat: reader.parse_number_until_unit_terminator(&self.ctx)?,
            east_lon: reader.parse_number_until_unit_terminator(&self.ctx)?,
            crc: reader.read_string_until_unit_terminator(&self.ctx)?,
            comment: reader.read_string_until_unit_terminator(&self.ctx)?,
        };
        reader.expect_field_terminator(&self.ctx)?;
        Ok(DataField::CatalogDirectory(Box::new(entry)))
    }
}
//const CATALOG_DIRECTORY_FIELD_READER: CatalogDirectoryFieldReader = CatalogDirectoryFieldReader;

struct RecordIdentifierAsciiReader {
    ctx: DecodingContext,
}
impl RecordIdentifierAsciiReader {
    fn new(ctx: DecodingContext) -> Self {
        Self { ctx }
    }
}
impl FieldReader for RecordIdentifierAsciiReader {
    const TAG: &'static str = "0001";

    const DESCRIPTOR: &'static str = "";

    const FORMAT: &'static str = "(I(5))";
}
impl ByteReader for RecordIdentifierAsciiReader {
    type Target = DataField;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead,
        Self: Sized,
    {
        let id: usize = reader.parse_number(5)?;
        reader.expect_field_terminator(&self.ctx)?;
        Ok(DataField::RecordIdentifier { id })
    }
}
//const RECORD_IDENTIFIER_ASCII_FIELD_READER: RecordIdentifierAsciiReader =
// RecordIdentifierAsciiReader;
struct RecordIdentifierBinaryReader {
    ctx: DecodingContext,
}
impl RecordIdentifierBinaryReader {
    fn new(ctx: DecodingContext) -> Self {
        Self { ctx }
    }
}
impl FieldReader for RecordIdentifierBinaryReader {
    const TAG: &'static str = "0001";

    const DESCRIPTOR: &'static str = "";

    const FORMAT: &'static str = "(b12)";
}
impl ByteReader for RecordIdentifierBinaryReader {
    type Target = DataField;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead,
        Self: Sized,
    {
        let mut buffer = [0u8; 2];
        reader.read_exact(&mut buffer)?;
        let id = u16::from_le_bytes(buffer) as usize;
        reader.expect_field_terminator(&self.ctx)?;
        Ok(DataField::RecordIdentifier { id })
    }
}

#[derive(Clone, Debug)]
pub struct DataSetIdentificationField {
    pub record_name: u8,
    pub record_id: u32,
    pub exchange_purpose: char,
    pub intended_usage: u8,
    pub data_set_name: String,
    pub edition_number: String,
    pub update_number: String,
    pub update_application_date: String,
    pub issue_date: String,
    pub edition_number_s57: String,
    pub product_specification: ProductSpecification,
    pub product_specification_description: String,
    pub product_specification_edition_number: String,
    pub application_profile_id: ApplicationProfileId,
    pub producing_agency: u16,
    pub comment: String,
}
impl Record for DataSetIdentificationField {
    fn unique_name(&self) -> RecordName {
        RecordName {
            name: self.record_name,
            identifier: self.record_id,
        }
    }
}
struct DataSetIdentificationFieldBinaryReader {
    ctx: DecodingContext,
}
impl DataSetIdentificationFieldBinaryReader {
    fn new(ctx: DecodingContext) -> Self {
        Self { ctx }
    }
}
impl FieldReader for DataSetIdentificationFieldBinaryReader {
    const TAG: &'static str = "DSID";

    const DESCRIPTOR: &'static str =
        "RCNM!RCID!EXPP!INTU!DSNM!EDTN!UPDN!UADT!ISDT!STED!PRSP!PSDN!PRED!PROF!AGEN!COMT";

    const FORMAT: &'static str = "(b11,b14,2b11,3A,2A(8),R(4),b11,2A,b11,b12,A)";
}
impl ByteReader for DataSetIdentificationFieldBinaryReader {
    type Target = DataField;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let field = DataSetIdentificationField {
            record_name: reader.read_u8()?,
            record_id: reader.read_u32::<LE>()?,
            exchange_purpose: reader.read_u8().map(|n| match n {
                1 => 'N',
                2 => 'R',
                _ => n.to_ascii_lowercase() as char,
            })?,
            intended_usage: reader.read_u8()?,
            data_set_name: reader.read_string_until_unit_terminator(&self.ctx)?,
            edition_number: reader.read_string_until_unit_terminator(&self.ctx)?,
            update_number: reader.read_string_until_unit_terminator(&self.ctx)?,
            update_application_date: reader.read_ascii_string(8)?,
            issue_date: reader.read_ascii_string(8)?,
            edition_number_s57: reader.read_ascii_string(4)?,
            product_specification: ProductSpecification::from_reader(reader)?,
            product_specification_description: reader
                .read_string_until_unit_terminator(&self.ctx)?,
            product_specification_edition_number: reader
                .read_string_until_unit_terminator(&self.ctx)?,
            application_profile_id: ApplicationProfileId::from_reader(reader)?,
            producing_agency: reader.read_u16::<LE>()?,
            comment: reader.read_string_until_unit_terminator(&self.ctx)?,
        };
        reader.expect_field_terminator(&self.ctx)?;
        Ok(DataField::DataSetIdentification(Box::new(field)))
    }
}

#[derive(Clone, Debug)]
pub struct DataSetStructureInformation {
    pub data_structure: DataStructure,
    pub attf_lexical_level: u8,
    pub natf_lexical_level: u8,
    pub number_of_meta_records: u32,
    pub number_of_cartographic_records: u32,
    pub number_of_geo_records: u32,
    pub number_of_collection_records: u32,
    pub number_of_isolated_node_records: u32,
    pub number_of_connected_node_records: u32,
    pub number_of_edge_records: u32,
    pub number_of_face_records: u32,
}

struct DataSetStructureInformationBinaryReader {
    ctx: DecodingContext,
}
impl DataSetStructureInformationBinaryReader {
    fn new(ctx: DecodingContext) -> Self {
        Self { ctx }
    }
}
impl FieldReader for DataSetStructureInformationBinaryReader {
    const TAG: &'static str = "DSSI";

    const DESCRIPTOR: &'static str = "DSTR!AALL!NALL!NOMR!NOCR!NOGR!NOLR!NOIN!NOCN!NOED!NOFA";

    const FORMAT: &'static str = "(3b11,8b14)";
}
impl ByteReader for DataSetStructureInformationBinaryReader {
    type Target = DataField;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let field = DataSetStructureInformation {
            data_structure: DataStructure::from_reader(reader)?,
            attf_lexical_level: reader.read_u8()?,
            natf_lexical_level: reader.read_u8()?,
            number_of_meta_records: reader.read_u32::<LE>()?,
            number_of_cartographic_records: reader.read_u32::<LE>()?,
            number_of_geo_records: reader.read_u32::<LE>()?,
            number_of_collection_records: reader.read_u32::<LE>()?,
            number_of_isolated_node_records: reader.read_u32::<LE>()?,
            number_of_connected_node_records: reader.read_u32::<LE>()?,
            number_of_edge_records: reader.read_u32::<LE>()?,
            number_of_face_records: reader.read_u32::<LE>()?,
        };
        reader.expect_field_terminator(&self.ctx)?;
        Ok(DataField::DataStructure(Box::new(field)))
    }
}

#[derive(Clone, Debug)]
pub struct DataSetParameterField {
    pub record_name: u8,
    pub record_id: u32,
    // These all come from the Appendix A - Object Catalogue (*DAT)
    pub horizontal_geodetic_datum: u8,
    pub vertical_datum: u8,
    pub sounding_datum: u8,
    /// The modulus of the compilation scale.
    /// For example, a scale of 1:25000 is encoded as 25000.
    pub compilation_scale_of_data: u32,
    // These all come from Appendix A - Object Catalogue (*UNITS)
    pub units_of_depth: u8,
    pub units_of_height: u8,
    pub units_of_positional_accuracy: u8,
    pub coordinate_units: CoordinateUnits,
    /// Floating-point to integer multiplication factor for coordinate values.
    pub coordinate_multiplication_factor: u32,
    /// Floating point to integer multiplication factor for 3-D (sounding) values.
    pub sounding_multiplication_factor: u32,
    pub comment: String,
}
impl Record for DataSetParameterField {
    fn unique_name(&self) -> RecordName {
        RecordName {
            name: self.record_name,
            identifier: self.record_id,
        }
    }
}
struct DataSetParameterFieldBinaryReader {
    ctx: DecodingContext,
}
impl DataSetParameterFieldBinaryReader {
    fn new(ctx: DecodingContext) -> Self {
        Self { ctx }
    }
}
impl FieldReader for DataSetParameterFieldBinaryReader {
    const TAG: &'static str = "DSPM";

    const DESCRIPTOR: &'static str =
        "RCNM!RCID!HDAT!VDAT!SDAT!CSCL!DUNI!HUNI!PUNI!COUN!COMF!SOMF!COMT";

    const FORMAT: &'static str = "(b11,b14,3b11,b14,4b11,2b14,A)";
}
impl ByteReader for DataSetParameterFieldBinaryReader {
    type Target = DataField;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let field = DataSetParameterField {
            record_name: reader.read_u8()?,
            record_id: reader.read_u32::<LE>()?,
            horizontal_geodetic_datum: reader.read_u8()?,
            vertical_datum: reader.read_u8()?,
            sounding_datum: reader.read_u8()?,
            compilation_scale_of_data: reader.read_u32::<LE>()?,
            units_of_depth: reader.read_u8()?,
            units_of_height: reader.read_u8()?,
            units_of_positional_accuracy: reader.read_u8()?,
            coordinate_units: CoordinateUnits::from_reader(reader)?,
            coordinate_multiplication_factor: reader.read_u32::<LE>()?,
            sounding_multiplication_factor: reader.read_u32::<LE>()?,
            comment: reader.read_string_until_unit_terminator(&self.ctx)?,
        };
        reader.expect_field_terminator(&self.ctx)?;
        Ok(DataField::DataSetParameter(Box::new(field)))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct VectorRecordIdField {
    pub record_name: u8,
    pub record_id: u32,
    pub record_version: u16,
    pub record_update_instruction: UpdateInstruction,
}
impl Record for VectorRecordIdField {
    fn unique_name(&self) -> RecordName {
        RecordName {
            name: self.record_name,
            identifier: self.record_id,
        }
    }
}
struct VectorRecordIdFieldBinaryReader {
    ctx: DecodingContext,
}
impl VectorRecordIdFieldBinaryReader {
    fn new(ctx: DecodingContext) -> Self {
        Self { ctx }
    }
}
impl FieldReader for VectorRecordIdFieldBinaryReader {
    const TAG: &'static str = "VRID";

    const DESCRIPTOR: &'static str = "RCNM!RCID!RVER!RUIN";

    const FORMAT: &'static str = "(b11,b14,b12,b11)";
}
impl ByteReader for VectorRecordIdFieldBinaryReader {
    type Target = DataField;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let field = VectorRecordIdField {
            record_name: reader.read_u8()?,
            record_id: reader.read_u32::<LE>()?,
            record_version: reader.read_u16::<LE>()?,
            record_update_instruction: UpdateInstruction::from_reader(reader)?,
        };
        reader.expect_field_terminator(&self.ctx)?;
        Ok(DataField::VectorRecordId(field))
    }
}

#[derive(Clone, Debug)]
pub struct ValueArray<T> {
    pub values: Vec<T>,
}

struct ValueArrayFixedLengthBinaryReader<T>
where
    T: FixedSizeValue + FromFixedLengthBytes,
{
    num_values: usize,
    ctx: DecodingContext,
    phantom: PhantomData<T>,
}
impl<T> ValueArrayFixedLengthBinaryReader<T>
where
    T: FixedSizeValue + FromFixedLengthBytes,
{
    fn with_size(num_bytes: usize, ctx: DecodingContext) -> Result<Self> {
        let num_values = num_bytes / T::BINARY_SIZE;
        let rest = num_bytes % T::BINARY_SIZE;
        // The 1 is the field terminator.
        ensure!(
            rest == 1,
            FormatViolationSnafu {
                description: format!(
                    "Expected {} records of size {}, but got {} remaining bytes.",
                    num_values,
                    T::BINARY_SIZE,
                    rest
                )
            }
        );
        Ok(Self {
            num_values,
            ctx,
            phantom: PhantomData::default(),
        })
    }
}
impl<T> FieldReader for ValueArrayFixedLengthBinaryReader<T>
where
    T: FixedSizeValue + FromFixedLengthBytes,
{
    const TAG: &'static str = T::TAG;

    const DESCRIPTOR: &'static str = T::DESCRIPTOR;

    const FORMAT: &'static str = T::FORMAT;
}
impl<T> ByteReader for ValueArrayFixedLengthBinaryReader<T>
where
    T: FixedSizeValue + FromFixedLengthBytes,
{
    type Target = DataField;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let mut data = Vec::with_capacity(self.num_values);
        let mut buffer = vec![0u8; T::BINARY_SIZE];
        for _ in 0..self.num_values {
            reader.read_exact(&mut buffer)?;
            let value = T::from_bytes_unchecked(&buffer)?;
            data.push(value);
        }
        reader.expect_field_terminator(&self.ctx)?;
        Ok(T::wrap_array_in_data_field(ValueArray { values: data }))
    }
}

struct ValueArrayTerminatedBinaryReader<T>
where
    T: TerminatedValue + FromBytes,
{
    ctx: DecodingContext,
    phantom: PhantomData<T>,
}
impl<T> ValueArrayTerminatedBinaryReader<T>
where
    T: TerminatedValue + FromBytes,
{
    fn new(ctx: DecodingContext) -> Self {
        Self {
            ctx,
            phantom: PhantomData::default(),
        }
    }
}
impl<T> FieldReader for ValueArrayTerminatedBinaryReader<T>
where
    T: TerminatedValue + FromBytes,
{
    const TAG: &'static str = T::TAG;

    const DESCRIPTOR: &'static str = T::DESCRIPTOR;

    const FORMAT: &'static str = T::FORMAT;
}
impl<T> ByteReader for ValueArrayTerminatedBinaryReader<T>
where
    T: TerminatedValue + FromBytes,
{
    type Target = DataField;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let mut data = Vec::new();
        while !reader.peek_is_field_terminator(&self.ctx)? {
            let value = T::from_reader(reader)?;
            data.push(value);
        }
        reader.expect_field_terminator(&self.ctx)?;
        Ok(T::wrap_array_in_data_field(ValueArray { values: data }))
    }
}

struct ValueArrayTerminatedReaderBinaryReader<R>
where
    R: TerminatedValueReader + ByteReader,
{
    value_reader: R,
    expected_size: usize,
    ctx: DecodingContext,
}
impl<R> ValueArrayTerminatedReaderBinaryReader<R>
where
    R: TerminatedValueReader + ByteReader,
{
    fn new(value_reader: R, expected_size: usize, ctx: DecodingContext) -> Self {
        Self {
            value_reader,
            expected_size,
            ctx,
        }
    }
}
impl<R> FieldReader for ValueArrayTerminatedReaderBinaryReader<R>
where
    R: TerminatedValueReader + ByteReader,
{
    const TAG: &'static str = R::TAG;

    const DESCRIPTOR: &'static str = R::DESCRIPTOR;

    const FORMAT: &'static str = R::FORMAT;
}
impl<R> ByteReader for ValueArrayTerminatedReaderBinaryReader<R>
where
    R: TerminatedValueReader + ByteReader,
{
    type Target = DataField;

    fn read_from_reader<B>(&self, reader: &mut B) -> Result<Self::Target>
    where
        B: BufRead + Seek,
        Self: Sized,
    {
        let mut data = Vec::new();
        let start_pos = reader.stream_position()?;
        'value_reading: loop {
            if reader.peek_is_field_terminator(&self.ctx)? {
                let current_pos = reader.stream_position()?;
                if current_pos + 1 == start_pos + self.expected_size as u64 {
                    break 'value_reading;
                }
                assert!(
                    current_pos + 1 < start_pos + self.expected_size as u64,
                    "We read too far. Current={}, expected end={}",
                    current_pos,
                    start_pos + self.expected_size as u64
                );
                // Otherwise we keep reading despite the terminator character,
                // because it may be part of a value (e.g. the next u16).
            }
            let value = self.value_reader.read_from_reader(reader)?;
            data.push(value);
        }
        reader.expect_field_terminator(&self.ctx)?;
        Ok(self
            .value_reader
            .wrap_array_in_data_field(ValueArray { values: data }))
    }
}

trait FixedSizeValue: FromFixedLengthBytes {
    /// Number of bytes in this record.
    const BINARY_SIZE: usize;

    const TAG: &'static str;
    const DESCRIPTOR: &'static str;
    const FORMAT: &'static str;

    fn wrap_array_in_data_field(data: ValueArray<Self>) -> DataField
    where
        Self: Sized;
}

trait TerminatedValue: FromBytes {
    const TAG: &'static str;
    const DESCRIPTOR: &'static str;
    const FORMAT: &'static str;

    fn wrap_array_in_data_field(data: ValueArray<Self>) -> DataField
    where
        Self: Sized;
}

trait TerminatedValueReader: ByteReader {
    const TAG: &'static str;
    const DESCRIPTOR: &'static str;
    const FORMAT: &'static str;

    fn wrap_array_in_data_field(&self, data: ValueArray<Self::Target>) -> DataField
    where
        Self: Sized;
}

#[derive(Clone, Copy, Debug)]
pub struct Sounding3D {
    pub y_coordinate: i32,
    pub x_coordinate: i32,
    pub sounding_value: i32,
}
impl FixedSizeValue for Sounding3D {
    const BINARY_SIZE: usize = 12;

    const TAG: &'static str = "SG3D";
    /// The * should come from [[ValueArrayBinaryReader]], but Rust doesn't support this kind
    /// of const concat at the moment.
    const DESCRIPTOR: &'static str = "*YCOO!XCOO!VE3D";
    const FORMAT: &'static str = "(3b24)";

    fn wrap_array_in_data_field(data: ValueArray<Self>) -> DataField
    where
        Self: Sized,
    {
        DataField::Array(DataArray::Sounding(data))
    }
}
impl FromFixedLengthBytes for Sounding3D {
    const LENGTH: usize = Self::BINARY_SIZE;

    fn from_bytes_unchecked(mut data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let value = Self {
            y_coordinate: data.read_i32::<LE>()?,
            x_coordinate: data.read_i32::<LE>()?,
            sounding_value: data.read_i32::<LE>()?,
        };
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Coordinate2D {
    pub y_coordinate: i32,
    pub x_coordinate: i32,
}
impl FixedSizeValue for Coordinate2D {
    const BINARY_SIZE: usize = 8;

    const TAG: &'static str = "SG2D";
    /// The * should come from [[ValueArrayBinaryReader]], but Rust doesn't support this kind
    /// of const concat at the moment.
    const DESCRIPTOR: &'static str = "*YCOO!XCOO";
    const FORMAT: &'static str = "(2b24)";

    fn wrap_array_in_data_field(data: ValueArray<Self>) -> DataField
    where
        Self: Sized,
    {
        DataField::Array(DataArray::Coordinate(data))
    }
}
impl FromFixedLengthBytes for Coordinate2D {
    const LENGTH: usize = Self::BINARY_SIZE;

    fn from_bytes_unchecked(mut data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let value = Self {
            y_coordinate: data.read_i32::<LE>()?,
            x_coordinate: data.read_i32::<LE>()?,
        };
        Ok(value)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorRecordPointer {
    pub name: RecordName,
    pub orientation: Option<Orientation>,
    pub usage_indicator: Option<UsageIndicator>,
    pub topology_indicator: Option<TopologyIndicator>,
    pub masking_indicator: Option<MaskingIndicator>,
}

impl TerminatedValue for VectorRecordPointer {
    const TAG: &'static str = "VRPT";

    const DESCRIPTOR: &'static str = "*NAME!ORNT!USAG!TOPI!MASK";

    const FORMAT: &'static str = "(B(40),4b11)";

    fn wrap_array_in_data_field(data: ValueArray<Self>) -> DataField
    where
        Self: Sized,
    {
        DataField::Array(DataArray::VectorRecordPointer(data))
    }
}
impl FromBytes for VectorRecordPointer {
    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let value = Self {
            name: RecordName::from_reader(reader)?,
            orientation: FromBytes::from_reader(reader)?,
            usage_indicator: FromBytes::from_reader(reader)?,
            topology_indicator: FromBytes::from_reader(reader)?,
            masking_indicator: FromBytes::from_reader(reader)?,
        };
        Ok(value)
    }
}

#[derive(Clone, Debug)]
pub struct FeatureRecordToSpatialRecordPointer {
    pub name: RecordName,
    pub orientation: Option<Orientation>,
    pub usage_indicator: Option<UsageIndicator>,
    pub masking_indicator: Option<MaskingIndicator>,
}

impl TerminatedValue for FeatureRecordToSpatialRecordPointer {
    const TAG: &'static str = "FSPT";

    const DESCRIPTOR: &'static str = "*NAME!ORNT!USAG!MASK";

    const FORMAT: &'static str = "(B(40),3b11)";

    fn wrap_array_in_data_field(data: ValueArray<Self>) -> DataField
    where
        Self: Sized,
    {
        DataField::Array(DataArray::FeatureToSpatialRecordPointer(data))
    }
}
impl FromBytes for FeatureRecordToSpatialRecordPointer {
    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let value = Self {
            name: RecordName::from_reader(reader)?,
            orientation: FromBytes::from_reader(reader)?,
            usage_indicator: FromBytes::from_reader(reader)?,
            masking_indicator: FromBytes::from_reader(reader)?,
        };
        Ok(value)
    }
}

#[derive(Clone, Debug)]
pub struct FeatureRecordToFeatureObjectPointer {
    pub name: LongRecordName,
    pub relationship_indicator: RelationshipIndicator,
    pub comment: String,
}
impl fmt::Display for FeatureRecordToFeatureObjectPointer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ({}) - {}",
            self.name, self.relationship_indicator, self.comment
        )
    }
}
impl TerminatedValue for FeatureRecordToFeatureObjectPointer {
    const TAG: &'static str = "FFPT";

    const DESCRIPTOR: &'static str = "*LNAM!RIND!COMT";

    const FORMAT: &'static str = "(B(64),b11,A)";

    fn wrap_array_in_data_field(data: ValueArray<Self>) -> DataField
    where
        Self: Sized,
    {
        DataField::Array(DataArray::FeatureToFeatureObjectPointer(data))
    }
}
impl FromBytes for FeatureRecordToFeatureObjectPointer {
    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let value = Self {
            name: LongRecordName::from_reader(reader)?,
            relationship_indicator: FromBytes::from_reader(reader)?,
            // I think this one is always ASCII/Level 0
            comment: reader.read_string_until_unit_terminator(&DDR_DECODING_CONTEXT)?,
        };
        Ok(value)
    }
}

#[derive(Clone, Debug)]
pub struct FeatureRecordIdField {
    pub record_name: u8,
    pub record_id: u32,
    pub object_geometric_primitive: Option<ObjectGeometricPrimitive>,
    pub group: u8,
    pub object_label: u16,
    pub record_version: u16,
    pub record_update_instruction: UpdateInstruction,
}
impl Record for FeatureRecordIdField {
    fn unique_name(&self) -> RecordName {
        RecordName {
            name: self.record_name,
            identifier: self.record_id,
        }
    }
}
struct FeatureRecordIdBinaryFieldReader {
    ctx: DecodingContext,
}
impl FeatureRecordIdBinaryFieldReader {
    fn new(ctx: DecodingContext) -> Self {
        Self { ctx }
    }
}
impl FieldReader for FeatureRecordIdBinaryFieldReader {
    const TAG: &'static str = "FRID";

    const DESCRIPTOR: &'static str = "RCNM!RCID!PRIM!GRUP!OBJL!RVER!RUIN";

    const FORMAT: &'static str = "(b11,b14,2b11,2b12,b11)";
}
impl ByteReaderWithKnownSize for FeatureRecordIdBinaryFieldReader {
    type Target = DataField;

    fn data_len(&self) -> usize {
        13
    }

    fn read_from_bytes_unchecked(&self, data: &[u8]) -> Result<Self::Target>
    where
        Self: Sized,
    {
        let mut cursor = Cursor::new(data);
        let value = FeatureRecordIdField {
            record_name: cursor.read_u8()?,
            record_id: cursor.read_u32::<LE>()?,
            object_geometric_primitive: FromBytes::from_reader(&mut cursor)?,
            group: cursor.read_u8()?,
            object_label: cursor.read_u16::<LE>()?,
            record_version: cursor.read_u16::<LE>()?,
            record_update_instruction: UpdateInstruction::from_reader(&mut cursor)?,
        };
        assert_eq!(
            value.record_name, 100,
            "FRID should have record name 100 according to spec",
        );
        cursor.expect_field_terminator(&self.ctx)?;
        Ok(DataField::FeatureRecordId(value))
    }
}

// They have identical structure.
pub type FeatureObjectIdField = LongRecordName;
struct FeatureObjectIdFieldBinaryReader {
    ctx: DecodingContext,
}
impl FeatureObjectIdFieldBinaryReader {
    fn new(ctx: DecodingContext) -> Self {
        Self { ctx }
    }
}
impl FieldReader for FeatureObjectIdFieldBinaryReader {
    const TAG: &'static str = "FOID";

    const DESCRIPTOR: &'static str = "AGEN!FIDN!FIDS";

    const FORMAT: &'static str = "(b12,b14,b12)";
}
impl ByteReaderWithKnownSize for FeatureObjectIdFieldBinaryReader {
    type Target = DataField;

    fn data_len(&self) -> usize {
        9
    }

    fn read_from_bytes_unchecked(&self, data: &[u8]) -> Result<Self::Target>
    where
        Self: Sized,
    {
        let value = FeatureObjectIdField::from_bytes_unchecked(&data[..9])?;
        (&data[8..]).expect_field_terminator(&self.ctx)?;
        Ok(DataField::FeatureObjectId(value))
    }
}

#[derive(Clone, Debug)]
pub struct FeatureRecordAttribute {
    pub label_domain: AttributeLabelDomain,
    pub label: u16,
    pub value: String,
}

/// Identical to [[FeatureRecordAttribute]].
pub type VectorRecordAttribute = FeatureRecordAttribute;

struct FeatureRecordAttributeGeneralBinaryReader {
    ctx: DecodingContext,
}
impl FeatureRecordAttributeGeneralBinaryReader {
    fn new(ctx: DecodingContext) -> Self {
        Self { ctx }
    }
}
impl TerminatedValueReader for FeatureRecordAttributeGeneralBinaryReader {
    const TAG: &'static str = "ATTF";

    const DESCRIPTOR: &'static str = "*ATTL!ATVL";

    const FORMAT: &'static str = "(b12,A)";

    fn wrap_array_in_data_field(&self, data: ValueArray<Self::Target>) -> DataField
    where
        Self: Sized,
    {
        DataField::Array(DataArray::FeatureAttribute(data))
    }
}
impl ByteReader for FeatureRecordAttributeGeneralBinaryReader {
    type Target = FeatureRecordAttribute;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let value = FeatureRecordAttribute {
            label_domain: AttributeLabelDomain::General,
            label: reader.read_u16::<LE>()?,
            value: reader
                .read_string_until_unit_terminator(&self.ctx)
                // Sometimes files put Level 1 encoded strings into the Level 0 marked fields -.-
                .unwrap_or_else(|_| "<invalid value>".to_string()),
        };
        Ok(value)
    }
}

/// This is basically identical to [[FeatureRecordAttributeGeneralBinaryReader]] except for the domain.
struct FeatureRecordAttributeNationalBinaryReader {
    general_reader: FeatureRecordAttributeGeneralBinaryReader,
}
impl FeatureRecordAttributeNationalBinaryReader {
    fn new(ctx: DecodingContext) -> Self {
        Self {
            general_reader: FeatureRecordAttributeGeneralBinaryReader::new(ctx),
        }
    }
}
impl TerminatedValueReader for FeatureRecordAttributeNationalBinaryReader {
    const TAG: &'static str = "NATF";

    const DESCRIPTOR: &'static str = FeatureRecordAttributeGeneralBinaryReader::DESCRIPTOR;

    const FORMAT: &'static str = FeatureRecordAttributeGeneralBinaryReader::FORMAT;

    fn wrap_array_in_data_field(&self, data: ValueArray<Self::Target>) -> DataField
    where
        Self: Sized,
    {
        self.general_reader.wrap_array_in_data_field(data)
    }
}
impl ByteReader for FeatureRecordAttributeNationalBinaryReader {
    type Target = FeatureRecordAttribute;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        self.general_reader.read_from_reader(reader).map(|mut res| {
            res.label_domain = AttributeLabelDomain::National;
            res
        })
    }
}

/// This is basically identical to [[FeatureRecordAttributeGeneralBinaryReader]] except that it
/// applies to vector records.
struct VectorRecordAttributeBinaryReader {
    general_reader: FeatureRecordAttributeGeneralBinaryReader,
}
impl VectorRecordAttributeBinaryReader {
    fn new(ctx: DecodingContext) -> Self {
        Self {
            general_reader: FeatureRecordAttributeGeneralBinaryReader::new(ctx),
        }
    }
}
impl TerminatedValueReader for VectorRecordAttributeBinaryReader {
    const TAG: &'static str = "ATTV";

    const DESCRIPTOR: &'static str = FeatureRecordAttributeGeneralBinaryReader::DESCRIPTOR;

    const FORMAT: &'static str = FeatureRecordAttributeGeneralBinaryReader::FORMAT;

    fn wrap_array_in_data_field(&self, data: ValueArray<Self::Target>) -> DataField
    where
        Self: Sized,
    {
        DataField::Array(DataArray::VectorAttribute(data))
    }
}
impl ByteReader for VectorRecordAttributeBinaryReader {
    type Target = FeatureRecordAttribute;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        self.general_reader.read_from_reader(reader)
    }
}

pub(super) struct FallbackFieldReader {
    ctx: DecodingContext,
}
impl FallbackFieldReader {
    fn new(ctx: DecodingContext) -> Self {
        Self { ctx }
    }
}
// impl FieldReader for FallbackFieldReader {
//     const TAG: &'static str = "";

//     const DESCRIPTOR: &'static str = "";

//     const FORMAT: &'static str = "";
// }
impl ByteReader for FallbackFieldReader {
    type Target = DataField;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead,
        Self: Sized,
    {
        let bytes =
            reader.read_bytes_until_terminator(self.ctx.lexical_level.field_terminator())?;
        // The clone here is clearly inefficient, but then again,
        // this whole thing is for debugging only anyway.
        // match self.ctx.bytes_to_string(bytes.clone()) {
        //     Ok(field_str) => Ok(DataField::UnsupportedString(field_str)),
        //     Err(e) => {
        //         eprintln!("Error decoding string from bytes: {e}");
        //         Ok(DataField::UnsupportedBytes(bytes.into_boxed_slice()))
        //     }
        // }
        Ok(DataField::UnsupportedBytes(bytes.into_boxed_slice()))
    }
}
//const FALLBACK_FIELD_READER: FallbackFieldReader = FallbackFieldReader;

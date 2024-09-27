use super::*;
use bytes::Buf;
use can2k_geo::utils::OptionExt;
use data_fields::DataFieldReaderProducer;
use itertools::process_results;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufReader, SeekFrom},
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub struct Generic8211FileReader {
    pub file_name: PathBuf,
    pub ddr_record: DdrRecord,
}
impl Generic8211FileReader {
    /// Open `path` and try to read the DDR record.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let f = File::open(path)?;
        let mut reader = BufReader::new(f);
        let ddr_record = DdrRecord::from_reader(&mut reader).unwrap();
        Ok(Self {
            file_name: path_buf,
            ddr_record,
        })
    }

    /// Gives an iterator that provides access to all DR records.
    pub fn data_records(&self) -> Result<impl Iterator<Item = Result<DrRecord>>> {
        let record_reader = self.ddr_record.record_reader()?;
        self.read_data_records(record_reader)
    }

    /// Gives an iterator that provides access to all DR records, but without any fields parsed.
    /// Only leader and directory entries are filled.
    pub fn data_record_headers(&self) -> Result<impl Iterator<Item = Result<DrRecord>>> {
        let record_reader = DrRecordNoFieldsReader;
        self.read_data_records(record_reader)
    }

    /// Gives an iterator that provides access to the DR records at the specified offsets.
    pub fn data_records_at_offsets(
        &self,
        offsets: Vec<u64>,
    ) -> Result<impl Iterator<Item = Result<DrRecord>>> {
        debug_assert!(offsets.is_sorted());
        let f = File::open(&self.file_name)?;
        let record_reader = self.ddr_record.record_reader()?;
        let mut reader = BufReader::new(f);
        let it = offsets.into_iter().map(move |offset| {
            reader.seek(SeekFrom::Start(offset))?;
            record_reader.read_from_reader(&mut reader)
        });
        Ok(it)
    }

    fn read_data_records<R>(
        &self,
        record_reader: R,
    ) -> Result<impl Iterator<Item = Result<DrRecord>>>
    where
        R: ByteReader<Target = DrRecord>,
    {
        let f = File::open(&self.file_name)?;
        let mut reader = BufReader::new(f);
        reader.seek(SeekFrom::Current(
            self.ddr_record.leader.record_length as i64,
        ))?;
        let it = std::iter::from_fn(move || {
            let dr_record_result = record_reader.read_from_reader(&mut reader);
            let stop_iterating = if let Err(EncError::Io {
                source: ref io_error,
                ..
            }) = dr_record_result
            {
                io_error.kind() == std::io::ErrorKind::UnexpectedEof
            } else {
                false
            };
            Option::when(!stop_iterating, || dr_record_result)
        });
        Ok(it)
    }
}

/// Number of bytes in the leader of a DDR header.
const DDR_LEADER_SIZE: usize = 24;

#[derive(Debug)]
pub struct DdrRecord {
    /// Must be a DDR leader.
    pub leader: Leader,
    pub directory: Vec<DirectoryEntry>,
    pub control_field: FieldControlField,
    pub fields: Vec<DataDescriptiveField>,
}
impl DdrRecord {
    pub fn get_descriptor_with_tag(&self, tag: &str) -> Option<&DataDescriptiveField> {
        if tag == CONTROL_FIELD_TAG {
            None // access this directly (different type)
        } else {
            self.directory
                .iter()
                .position(|entry| entry.tag == tag)
                .map(|directory_position| {
                    &self.fields[directory_position - 1] // skip control_field index
                })
        }
    }

    fn record_reader(&self) -> Result<DrRecordReader> {
        let mut field_readers = HashMap::with_capacity(self.fields.len());
        for (data_descriptor, entry) in self.fields.iter().zip(self.directory.iter().skip(1)) {
            let producer = DataFieldReaderProducer::for_data_descriptor(entry, data_descriptor)?;
            field_readers.insert(entry.tag.clone(), producer);
        }
        Ok(DrRecordReader { field_readers })
    }
}
impl FromBytes for DdrRecord {
    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let leader = Leader::from_reader(reader)?;
        EncError::assert(
            || leader.is_ddr_leader(),
            || "DDR Record should start with DDR Leader".to_string(),
        )?;
        let num_directory_entries = leader.num_directory_entries();
        let mut directory: Vec<DirectoryEntry> = Vec::with_capacity(num_directory_entries);
        let directory_entry_reader = leader.directory_entry_reader();
        for _ in 0..num_directory_entries {
            let entry = directory_entry_reader.read_from_reader(reader)?;
            directory.push(entry);
        }
        reader.expect_field_terminator(&DDR_DECODING_CONTEXT)?;
        assert!(directory.len() >= 1, "Must have at least a control field");
        let control_field = FieldControlField::from_reader(reader)?;
        let mut fields = Vec::with_capacity(directory.len() - 1);
        for _entry in directory.iter().skip(1) {
            let data_descriptive_field = DataDescriptiveField::from_reader(reader)?;
            fields.push(data_descriptive_field);
        }
        Ok(DdrRecord {
            leader,
            directory,
            control_field,
            fields,
        })
    }
}

#[derive(Debug)]
pub struct DrRecord {
    /// Must be a DR leader.
    pub leader: Leader,
    pub directory: Vec<DirectoryEntry>,
    pub fields: Vec<DataField>,
}
impl DrRecord {
    pub fn get_field_with_tag(&self, tag: &str) -> Option<&DataField> {
        self.directory
            .iter()
            .position(|entry| entry.tag == tag)
            .map(|field_index| &self.fields[field_index])
    }

    pub fn take_field_with_tag(self, tag: &str) -> Option<DataField> {
        let position = self.directory.iter().position(|entry| entry.tag == tag);
        position.map(move |field_index| self.fields.into_iter().nth(field_index).unwrap())
    }

    pub fn take_fields_with_tag_in<'a>(self, tags: &HashSet<&'a str>) -> Vec<DataField> {
        let mut positions = self
            .directory
            .iter()
            .positions(|entry| tags.contains(entry.tag.as_str()))
            .collect_vec();
        positions.sort();
        self.fields
            .into_iter()
            .enumerate()
            .filter_map(|(index, value)| positions.binary_search(&index).ok().map(|_| value))
            .collect()
    }
}

struct DrRecordReader {
    field_readers: HashMap<String, DataFieldReaderProducer>,
}
impl ByteReader for DrRecordReader {
    type Target = DrRecord;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let start_pos = reader.stream_position()?;
        let leader = Leader::from_reader(reader)?;
        let ctx = DecodingContext::default();
        EncError::assert(
            || leader.is_dr_leader(),
            || "DR Record should start with DR Leader".to_string(),
        )?;
        let num_directory_entries = leader.num_directory_entries();
        let mut directory: Vec<DirectoryEntry> = Vec::with_capacity(num_directory_entries);
        let directory_entry_reader = leader.directory_entry_reader();
        for _ in 0..num_directory_entries {
            let entry = directory_entry_reader.read_from_reader(reader)?;
            directory.push(entry);
        }
        // The directory always uses the single byte terminator, I think.
        reader.expect_field_terminator(&DDR_DECODING_CONTEXT)?;
        let mut fields = Vec::with_capacity(directory.len());
        for entry in directory.iter() {
            let entry_ctx = ctx.clone();
            let field_reader = self
                .field_readers
                .get(&entry.tag)
                .expect("Should have a field format for every tag");
            let pre_read_pos = reader.stream_position()?;
            let field = field_reader.read_with_ctx(entry_ctx, entry, reader)?;
            let post_read_pos = reader.stream_position()?;
            assert_eq!(
                entry.length,
                (post_read_pos - pre_read_pos) as usize,
                "Inconsistent field read for field {entry:?}"
            );
            //let field_string = reader.read_string(entry.length)?;
            fields.push(field);
        }
        let expected_end_pos = start_pos + leader.record_length as u64;
        let actual_end_pos = reader.stream_position()?;
        if actual_end_pos < expected_end_pos {
            let mut buffer = vec![0u8; (expected_end_pos - actual_end_pos) as usize];
            reader.read_exact(&mut buffer)?;
            eprintln!(
                "Inconsistent read, there are {} bytes remaining: {:?}",
                buffer.len(),
                buffer
            );
            // Fix the stream.
            reader.seek(SeekFrom::Start(expected_end_pos))?;
        } else if actual_end_pos > expected_end_pos {
            eprintln!(
                "Inconsistent read, over-read by {} bytes.",
                actual_end_pos - expected_end_pos
            );
            // Fix the stream.
            reader.seek(SeekFrom::Start(expected_end_pos))?;
        }
        Ok(DrRecord {
            leader,
            directory,
            fields,
        })
    }
}

struct DrRecordNoFieldsReader;
impl ByteReader for DrRecordNoFieldsReader {
    type Target = DrRecord;

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let start_pos = reader.stream_position()?;
        let leader = Leader::from_reader(reader)?;
        EncError::assert(
            || leader.is_dr_leader(),
            || "DR Record should start with DR Leader".to_string(),
        )?;
        let num_directory_entries = leader.num_directory_entries();
        let mut directory: Vec<DirectoryEntry> = Vec::with_capacity(num_directory_entries);
        let directory_entry_reader = leader.directory_entry_reader();
        for _ in 0..num_directory_entries {
            let entry = directory_entry_reader.read_from_reader(reader)?;
            directory.push(entry);
        }
        // Move to the end of the record.
        let expected_end_pos = start_pos + leader.record_length as u64;
        reader.seek(SeekFrom::Start(expected_end_pos))?;
        Ok(DrRecord {
            leader,
            directory,
            fields: Vec::new(),
        })
    }
}

#[derive(Debug)]
pub struct Leader {
    pub record_length: usize,
    pub interchange_level: Option<u8>,
    pub leader_identifier: char,
    pub in_line_code_ext_indicator: char,
    pub version: Option<u8>,
    pub application_indicator: char,
    pub field_control_length: Option<u16>,
    /// Start address of field area (number of bytes in leader and directory)
    pub base_address_of_field_area: usize,
    pub extended_character_set_indicator: [char; 3],
    pub size_of_field_length: u8,
    pub size_of_field_position: u8,
    pub reserved: u8,
    pub size_of_field_tag: u8,
}

impl Leader {
    pub fn is_ddr_leader(&self) -> bool {
        self.interchange_level == Some(3)
            && self.leader_identifier == 'L'
            && self.in_line_code_ext_indicator == 'E'
            && self.version == Some(1)
            && self.application_indicator == ' '
            && self.field_control_length == Some(9)
            && self.extended_character_set_indicator == [' ', '!', ' ']
    }

    pub fn is_dr_leader(&self) -> bool {
        self.interchange_level == None
            && self.leader_identifier == 'D'
            && self.in_line_code_ext_indicator == ' '
            && self.version == None
            && self.application_indicator == ' '
            && self.field_control_length == None
            && self.extended_character_set_indicator == [' ', ' ', ' ']
    }

    pub fn directory_entry_size(&self) -> usize {
        self.size_of_field_length as usize
            + self.size_of_field_position as usize
            + self.size_of_field_tag as usize
    }

    pub fn num_directory_entries(&self) -> usize {
        let directory_bytes = self.base_address_of_field_area - 1 - DDR_LEADER_SIZE;
        directory_bytes / self.directory_entry_size()
    }

    fn directory_entry_reader(&self) -> DirectoryEntryReader {
        DirectoryEntryReader {
            tag_size: self.size_of_field_tag as usize,
            field_length_size: self.size_of_field_length as usize,
            field_position_size: self.size_of_field_position as usize,
        }
    }
}

impl FromFixedLengthBytes for Leader {
    const LENGTH: usize = 24;

    fn from_bytes_unchecked(data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let res = Leader {
            record_length: data[0..5].parse_number()?,
            interchange_level: data.parse_u8_option(5)?,
            leader_identifier: data.read_char(6)?,
            in_line_code_ext_indicator: data.read_char(7)?,
            version: data.parse_u8_option(8)?,
            application_indicator: data.read_char(9)?,
            field_control_length: data[10..=11].parse_number_option()?,
            base_address_of_field_area: data[12..=16].parse_number()?,
            extended_character_set_indicator: data.take_chars(17)?,
            size_of_field_length: data.parse_u8(20)?,
            size_of_field_position: data.parse_u8(21)?,
            reserved: data.parse_u8(22)?,
            size_of_field_tag: data.parse_u8(23)?,
        };
        Ok(res)
    }
}

#[derive(Debug)]
struct DirectoryEntryReader {
    tag_size: usize,
    field_length_size: usize,
    field_position_size: usize,
}
impl ByteReaderWithKnownSize for DirectoryEntryReader {
    type Target = DirectoryEntry;

    fn data_len(&self) -> usize {
        self.tag_size + self.field_length_size + self.field_position_size
    }

    fn read_from_bytes_unchecked(&self, data: &[u8]) -> Result<Self::Target>
    where
        Self: Sized,
    {
        let mut cursor = Cursor::new(data);
        let tag = cursor.read_ascii_string(self.tag_size)?;
        let length = cursor.parse_number(self.field_length_size)?;
        let position = cursor.parse_number(self.field_position_size)?;
        let entry = DirectoryEntry {
            tag,
            length,
            position,
        };
        assert!(!cursor.has_remaining());
        Ok(entry)
    }
}

#[derive(Debug)]
pub struct DirectoryEntry {
    pub tag: String,
    pub length: usize,
    pub position: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub struct FieldControls {
    pub data_structure_code: u8,
    pub data_type_code: u8,
    pub auxiliary_controls: [char; 2],
    pub printable_graphics: [char; 2],
    pub truncated_escape_sequence: [char; 3],
}

impl FromFixedLengthBytes for FieldControls {
    const LENGTH: usize = 9;

    fn from_bytes_unchecked(data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let fc = FieldControls {
            data_structure_code: data.parse_u8(0)?,
            data_type_code: data.parse_u8(1)?,
            auxiliary_controls: data.take_chars(2)?,
            printable_graphics: data.take_chars(4)?,
            truncated_escape_sequence: data.take_chars(6)?,
        };
        Ok(fc)
    }
}

const FIELD_CONTROL_FIELD_FIELD_CONTROLS: FieldControls = FieldControls {
    data_structure_code: 0,
    data_type_code: 0,
    auxiliary_controls: ['0', '0'],
    printable_graphics: [';', '&'],
    truncated_escape_sequence: [' ', ' ', ' '],
};

#[derive(Debug)]
pub enum FieldTree<E> {
    Empty,
    Node {
        tag: String,
        children: Vec<FieldTree<E>>,
        extra: E,
    },
    Leaf {
        tag: String,
        extra: E,
    },
}
impl FieldTree<()> {
    fn insert(&mut self, parent: &str, child: &str) -> bool {
        match self {
            Self::Empty => {
                *self = FieldTree::Node {
                    tag: parent.to_string(),
                    children: vec![FieldTree::Leaf {
                        tag: child.to_string(),
                        extra: (),
                    }],
                    extra: (),
                };
                return true;
            }
            Self::Node {
                tag,
                children,
                extra: _,
            } => {
                if *tag == parent {
                    children.push(Self::Leaf {
                        tag: child.to_string(),
                        extra: (),
                    });
                    return true;
                } else {
                    for c in children {
                        if c.insert(parent, child) {
                            return true;
                        }
                    }
                    // Not in this sub-tree;
                    return false;
                }
            }
            Self::Leaf { tag, extra } => {
                if *tag == parent {
                    let mut new_parent_node = Self::Node {
                        tag: tag.clone(),
                        children: vec![Self::Leaf {
                            tag: child.to_string(),
                            extra: (),
                        }],
                        extra: extra.clone(),
                    };
                    std::mem::swap(self, &mut new_parent_node);
                    return true;
                } else {
                    // Not in this sub-tree.
                    return false;
                }
            }
        };
    }

    #[allow(unused)]
    pub fn parse_from_str(input: &str, tag_length: usize) -> Result<Self> {
        let mut remaining: &str = input;
        let mut tree: Self = Self::Empty;
        while !remaining.is_empty() {
            let (parent, rest) = remaining.split_at(tag_length);
            let (child, rest) = rest.split_at(tag_length);
            remaining = rest;
            ensure_whatever!(
                tree.insert(parent, child),
                "Could not insert {parent} -> {child} into {tree:#?}"
            );
        }
        Ok(tree)
    }
}
impl<E> FieldTree<E>
where
    E: fmt::Display,
{
    pub fn tree_string(&self) -> String {
        self.tree_string_prefixed("".to_string())
    }

    fn tree_string_prefixed(&self, prefix: String) -> String {
        match self {
            Self::Empty => "|".to_string(),
            Self::Leaf { tag, extra } => format!("{prefix}|- {tag}: {extra}"),
            Self::Node {
                tag,
                children,
                extra,
            } => {
                let child_prefix = prefix.clone() + "   ";
                format!(
                    "{prefix}|-{tag}: {extra}\n{}",
                    children
                        .iter()
                        .map(move |child| child.tree_string_prefixed(child_prefix.clone()))
                        .join("\n")
                )
            }
        }
    }
}
impl<E> FieldTree<E> {
    pub fn map_extra<O, F>(self, mapper: F) -> FieldTree<O>
    where
        F: Fn(&str) -> O + Copy,
    {
        match self {
            Self::Empty => FieldTree::Empty,
            Self::Leaf { tag, .. } => {
                let extra = mapper(&tag);
                FieldTree::Leaf { tag, extra }
            }
            Self::Node { tag, children, .. } => {
                let extra = mapper(&tag);
                let new_children = children
                    .into_iter()
                    .map(|child| child.map_extra(mapper))
                    .collect();
                FieldTree::Node {
                    tag,
                    children: new_children,
                    extra,
                }
            }
        }
    }
}
impl<E> fmt::Display for FieldTree<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.tree_string())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FieldControlField {
    field_controls: FieldControls,
    external_file_title: String,
    field_tag_pairs: String,
}

impl FromBytes for FieldControlField {
    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let field_controls = FieldControls::from_reader(reader)?;
        ensure!(
            FIELD_CONTROL_FIELD_FIELD_CONTROLS == field_controls,
            FormatViolationSnafu {
                description: format!(
                    "Illegal field control field field controls: {field_controls:?}"
                )
            }
        );

        let field = FieldControlField {
            field_controls,
            external_file_title: reader.read_string_until_unit_terminator(&DDR_DECODING_CONTEXT)?,
            field_tag_pairs: reader.read_string_until_field_terminator(&DDR_DECODING_CONTEXT)?,
        };
        Ok(field)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct DataDescriptiveField {
    pub field_controls: FieldControls,
    pub field_name: String,
    pub array_descriptor: String,
    pub format_controls: String,
}
impl FromBytes for DataDescriptiveField {
    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + Seek,
        Self: Sized,
    {
        let field_controls = FieldControls::from_reader(reader)?;
        let field = DataDescriptiveField {
            field_controls,
            field_name: reader.read_string_until_unit_terminator(&DDR_DECODING_CONTEXT)?,
            array_descriptor: reader.read_string_until_unit_terminator(&DDR_DECODING_CONTEXT)?,
            format_controls: reader.read_string_until_field_terminator(&DDR_DECODING_CONTEXT)?,
        };
        Ok(field)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_DIRECTORY_FILE: &str = "/Users/lkroll/Programming/Sailing/test-data/20240816_U7Inland_Waddenzee_week 33_NL/ENC_ROOT/catalog.031";
    const TEST_CHART_FILE: &str = "/Users/lkroll/Programming/Sailing/test-data/20240816_U7Inland_Waddenzee_week 33_NL/ENC_ROOT/1R/7/1R7EMS01/1R7EMS01.000";

    #[test]
    fn read_directory() {
        println!("Path: {TEST_DIRECTORY_FILE}");
        let generic_reader = Generic8211FileReader::open(TEST_DIRECTORY_FILE).unwrap();
        let mut num_records: usize = 0;
        for record_result in generic_reader.data_records().unwrap() {
            match record_result {
                Ok(dr_record) => {
                    println!("DR ({num_records}): {dr_record:#?}");
                    num_records += 1;
                }
                Err(e) => eprintln!("Error on record: {e}"),
            }
        }
        assert_eq!(22, num_records);
    }

    #[test]
    fn read_chart() {
        println!("Path: {TEST_CHART_FILE}");
        let generic_reader = Generic8211FileReader::open(TEST_CHART_FILE).unwrap();
        println!("DDR: {:#?}", generic_reader.ddr_record);
        let mut num_records: usize = 0;
        for record_result in generic_reader.data_records().unwrap() {
            match record_result {
                Ok(dr_record) => {
                    println!("DR ({num_records}): {dr_record:#?}");
                    num_records += 1;
                }
                Err(e) => eprintln!("Error on record: {e}"),
            }
        }
        assert_eq!(19604, num_records);
    }

    #[test]
    fn read_field_tree() {
        let test_str = "0001DSIDDSIDDSSI0001DSPM0001VRIDVRIDATTVVRIDVRPCVRIDVRPTVRIDSGCCVRIDSG2DVRIDSG3D0001FRIDFRIDFOIDFRIDATTFFRIDNATFFRIDFFPCFRIDFFPTFRIDFSPCFRIDFSPT";
        let field_tree = FieldTree::parse_from_str(test_str, 4).unwrap();
        println!("{:#?}", field_tree);
        let field_tree_with_sub_fields = field_tree.map_extra(|tag| tag.to_string());
        println!("{}", field_tree_with_sub_fields);

        let generic_reader = Generic8211FileReader::open(TEST_CHART_FILE).unwrap();
        let tag_tree = FieldTree::parse_from_str(
            &generic_reader.ddr_record.control_field.field_tag_pairs,
            generic_reader.ddr_record.leader.size_of_field_tag as usize,
        )
        .unwrap();
        println!("{:#?}", tag_tree);
        let tag_tree_with_sub_fields = tag_tree.map_extra(|tag| {
            if let Some(descriptor) = generic_reader.ddr_record.get_descriptor_with_tag(tag) {
                format!(
                    "{} {}",
                    descriptor.array_descriptor, descriptor.format_controls
                )
            } else {
                "<descriptor not found>".to_string()
            }
        });
        println!("{}", tag_tree_with_sub_fields);
    }
}

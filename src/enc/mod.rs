pub use can2k_uom::static_units::{angle::Degrees, length::Meter};
use itertools::Itertools;
use snafu::{prelude::*, Backtrace};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    io::{prelude::*, Cursor},
    str::FromStr,
    sync::LazyLock,
};

pub mod data_types;
mod reader;
pub use reader::Generic8211FileReader;
pub mod catalog_reader;
pub mod chart_reader;
pub mod data_fields;
pub mod dataset_reader;
pub mod field_tree;

const ATTRIBUTE_VALUE_LOOKUP_CSV: &'static str = include_str!("attribute_value_lookup.csv");
const LABEL_VALUE_LOOKUP_CSV: &'static str = include_str!("label_value_lookup.csv");
const OBJECT_LOOKUP_CSV: &'static str = include_str!("object_lookup.csv");

static ATTRIBUTE_VALUE_LOOKUP: LazyLock<HashMap<&'static str, BTreeMap<u32, &'static str>>> =
    LazyLock::new(parse_attribute_values);

fn attribute_value_lookup(attr_name: &'static str, attr_value: &str) -> Result<&'static str> {
    let lookup_table =
        ATTRIBUTE_VALUE_LOOKUP
            .get(attr_name)
            .with_context(|| FormatViolationSnafu {
                description: format!("Could not find lookup table for {attr_name}"),
            })?;
    // Try to parse the value.
    let code: u32 = attr_value
        .parse()
        .with_whatever_context::<_, _, EncError>(|_| {
            format!("Could not parse {attr_value} to u32.")
        })?;
    lookup_table.get(&code).map(|v| *v).with_context(|| FormatViolationSnafu{description: format!("Could not find an attribute value for attr={attr_name} and code={code} in lookup table: {lookup_table:#?}")})
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttributeType {
    Enumerated,
    List,
    Float,
    Integer,
    CodedString,
    FreeText,
    Unknown,
}
impl From<char> for AttributeType {
    fn from(c: char) -> Self {
        match c {
            'E' => Self::Enumerated,
            'L' => Self::List,
            'F' => Self::Float,
            'I' => Self::Integer,
            'A' => Self::CodedString,
            'S' => Self::FreeText,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttributeLabel {
    pub code: u16,
    pub name: &'static str,
    pub acronym: &'static str,
    pub attr_type: AttributeType,
    pub class: char,
}
impl fmt::Display for AttributeLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.name, self.acronym)
    }
}
impl Default for AttributeLabel {
    fn default() -> Self {
        Self {
            code: 0,
            name: "Missing Attribute",
            acronym: "MISSNG",
            attr_type: AttributeType::Unknown,
            class: '?',
        }
    }
}

static LABEL_VALUE_LOOKUP: LazyLock<BTreeMap<u16, AttributeLabel>> =
    LazyLock::new(parse_attribute_labels);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ObjectClass {
    pub code: u16,
    pub name: &'static str,
    pub acronym: &'static str,
    pub attributes_a: &'static str,
    pub attributes_b: &'static str,
    pub attributes_c: &'static str,
    pub class: char,
    pub primitives: &'static str,
}
impl fmt::Display for ObjectClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.name, self.acronym)
    }
}
impl Default for ObjectClass {
    fn default() -> Self {
        MISSING_OBJECT
    }
}
impl Default for &ObjectClass {
    fn default() -> Self {
        &MISSING_OBJECT
    }
}
const MISSING_OBJECT: ObjectClass = ObjectClass {
    code: 0,
    name: "Missing Object",
    acronym: "MISSNG",
    attributes_a: "",
    attributes_b: "",
    attributes_c: "",
    class: '?',
    primitives: "",
};

static OBJECT_LOOKUP: LazyLock<BTreeMap<u16, ObjectClass>> = LazyLock::new(parse_objects);

fn parse_attribute_values() -> HashMap<&'static str, BTreeMap<u32, &'static str>> {
    let mut tags: HashMap<&'static str, BTreeMap<u32, &'static str>> = HashMap::new();
    for line in ATTRIBUTE_VALUE_LOOKUP_CSV.lines().skip(1) {
        let comma_pos = line.find(',').expect("every line contains a comma");
        let (tag, mut rest) = line.split_at(comma_pos);
        // Get rid of the leading comma.
        rest = &rest[1..];
        // let mut values = BTreeMap::new();
        let entries: Vec<&'static str> = rest.split(';').collect();
        let values: BTreeMap<u32, &'static str> = entries
            .chunks(2)
            .map(|chunk| {
                // println!("Chunk: {chunk:?}");
                (chunk[0].parse().expect("number"), chunk[1])
            })
            .collect();

        #[cfg(test)]
        {
            let upper_case_tag = tag.to_ascii_uppercase();
            if let Some(existing) = tags.get(upper_case_tag.as_str()) {
                let left_diff = {
                    let mut diff: BTreeMap<u32, &'static str> = existing.clone();
                    for value in values.iter() {
                        diff.remove(value.0);
                    }
                    diff
                };
                let right_diff = {
                    let mut diff: BTreeMap<u32, &'static str> = values.clone();
                    for value in existing.iter() {
                        diff.remove(value.0);
                    }
                    diff
                };
                assert!(
                false,
                "There should not be duplicate entries, but got {:#?} for {}, {:#?}:\nLeft: {:#?}\nRight: {:#?}",
                existing,
                tag,
                values,
                left_diff,
                right_diff
            );
            }

            // Just disallow this, since it leads to ambiguity.
            debug_assert!(tag == upper_case_tag, "Only use upper case acronyms: {tag}");
        }

        tags.insert(tag, values);
    }
    tags
}

fn parse_attribute_labels() -> BTreeMap<u16, AttributeLabel> {
    let mut labels = BTreeMap::new();
    for line in LABEL_VALUE_LOOKUP_CSV.lines().skip(1) {
        let entries: Vec<&'static str> = line.split(',').collect();
        let value = AttributeLabel {
            code: entries[0].parse().unwrap(),
            name: entries[1],
            acronym: entries[2],
            attr_type: entries[3].chars().next().unwrap().into(),
            class: entries[4].chars().next().unwrap(),
        };
        // Just disallow this, since it leads to ambiguity.
        debug_assert!(
            value.acronym == value.acronym.to_ascii_uppercase(),
            "Only use upper case acronyms"
        );
        labels.insert(value.code, value);
    }
    labels
}

fn parse_objects() -> BTreeMap<u16, ObjectClass> {
    let mut objects = BTreeMap::new();
    for entries in parse_csv(OBJECT_LOOKUP_CSV.lines().skip(1)).into_iter() {
        assert!(entries.len() == 8, "Wrong number of columns: {entries:#?}");
        let code = entries[0].parse().unwrap();
        let value = ObjectClass {
            code,
            name: entries[1],
            acronym: entries[2],
            attributes_a: entries[3],
            attributes_b: entries[4],
            attributes_c: entries[5],
            class: entries[6].chars().next().unwrap_or(' '),
            primitives: entries[7],
        };
        let existing = objects.insert(value.code, value);
        assert!(
            existing.is_none(),
            "Duplicate object class value for code {} ({})",
            code,
            entries[0]
        );
    }
    objects
}

fn parse_csv(data: impl Iterator<Item = &'static str>) -> Vec<Vec<&'static str>> {
    let mut rows = Vec::new();

    for line in data {
        let mut row: Vec<&'static str> = Vec::new();
        let mut current_field_start = 0usize;
        let mut current_field_end = 0usize;
        // This is usually the same as current_field_end + 1, but for quoted fields we skip the final quote.
        let mut next_field_start = 0usize;
        let mut in_quotes = false;

        for c in line.chars() {
            // println!("c='{c}', current_field='{}', next_field_start={next_field_start}, in_quotes={in_quotes}, row={row:?}", &line[current_field_start..current_field_end]);
            match c {
                // Handle quotes
                '"' => {
                    if in_quotes {
                        // Closing quote.
                        in_quotes = false;
                        // Don't move current_field_end to skip the quote.
                        next_field_start += 1;
                    } else {
                        // Starting a quoted field
                        in_quotes = true;
                        // Skip the starting quote.
                        current_field_start += 1;
                        current_field_end += 1;
                        next_field_start += 1;
                    }
                }
                // Handle comma separators (only if we're not in quotes)
                ',' if !in_quotes => {
                    // Push the completed field
                    row.push(&line[current_field_start..current_field_end]);
                    // Start a new field.
                    next_field_start += 1;
                    current_field_start = next_field_start;
                    current_field_end = current_field_start;
                }
                // Add characters to the current field
                _ => {
                    current_field_end += 1;
                    next_field_start += 1;
                }
            }
        }
        // Push the final field in the row
        row.push(&line[current_field_start..current_field_end]);
        rows.push(row);
    }

    rows
}

/// We return coordinates in this format.
pub type GeoPoint = can2k_geo::points::GeoPoint<Degrees<f64>>;

/// A depth sounding at the given `location`.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct Sounding {
    pub location: GeoPoint,
    pub depth: Meter<f64>,
}
impl fmt::Display for Sounding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.depth, self.location)
    }
}

const SPACE: u8 = b' ';

const CONTROL_FIELD_TAG: &str = "0000";

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum TerminatorValue {
    SingleByte(u8),
    TwoByte([u8; 2]),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum LexicalLevel {
    /// Corresponds to ASCII
    Level0,
    /// Corresponds to Latin 1 - ISO 8859
    Level1,
    /// Corresponds to UTF-16 (I think)
    Level2,
}
impl LexicalLevel {
    const UNIT_TERMINATOR: u8 = 0x1F;
    const FIELD_TERMINATOR: u8 = 0x1E;
    #[allow(unused)]
    const NULL: u8 = 0x00;

    fn for_character_set_indicator(input: [char; 3]) -> Result<Self> {
        match input {
            [' ', ' ', ' '] => Ok(Self::Level0),
            ['-', 'A', ' '] => Ok(Self::Level1),
            ['%', '/', 'A'] => Ok(Self::Level2),
            _ => FormatViolationSnafu {
                description: format!("Invalid character set indicator: {:?}", input),
            }
            .fail(),
        }
    }

    fn unit_terminator(&self) -> TerminatorValue {
        match self {
            Self::Level0 => TerminatorValue::SingleByte(Self::UNIT_TERMINATOR),
            Self::Level1 => TerminatorValue::SingleByte(Self::UNIT_TERMINATOR),
            Self::Level2 => TerminatorValue::TwoByte([0, Self::UNIT_TERMINATOR]),
        }
    }
    fn field_terminator(&self) -> TerminatorValue {
        match self {
            Self::Level0 => TerminatorValue::SingleByte(Self::FIELD_TERMINATOR),
            Self::Level1 => TerminatorValue::SingleByte(Self::FIELD_TERMINATOR),
            Self::Level2 => TerminatorValue::TwoByte([0, Self::FIELD_TERMINATOR]),
        }
    }
}
impl Default for LexicalLevel {
    fn default() -> Self {
        Self::Level0
    }
}

#[derive(Clone, Debug, Default)]
struct DecodingContext {
    lexical_level: LexicalLevel,
}
impl DecodingContext {
    fn with_lexical_level(self, lexical_level: LexicalLevel) -> Self {
        Self {
            lexical_level,
            ..self
        }
    }

    fn bytes_to_string(&self, bytes: Vec<u8>) -> Result<String> {
        match self.lexical_level {
            LexicalLevel::Level0 => {
                // Read ASCII as UTF-8
                String::from_utf8(bytes)
                    .context(FromUtf8Snafu)
                    .context(StringConversionSnafu)
            }
            LexicalLevel::Level1 => {
                let (cow, _, had_errors) = encoding_rs::WINDOWS_1252.decode(&bytes);
                if had_errors {
                    whatever!("Invalid WINDOWS_1252 string: {:?}", bytes);
                } else {
                    Ok(cow.to_string())
                }
            }
            LexicalLevel::Level2 => {
                let two_byte_buffer: Vec<u16> = bytes
                    .chunks_exact(2) // Create chunks of 2 bytes
                    .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]])) // Combine as little-endian u16
                    .collect();
                String::from_utf16(&two_byte_buffer)
                    .context(FromUtf16Snafu)
                    .context(StringConversionSnafu)
            }
        }
    }
}
const DDR_DECODING_CONTEXT: DecodingContext = DecodingContext {
    lexical_level: LexicalLevel::Level0,
};

trait PeekableBufRead {
    /// Peeks at the next byte without consuming it.
    fn peek_byte(&mut self) -> Result<Option<u8>>;

    fn peek_is_field_terminator(&mut self, ctx: &DecodingContext) -> Result<bool>;
}

impl<T: BufRead> PeekableBufRead for T {
    fn peek_byte(&mut self) -> Result<Option<u8>> {
        let buffer = self.fill_buf().context(IoSnafu)?;
        Ok(buffer.get(0).copied()) // Returns the first byte, if available, without consuming
    }

    fn peek_is_field_terminator(&mut self, ctx: &DecodingContext) -> Result<bool> {
        let buffer = self.fill_buf().context(IoSnafu)?;
        match ctx.lexical_level.field_terminator() {
            TerminatorValue::SingleByte(t) => match buffer.get(0) {
                Some(b) => Ok(*b == t),
                None => FormatViolationSnafu {
                    description: "Expected more characters but got empty buffer".to_string(),
                }
                .fail(),
            },
            TerminatorValue::TwoByte(chars) => {
                ensure!(
                    buffer.len() >= 2,
                    FormatViolationSnafu {
                        description: format!(
                            "Expected 2 or more characters but got buffer: {buffer:?}"
                        )
                    }
                );
                let slice = &buffer[..2];
                Ok(chars == slice)
            }
        }
    }
}

trait FromBytes {
    #[allow(unused)]
    fn from_bytes(data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let mut cursor = Cursor::new(data);
        Self::from_reader(&mut cursor)
    }

    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + Seek,
        Self: Sized;
}

trait FromFixedLengthBytes {
    const LENGTH: usize;

    /// `data` *must* be of `LENGTH`
    fn from_bytes_unchecked(data: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

impl<T> FromBytes for T
where
    T: FromFixedLengthBytes,
{
    fn from_bytes(data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        assert_eq!(Self::LENGTH, data.len());
        Self::from_bytes_unchecked(data)
    }

    fn from_reader<R>(reader: &mut R) -> Result<Self>
    where
        R: BufRead + ?Sized,
        Self: Sized,
    {
        let mut buffer = vec![0u8; Self::LENGTH];
        reader.read_exact(&mut buffer)?;
        Self::from_bytes_unchecked(&buffer)
    }
}

trait FromByteOpt: Sized {
    fn from_byte(value: u8) -> Result<Self>;
}

impl<T> FromFixedLengthBytes for Option<T>
where
    T: FromByteOpt,
{
    const LENGTH: usize = 1;

    fn from_bytes_unchecked(data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        match data[0] {
            255 => Ok(None),
            b => T::from_byte(b).map(Some),
        }
    }
}

trait ByteReader {
    type Target;

    #[allow(unused)]
    fn read_from_bytes(&self, data: &[u8]) -> Result<Self::Target>
    where
        Self: Sized,
    {
        let mut cursor = Cursor::new(data);
        self.read_from_reader(&mut cursor)
    }

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead + Seek,
        Self: Sized;
}

trait ByteReaderWithKnownSize {
    type Target;

    fn data_len(&self) -> usize;

    /// `data` *must* be of `self.data_len()`
    fn read_from_bytes_unchecked(&self, data: &[u8]) -> Result<Self::Target>
    where
        Self: Sized;
}

impl<T> ByteReader for T
where
    T: ByteReaderWithKnownSize,
{
    type Target = <Self as ByteReaderWithKnownSize>::Target;

    fn read_from_bytes(&self, data: &[u8]) -> Result<Self::Target>
    where
        Self: Sized,
    {
        assert_eq!(self.data_len(), data.len());
        self.read_from_bytes_unchecked(data)
    }

    fn read_from_reader<R>(&self, reader: &mut R) -> Result<Self::Target>
    where
        R: BufRead,
        Self: Sized,
    {
        let mut buffer = vec![0u8; self.data_len()];
        reader.read_exact(&mut buffer)?;
        self.read_from_bytes_unchecked(&buffer)
    }
}

// TODO: Later, maybe.
// // This is the public facing error type.
// #[derive(Debug, Snafu)]
// pub struct Error(EncError);

pub type Result<T> = std::result::Result<T, EncError>;

#[derive(Debug, Snafu)]
pub enum EncError {
    #[snafu(display("Format spec was violated: {description}"))]
    FormatViolation {
        description: String,
        backtrace: Backtrace,
    },
    #[snafu(display("This feature is not supported (yet): {description}"))]
    Unsupported {
        /// What is the thing that's unsupported?
        description: &'static str,
        backtrace: Backtrace,
    },
    #[snafu(display(
        "A record that was referenced as {pointer} could not be found during pointer resolution"
    ))]
    FailedLookup {
        pointer: data_types::RecordName,
        backtrace: Backtrace,
    },
    #[snafu(display("Error converting slice into array: {source}"))]
    Slicing {
        source: std::array::TryFromSliceError,
        backtrace: Backtrace,
    },
    #[snafu(display("Error converting bytes into a string: {source}"))]
    StringConversion {
        source: StringConversionError,
        backtrace: Backtrace,
    },
    #[snafu(display("Error reading bytes: {source}"))]
    Io {
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("{source}\nContext: {context}"))]
    WithExtraContext {
        #[snafu(backtrace)]
        source: Box<EncError>,
        context: String,
    },
    #[snafu(whatever, display("{message}"))]
    Other {
        message: String,
        // Having a `source` is optional, but if it is present, it must
        // have this specific attribute and type:
        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
        backtrace: Backtrace,
    },
}
impl EncError {
    fn assert(
        invariant_satisfied: impl FnOnce() -> bool,
        error_msg: impl FnOnce() -> String,
    ) -> Result<()> {
        // if cfg!(debug_assertions) {
        //     assert!(invariant_satisfied(), "{}", error_msg());
        //     Ok(())
        // } else {
        if invariant_satisfied() {
            Ok(())
        } else {
            FormatViolationSnafu {
                description: error_msg(),
            }
            .fail()
        }
        // }
    }

    fn pointer_lookup_failed(pointer: data_types::RecordName) -> Self {
        FailedLookupSnafu { pointer }.build()
    }
}

#[derive(Debug, Snafu)]
pub enum StringConversionError {
    FromUtf8 { source: std::string::FromUtf8Error },
    FromUtf16 { source: std::string::FromUtf16Error },
    Utf8 { source: std::str::Utf8Error },
}

impl From<std::io::Error> for EncError {
    fn from(source: std::io::Error) -> Self {
        EncError::Io {
            source,
            backtrace: Backtrace::capture(),
        }
    }
}

trait EncSliceExt {
    fn parse_u8(&self, pos: usize) -> Result<u8>;

    /// Same as [[parse_u8]] but treat the space character as None
    fn parse_u8_option(&self, pos: usize) -> Result<Option<u8>>;

    fn parse_number<N>(&self) -> Result<N>
    where
        N: FromStr,
        N::Err: snafu::Error + 'static;

    /// Same as [[parse_number]] but treat the space character as None
    fn parse_number_option<N>(&self) -> Result<Option<N>>
    where
        N: FromStr,
        N::Err: snafu::Error + 'static;

    fn take_chars<const NUM: usize>(&self, offset: usize) -> Result<[char; NUM]>;

    fn read_char(&self, pos: usize) -> Result<char>;
}

impl EncSliceExt for [u8] {
    fn parse_number<N>(&self) -> Result<N>
    where
        N: FromStr,
        N::Err: snafu::Error + 'static,
    {
        let number_str = std::str::from_utf8(self)
            .context(Utf8Snafu)
            .context(StringConversionSnafu)?;
        let res = number_str.parse();
        let res = whatever!(res, "Number parsing issue for string '{number_str}'");
        Ok(res)
    }

    fn parse_number_option<N>(&self) -> Result<Option<N>>
    where
        N: FromStr,
        N::Err: snafu::Error + 'static,
    {
        if self.iter().all(|c| c == &SPACE) {
            return Ok(None);
        }
        let number_str = std::str::from_utf8(self)
            .context(Utf8Snafu)
            .context(StringConversionSnafu)?;
        let res = number_str.parse().map(Some);
        let res = whatever!(res, "Number parsing issue for string '{number_str}'");
        Ok(res)
    }

    fn parse_u8(&self, pos: usize) -> Result<u8> {
        self[pos..=pos].parse_number()
    }

    fn parse_u8_option(&self, pos: usize) -> Result<Option<u8>> {
        self[pos..=pos].parse_number_option()
    }

    fn take_chars<const NUM: usize>(&self, offset: usize) -> Result<[char; NUM]> {
        {
            let bytes: [u8; NUM] = self[offset..(offset + NUM)]
                .try_into()
                .context(SlicingSnafu)?;
            let chars = bytes.map(char::from);
            Ok(chars)
        }
    }

    fn read_char(&self, pos: usize) -> Result<char> {
        Ok(char::from(self[pos]))
    }
}

trait EncReaderExt {
    fn read_bytes_until_terminator(&mut self, terminator: TerminatorValue) -> Result<Vec<u8>>;
    fn read_string_until_unit_terminator(&mut self, ctx: &DecodingContext) -> Result<String>;
    fn read_string_until_field_terminator(&mut self, ctx: &DecodingContext) -> Result<String>;
    //fn read_string(&mut self, num_bytes: usize, ctx: &DecodingContext) -> Result<String>;
    fn read_ascii_string(&mut self, num_bytes: usize) -> Result<String>;
    //fn read_chars<const NUM: usize>(&mut self) -> Result<[char; NUM]>;
    fn parse_number<N>(&mut self, num_bytes: usize) -> Result<N>
    where
        N: FromStr,
        N::Err: snafu::Error + 'static;
    fn parse_number_until_unit_terminator<N>(&mut self, ctx: &DecodingContext) -> Result<N>
    where
        N: FromStr,
        N::Err: snafu::Error + 'static;

    /// Ensure that the next consumed byte is `value`.
    fn expect_u8(&mut self, value: u8) -> Result<()>;

    /// Ensure the next byte (or two) consumed are the field terminator.
    fn expect_field_terminator(&mut self, ctx: &DecodingContext) -> Result<()>;

    /// Ensure that the next bytes correspond to `expected` and consume them.
    fn expect_bytes(&mut self, expected: &[u8]) -> Result<()>;
    /// Ensure the next bytes correspond to `expected` and consume them.
    fn expect_ascii_str(&mut self, expected: &str) -> Result<()>;
}

impl<R: BufRead> EncReaderExt for R {
    fn read_bytes_until_terminator(&mut self, terminator: TerminatorValue) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        match terminator {
            TerminatorValue::SingleByte(t) => {
                self.read_until(t, &mut buffer)?;
                ensure!(
                    buffer.last() == Some(&t),
                    FormatViolationSnafu {
                        description: format!("Expected '{t}' in final position but got {buffer:?}")
                    }
                );
                buffer.pop(); // Get rid of the terminator.
            }
            TerminatorValue::TwoByte([first, second]) => 'read_until_loop: loop {
                self.read_until(first, &mut buffer)?;
                ensure!(
                    buffer.last() == Some(&first),
                    FormatViolationSnafu {
                        description: format!("Expected '>{first}, {second}' in final position but got EOF at {buffer:?}")
                    }
                );
                match self.peek_byte()? {
                    Some(c) => {
                        if c == second {
                            buffer.pop();
                            break 'read_until_loop;
                        } else {
                            buffer.push(c);
                            continue 'read_until_loop;
                        }
                    }
                    None => {
                        return FormatViolationSnafu {
                            description: format!("Expected '{first}, >{second}' in final position but got EOF at {buffer:?}")
                        }.fail();
                    }
                }
            },
        }
        Ok(buffer)
    }

    fn read_string_until_unit_terminator(&mut self, ctx: &DecodingContext) -> Result<String> {
        let bytes = self.read_bytes_until_terminator(ctx.lexical_level.unit_terminator())?;
        ctx.bytes_to_string(bytes)
    }
    fn read_string_until_field_terminator(&mut self, ctx: &DecodingContext) -> Result<String> {
        let bytes = self.read_bytes_until_terminator(ctx.lexical_level.field_terminator())?;
        ctx.bytes_to_string(bytes)
    }
    fn read_ascii_string(&mut self, num_bytes: usize) -> Result<String> {
        let mut buffer = vec![0u8; num_bytes];
        self.read_exact(&mut buffer)?;
        DDR_DECODING_CONTEXT.bytes_to_string(buffer)
    }
    // fn read_chars<const NUM: usize>(&mut self) -> Result<[char; NUM]> {
    //     let mut buffer: [u8; NUM] = [0u8; NUM];
    //     self.read_exact(&mut buffer)?;
    //     Ok(buffer.map(char::from))
    // }

    fn parse_number<N>(&mut self, num_bytes: usize) -> Result<N>
    where
        N: FromStr,
        N::Err: snafu::Error + 'static,
    {
        let mut buffer = vec![0u8; num_bytes];
        self.read_exact(&mut buffer)?;
        let number_str = std::str::from_utf8(&buffer)
            .context(Utf8Snafu)
            .context(StringConversionSnafu)?;
        let res = number_str.parse();
        let res = whatever!(res, "Number parsing issue for string '{number_str}'");
        Ok(res)
    }

    fn parse_number_until_unit_terminator<N>(&mut self, ctx: &DecodingContext) -> Result<N>
    where
        N: FromStr,
        N::Err: snafu::Error + 'static,
    {
        let number_str = self.read_string_until_unit_terminator(ctx)?;
        let res = number_str.parse();
        let res = whatever!(res, "Number parsing issue for string '{number_str}'");
        Ok(res)
    }

    fn expect_u8(&mut self, value: u8) -> Result<()> {
        let mut buffer = [0u8; 1];
        self.read_exact(&mut buffer)?;
        ensure!(
            buffer[0] == value,
            FormatViolationSnafu {
                description: format!("Expected {value} but got {}", buffer[0])
            }
        );
        Ok(())
    }

    fn expect_bytes(&mut self, expected: &[u8]) -> Result<()> {
        let mut buffer = vec![0u8; expected.len()];
        self.read_exact(&mut buffer)?;
        ensure!(
            &buffer == expected,
            FormatViolationSnafu {
                description: format!("Expected '{expected:?}' but got '{buffer:?}'")
            }
        );
        Ok(())
    }

    fn expect_field_terminator(&mut self, ctx: &DecodingContext) -> Result<()> {
        match ctx.lexical_level.field_terminator() {
            TerminatorValue::SingleByte(t) => self.expect_u8(t),
            TerminatorValue::TwoByte(t) => self.expect_bytes(&t),
        }
    }

    fn expect_ascii_str(&mut self, expected: &str) -> Result<()> {
        let mut buffer = vec![0u8; expected.len()];
        self.read_exact(&mut buffer)?;
        let read_str = std::str::from_utf8(&buffer)
            .context(Utf8Snafu)
            .context(StringConversionSnafu)?;
        ensure!(
            read_str == expected,
            FormatViolationSnafu {
                description: format!("Expected '{expected}' but got {read_str}")
            }
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) const TEST_DATA_ROOT: &str = "tests/data/S57";
    pub(super) const TEST_DATA_SET: &str =
        "tests/data/S57/20240816_U7Inland_Waddenzee_week 33_NL/ENC_ROOT";
    pub(super) const TEST_CATALOG_FILE: &str =
        "tests/data/S57/20240816_U7Inland_Waddenzee_week 33_NL/ENC_ROOT/catalog.031";
    pub(super) const TEST_CHART_FILE: &str =
        "tests/data/S57/20240816_U7Inland_Waddenzee_week 33_NL/ENC_ROOT/1R/7/1R7EMS01/1R7EMS01.000";
    pub(super) const TEST_CHART_FILE2: &str =
        "tests/data/S57/20240816_U7Inland_Waddenzee_week 33_NL/ENC_ROOT/1R/7/1R7EMS04/1R7EMS04.000";

    #[test]
    fn read_attribute_values() {
        // println!("Raw: {ATTRIBUTE_VALUE_LOOKUP_CSV}");
        // println!("Parsed: {:#?}", *ATTRIBUTE_VALUE_LOOKUP);
        assert_eq!((*ATTRIBUTE_VALUE_LOOKUP).len(), 124);
        assert_eq!((*LABEL_VALUE_LOOKUP).len(), 309);
        assert_eq!((*OBJECT_LOOKUP).len(), 250);
    }

    #[test]
    fn test_non_utf8() {
        const DATA: [u8; 58] = [
            83, 95, 68, 32, 45, 32, 50, 55, 54, 56, 52, 32, 45, 32, 78, 78, 95, 80, 97, 97, 112,
            115, 97, 110, 100, 32, 83, 252, 100, 95, 50, 48, 50, 52, 48, 52, 48, 52, 32, 45, 32,
            79, 110, 100, 105, 101, 112, 115, 116, 101, 32, 40, 50, 53, 47, 50, 53, 41,
        ];
        let (cow, _, had_errors) = encoding_rs::WINDOWS_1252.decode(&DATA);
        assert!(!had_errors);
        assert_eq!(
            "S_D - 27684 - NN_Paapsand Süd_20240404 - Ondiepste (25/25)",
            cow.to_string()
        );
    }
}

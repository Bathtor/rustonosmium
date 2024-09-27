use super::*;

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

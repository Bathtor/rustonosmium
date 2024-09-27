use crate::fail;

use super::*;
use data_fields::CatalogDirectoryField;
use itertools::Itertools;
use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};

const CATALOG_RECORD_TAG: &str = "CATD";

pub struct CatalogReader {
    generic_reader: Generic8211FileReader,
}

impl CatalogReader {
    pub fn with(generic_reader: Generic8211FileReader) -> Result<Self> {
        EncError::assert(
            || {
                generic_reader
                    .ddr_record
                    .directory
                    .iter()
                    .any(|entry| entry.tag == CATALOG_RECORD_TAG)
            },
            || {
                format!(
                    "Expected entries with tag '{CATALOG_RECORD_TAG}' but found none in [{}]",
                    generic_reader
                        .ddr_record
                        .directory
                        .iter()
                        .map(|e| &e.tag)
                        .join(", ")
                )
            },
        )?;
        Ok(Self { generic_reader })
    }

    pub fn files_in_catalog(&self) -> Result<Vec<CatalogFile>> {
        let mut files = Vec::new();
        let parent_path = self
            .generic_reader
            .file_name
            .parent()
            .with_whatever_context::<_, _, EncError>(|| {
                format!(
                    "Expected directory path to be absolute but was {}",
                    self.generic_reader.file_name.display()
                )
            })?;
        for res in self.generic_reader.data_records()? {
            let record = res?;
            match record.take_field_with_tag(CATALOG_RECORD_TAG) {
                Some(DataField::CatalogDirectory(c)) => {
                    files.push(CatalogFile::with_absolute_path(parent_path, *c));
                }
                Some(f) => {
                    fail!(FormatViolationSnafu {
                        description: format!("Expected CatalogDirectoryField but got {f:?}")
                    });
                }
                None => (), // skip this field that doesn't have the tag.
            }
        }
        Ok(files)
    }

    pub fn chart_files_in_catalog(&self) -> Result<Vec<CatalogFile>> {
        let chart_files = self
            .files_in_catalog()?
            .into_iter()
            .filter(|f| f.path.extension().and_then(OsStr::to_str) == Some("000"))
            .collect();
        Ok(chart_files)
    }
}

#[derive(Debug)]
pub struct CatalogFile {
    /// A string indicating the absolute path to a valid file name.
    pub path: PathBuf,
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
    /// A string of characters.
    pub comment: String,
}
impl CatalogFile {
    fn with_absolute_path(parent: &Path, value: CatalogDirectoryField) -> Self {
        // TODO: No idea how this handles special characters, I'd need an example file, the spec is arcane.
        let path_str = value.file_name.replace("\\", std::path::MAIN_SEPARATOR_STR);
        CatalogFile {
            path: parent.join(PathBuf::from(path_str)),
            implementation: value.implementation,
            south_lat: value.south_lat,
            west_lon: value.west_lon,
            north_lat: value.north_lat,
            east_lon: value.east_lon,
            comment: value.comment,
        }
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
        let catalog_reader = CatalogReader::with(generic_reader).unwrap();
        let mut num_files: usize = 0;
        for file in catalog_reader.files_in_catalog().unwrap() {
            println!("File: {file:?}");
            num_files += 1;
        }
        assert_eq!(22, num_files);

        let mut num_chart_files: usize = 0;
        for file in catalog_reader.chart_files_in_catalog().unwrap() {
            println!("Chart File: {file:?}");
            num_chart_files += 1;
        }
        assert_eq!(20, num_chart_files);
    }

    #[test]
    fn fail_on_wrong_file_type() {
        let generic_reader = Generic8211FileReader::open(TEST_CHART_FILE).unwrap();
        let catalog_reader_res = CatalogReader::with(generic_reader);
        assert!(
            catalog_reader_res.is_err(),
            "Did not fail to produce a reader"
        );
    }
}

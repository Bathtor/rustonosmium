use super::*;
use catalog_reader::{CatalogFile, CatalogReader};
use std::{
    fs,
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

/// Traverses `path` and tries to read every folder called `ENC_ROOT` as its own dataset.
pub fn discover_datasets<P: AsRef<Path>>(path: P) -> Result<Vec<DatasetReader>> {
    let mut enc_roots = Vec::new();
    let mut it = WalkDir::new(path).follow_links(true).into_iter();
    //.filter_map(|e| e.ok());
    while let Some(res) = it.next() {
        if let Ok(entry) = res {
            let path = entry.path();

            // Check if it's a directory and named "ENC_ROOT"
            if path.is_dir() && path.file_name().map_or(false, |name| name == "ENC_ROOT") {
                println!("Found ENC_ROOT at: {:?}", path);
                it.skip_current_dir(); // No need to traverse further into this looking for more.
                enc_roots.push(path.to_path_buf());
            } else {
                println!("No root at: {:?}", path);
            }
        }
    }
    let datasets = enc_roots
        .into_iter()
        .flat_map(|root| match DatasetReader::open(&root) {
            Ok(dataset) => Some(dataset),
            Err(e) => {
                eprintln!("{root:?} was not a valid dataset: {e}");
                None
            }
        })
        .collect();
    Ok(datasets)
}

/// Provides access to a S-57 dataset with a single "ENC_ROOT" directory
///  and "catalog.031" file within.
pub struct DatasetReader {
    /// The
    pub root: PathBuf,
    catalog_reader: CatalogReader,
}
impl DatasetReader {
    /// Open `path` and try to read the catalog.
    ///
    /// The `path` should point to a "ENC_ROOT" directory.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let root = fs::canonicalize(path.as_ref())?;
        let catalog_path = root.join("catalog.031");
        let generic_reader = Generic8211FileReader::open(catalog_path)?;
        let catalog_reader = CatalogReader::with(generic_reader)?;
        Ok(Self {
            root,
            catalog_reader,
        })
    }

    /// Return descriptions of all the files listed in the catalog of this dataset.
    pub fn files_in_catalog(&self) -> Result<Vec<CatalogFile>> {
        self.catalog_reader.files_in_catalog()
    }

    /// Return descriptions of only the chart files listed in the catalog of this dataset.
    pub fn chart_files_in_catalog(&self) -> Result<Vec<CatalogFile>> {
        self.catalog_reader.chart_files_in_catalog()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::ProductSpecification;
    use snafu::ErrorCompat;
    use std::time::Instant;

    #[test]
    fn read_dataset() {
        let path = crate::enc::tests::TEST_DATA_SET;
        println!("Path: {path}");
        let dataset_reader = DatasetReader::open(path).expect("dataset");
        let all_files = dataset_reader.files_in_catalog().expect("files");
        assert_eq!(22, all_files.len());
        let chart_files = dataset_reader
            .chart_files_in_catalog()
            .expect("chart files");
        assert_eq!(20, chart_files.len());

        for chart_file in dataset_reader.chart_files_in_catalog().unwrap() {
            println!("Reading file: {:?}", chart_file.path);
            let mut chart_reader = chart_file.chart_reader().expect("chart reader");
            let start = Instant::now();
            let metadata = chart_reader.metadata().expect("metadata");
            assert_eq!(
                metadata.identification.product_specification,
                ProductSpecification::ElectronicNavigationalChart,
                "Chart reader was not reading a chart at:\nCatalog Entry\n{:#?}\nMetadata\n{:#?}",
                chart_file,
                metadata
            );
            let mut successes = 0usize;
            let mut failures = 0usize;
            for feature in chart_reader
                .feature_records_resolved()
                .expect("resolved records")
            {
                match feature {
                    Ok(_) => {
                        successes += 1;
                    }
                    Err(e) => {
                        eprintln!(
                            "Error resolving feature in file {:?}: {}",
                            chart_file.path, e
                        );
                        if let Some(backtrace) = e.backtrace() {
                            eprintln!("Backtrace: {}", backtrace);
                        }
                        failures += 1;
                    }
                }
            }
            assert_eq!(failures, 0);
            let time = start.elapsed();
            println!("Read {successes} features in {}ms.", time.as_millis());
        }
    }

    #[test]
    fn read_discovered_datasets() {
        let path = crate::enc::tests::TEST_DATA_ROOT;
        println!("Path: {path}");
        let datasets = discover_datasets(path).expect("datasets");
        for dataset_reader in datasets {
            let all_files = dataset_reader.files_in_catalog().expect("files");
            assert_eq!(22, all_files.len());
            let chart_files = dataset_reader
                .chart_files_in_catalog()
                .expect("chart files");
            assert_eq!(20, chart_files.len());

            for chart_file in dataset_reader.chart_files_in_catalog().unwrap() {
                println!("Reading file: {:?}", chart_file.path);
                let mut chart_reader = chart_file.chart_reader().expect("chart reader");
                let start = Instant::now();
                let metadata = chart_reader.metadata().expect("metadata");
                assert_eq!(
                metadata.identification.product_specification,
                ProductSpecification::ElectronicNavigationalChart,
                "Chart reader was not reading a chart at:\nCatalog Entry\n{:#?}\nMetadata\n{:#?}",
                chart_file,
                metadata
            );
                let mut successes = 0usize;
                let mut failures = 0usize;
                for feature in chart_reader
                    .feature_records_resolved()
                    .expect("resolved records")
                {
                    match feature {
                        Ok(_) => {
                            successes += 1;
                        }
                        Err(e) => {
                            eprintln!(
                                "Error resolving feature in file {:?}: {}",
                                chart_file.path, e
                            );
                            if let Some(backtrace) = e.backtrace() {
                                eprintln!("Backtrace: {}", backtrace);
                            }
                            failures += 1;
                        }
                    }
                }
                assert_eq!(failures, 0);
                let time = start.elapsed();
                println!("Read {successes} features in {}ms.", time.as_millis());
            }
        }
    }
}

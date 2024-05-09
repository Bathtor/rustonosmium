use std::io::BufWriter;

use super::*;
use can2k_geo::{abbreviations::*, points::GeoPoint, utils::*};
use itertools::Itertools;
use shapefile::{
    dbase::{FieldName, FieldValue, TableInfo, TableWriterBuilder, WritableRecord},
    *,
};

pub struct Polygon {
    // TODO: Bounding box
    // pub bounding_box: GeoRectangle<>
    pub id: i64,
    pub outer_ring: Vec<GeoPoint<Degrees>>,
    pub inner_ring: Option<Vec<GeoPoint<Degrees>>>,
}
impl Polygon {
    pub fn as_esri(&self) -> shapefile::Polygon {
        let mut rings = Vec::with_capacity(if self.inner_ring.is_some() { 2 } else { 1 });
        fn geo_to_point(gp: &GeoPoint<Degrees>) -> shapefile::Point {
            shapefile::Point {
                x: gp.lon.into_value(),
                y: gp.lat.into_value(),
            }
        }
        rings.push(shapefile::PolygonRing::Outer(
            self.outer_ring.iter().map(geo_to_point).collect(),
        ));
        if let Some(ref inner_ring) = self.inner_ring {
            rings.push(shapefile::PolygonRing::Inner(
                inner_ring.iter().map(geo_to_point).collect(),
            ));
        }
        shapefile::Polygon::with_rings(rings)
    }
}
impl WritableRecord for Polygon {
    fn write_using<'a, W: std::io::prelude::Write>(
        &self,
        field_writer: &mut dbase::FieldWriter<'a, W>,
    ) -> Result<(), dbase::FieldIOError> {
        field_writer.write_next_field_value(&(self.id as f64))?;
        Ok(())
    }
}

pub struct ShapefileReadDb<'p> {
    path: &'p Path,
    reader: reader::Reader<BufReader<File>, BufReader<File>>,
    reader_used: bool,
}

impl<'p> ShapefileReadDb<'p> {
    pub fn open<P>(file: &'p P) -> Result<Self, ScanError>
    where
        P: AsRef<Path> + ?Sized,
    {
        let reader = Reader::from_path(file)?;
        Ok(Self {
            path: file.as_ref(),
            reader,
            reader_used: false,
        })
    }

    pub fn writer_with_same_table_schema<'f, P>(
        &self,
        file: &'f P,
    ) -> Result<ShapefileWriteDb<'f>, ScanError>
    where
        P: AsRef<Path> + ?Sized,
    {
        let fresh_reader = Reader::from_path(self.path).expect("Worked the first time");
        ShapefileWriteDb::with_table_info(file, fresh_reader.into_table_info())
    }

    pub fn read_polygons<'a>(
        &'a mut self,
        // Assign running id if not given.
        id_field_name: Option<&'static str>,
    ) -> impl Iterator<Item = Result<Polygon, ScanError>> + 'a {
        if self.reader_used {
            // reset the reader
            self.reader = Reader::from_path(self.path).expect("Worked the first time");
        }
        self.reader_used = true;
        let mut id_counter = 0i64;
        let iter = self
            .reader
            .iter_shapes_and_records()
            .flat_map(move |result| match result {
                Ok((Shape::NullShape, _)) => None,
                Ok((Shape::Polygon(poly), record)) => {
                    let id = if let Some(name) = id_field_name {
                        match record.get(name) {
                            Some(fid) => match fid {
                                FieldValue::Numeric(Some(n)) => {
                                    assert!(
                                        n.is_finite() && n.is_normal(),
                                        "Id {n} is not representable as i64"
                                    );
                                    let n_rounded = n.round();
                                    assert_eq!(n_rounded, *n, "Id {n} is not representable as i64");
                                    n_rounded as i64
                                }
                                v => panic!("Expected some numeric id, got {v:?}"),
                            },
                            None => {
                                let fields = record.into_iter().map(|v| v.0).join(", ");
                                panic!("No FID in record. Available keys: {fields}");
                            }
                        }
                    } else {
                        id_counter += 1;
                        id_counter
                    };
                    // let id = match record.get("FID").expect("No FID") {
                    //     FieldValue::Numeric(n) => n.expect("Empty FID"),
                    //     _ => panic!("Expected numeric FID"),
                    // };
                    let rings = poly.rings();
                    assert!(rings.len() <= 2, "More than 2 rings are not supported");
                    fn point_to_geo(p: &shapefile::Point) -> GeoPoint<Degrees> {
                        let gp = coord(p.y, p.x);
                        require_lat_range(gp.lat).expect("lat range");
                        require_lon_range(gp.lon).expect("lon range");
                        gp
                    }
                    assert!(matches!(rings[0], shapefile::PolygonRing::Outer(_)));
                    if rings.len() >= 2 {
                        assert!(matches!(rings[1], shapefile::PolygonRing::Inner(_)));
                    }
                    let p = Polygon {
                        id,
                        outer_ring: rings[0].points().iter().map(point_to_geo).collect(),
                        inner_ring: Option::when(rings.len() >= 2, || {
                            rings[1].points().iter().map(point_to_geo).collect()
                        }),
                    };
                    Some(Ok(p))
                }
                Ok(_) => None, // Skip other shapes
                Err(e) => Some(Err(ScanError::ShapefileError(e))),
            });
        iter
    }
}

pub struct ShapefileWriteDb<'p> {
    path: &'p Path,
    writer: writer::Writer<BufWriter<File>>,
}

impl<'p> ShapefileWriteDb<'p> {
    pub fn open<P>(file: &'p P) -> Result<Self, ScanError>
    where
        P: AsRef<Path> + ?Sized,
    {
        let table_writer =
            TableWriterBuilder::new().add_numeric_field(FieldName::try_from("FID").unwrap(), 20, 1);
        let writer = Writer::from_path(file, table_writer)?;
        Ok(Self {
            path: file.as_ref(),
            writer,
        })
    }

    pub fn with_table_info<P>(file: &'p P, table_info: TableInfo) -> Result<Self, ScanError>
    where
        P: AsRef<Path> + ?Sized,
    {
        let table_writer = TableWriterBuilder::from_table_info(table_info);
        let writer = Writer::from_path(file, table_writer)?;
        Ok(Self {
            path: file.as_ref(),
            writer,
        })
    }

    pub fn write_polygon(&mut self, polygon: &Polygon) -> Result<(), ScanError> {
        let poly = polygon.as_esri();
        self.writer.write_shape_and_record(&poly, polygon)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_FILE_NAME: &'static str =
        "/Users/lkroll/Programming/Sailing/test-data/land-polygons-complete-4326/land_polygons.shp";
    const TEST_FILE_DB_NAME: &'static str =
        "/Users/lkroll/Programming/Sailing/test-data/land-polygons-complete-4326/land_polygons.dbf";

    #[ignore = "experiment only"]
    #[test]
    fn test_scan() {
        let mut reader = Reader::from_path(TEST_FILE_NAME).unwrap();
        //let records = dbase::read(TEST_FILE_DB_NAME).unwrap();

        let mut num_polygons = 0usize;
        let mut largest_polygon = 0usize;
        let mut max_rings = 0usize;
        for result in reader.iter_shapes_and_records() {
            let (shape, record) = result.unwrap();
            // println!("Shape: {}, records: ", shape);
            // for (name, value) in record {
            //     println!("\t{}: {:?}, ", name, value);
            // }
            match shape {
                Shape::NullShape => continue,
                Shape::Polygon(poly) => {
                    num_polygons += 1;
                    largest_polygon = largest_polygon.max(poly.total_point_count());
                    let num_rings = poly.rings().len();
                    max_rings = max_rings.max(num_rings);
                    if num_rings > 1 {
                        println!(
                            "Poly {} has bounding-box {:?} and {} points in {} rings.",
                            record.get("FID").unwrap(),
                            poly.bbox(),
                            poly.total_point_count(),
                            num_rings
                        );
                    }
                }
                _ => unreachable!("Only polygons expected in this file"),
            }
        }
        println!("Total number of polygons: {num_polygons} with the largest having {largest_polygon} points, and {max_rings} rings.");
    }
}

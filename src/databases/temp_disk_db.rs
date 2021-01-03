use super::*;
use crate::ScanError;
use bytes::{Buf, BufMut};
use memmap::*;
use rustc_hash::FxHashMap;
use std::{convert::TryInto, path::Path};
use tempfile::tempfile;

use uom::si::{f64::*, length::kilometer};

pub fn from_file<P>(path: P) -> Result<TempDiskDb, ScanError>
where
    P: AsRef<Path>,
{
    let mut node_locations: FxHashMap<i64, Offset> = FxHashMap::default();
    let mut writer = DbWriter::new();
    crate::quick_xml_reader::scan_nodes(
        path,
        |node, _acc| {
            let offset = writer.write_node(&node);
            node_locations.insert(node.id, offset);
        },
        (),
    )?;
    Ok(writer.into_db(node_locations))
}

type Offset = usize;
const FILE_SIZE: u64 = 128 * 1000 * 1000; // 128MB should do

pub struct TempDiskDb {
    node_locations: FxHashMap<i64, Offset>,
    node_file: Mmap,
    tags_file: Mmap,
}

impl TempDiskDb {
    /// Gives the number of nodes in the database
    pub fn len(&self) -> usize {
        self.node_locations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.node_locations.is_empty()
    }

    pub(super) fn scan_nodes(&self) -> NodeIter<'_> {
        NodeIter::new(&self.node_file, self.len())
    }

    pub(super) fn scan_nodes_with_tags(&self) -> NodeAndTagIter<'_> {
        NodeAndTagIter::new(&self.node_file, &self.tags_file, self.len())
    }
}

impl OwnedOsmDatabase for TempDiskDb {
    fn node_by_id(&self, id: i64) -> Option<Node> {
        let offset = *self.node_locations.get(&id)?;
        let node = OnDiskNode::read_from(&self.node_file[offset..]);
        let tags = OnDiskTags::read_from(&self.tags_file[node.tags_offset..]);
        Some(node.into_osm_with(tags))
    }

    fn nodes_in_radius(&self, pos: Position, radius: Length) -> Vec<Node> {
        let radius_km = radius.get::<kilometer>();
        let error_radius_km = radius_km * 1.1;
        let error_radius = Length::new::<kilometer>(error_radius_km);
        self.scan_nodes()
            .filter(|n| n.position().distance_approximate(&pos) < error_radius)
            .filter(|n| n.position().distance_accurate(&pos) < radius)
            .map(|node| {
                let tags = OnDiskTags::read_from(&self.tags_file[node.tags_offset..]);
                node.into_osm_with(tags)
            })
            .collect()
    }

    fn nodes_with_tag(&self, key_pattern: &str, value_pattern: Option<&str>) -> Vec<Node> {
        let filter_fun: Box<dyn Fn(&Node) -> bool> = if let Some(vp) = value_pattern {
            let vp = vp.to_string();
            let c = move |n: &Node| {
                n.tags
                    .iter()
                    .any(|t| t.key.contains(key_pattern) && t.value.contains(&vp))
            };
            Box::new(c)
        } else {
            let c = move |n: &Node| n.tags.iter().any(|t| t.key.contains(key_pattern));
            Box::new(c)
        };
        self.scan_nodes_with_tags().filter(|n| filter_fun(n)).collect()
    }
}

pub(super) struct NodeIter<'a> {
    file: &'a Mmap,
    offset: usize,
    read_records: usize,
    num_records: usize,
}

impl<'a> NodeIter<'a> {
    fn new(file: &'a Mmap, num_records: usize) -> Self {
        NodeIter {
            file,
            offset: 0,
            read_records: 0,
            num_records,
        }
    }
}

impl<'a> Iterator for NodeIter<'a> {
    type Item = OnDiskNode;

    fn next(&mut self) -> Option<Self::Item> {
        if self.read_records < self.num_records {
            let odn = OnDiskNode::read_from(&self.file[self.offset..]);
            self.offset += OnDiskNode::LENGTH;
            self.read_records += 1;
            Some(odn)
        } else {
            None
        }
    }
}

pub(super) struct NodeAndTagIter<'a> {
    node_file: &'a Mmap,
    tags_file: &'a Mmap,
    offset: usize,
    read_records: usize,
    num_records: usize,
}

impl<'a> NodeAndTagIter<'a> {
    fn new(node_file: &'a Mmap, tags_file: &'a Mmap, num_records: usize) -> Self {
        NodeAndTagIter {
            node_file,
            tags_file,
            offset: 0,
            read_records: 0,
            num_records,
        }
    }
}

impl<'a> Iterator for NodeAndTagIter<'a> {
    type Item = Node;

    fn next(&mut self) -> Option<Self::Item> {
        if self.read_records < self.num_records {
            let odn = OnDiskNode::read_from(&self.node_file[self.offset..]);
            self.offset += OnDiskNode::LENGTH;
            self.read_records += 1;
            let tags = OnDiskTags::read_from(&self.tags_file[odn.tags_offset..]);
            Some(odn.into_osm_with(tags))
        } else {
            None
        }
    }
}

struct DbWriter {
    node_file: MmapMut,
    node_offset: usize,
    tags_file: MmapMut,
    tags_offset: usize,
}

impl DbWriter {
    fn new() -> Self {
        let node_file = tempfile().expect("Could not create temporary file for nodes");
        let tags_file = tempfile().expect("Could not create temporary file for tags");
        node_file
            .set_len(FILE_SIZE)
            .expect("Could not set file length for nodes");
        tags_file
            .set_len(FILE_SIZE)
            .expect("Could not set file length for tags");
        let node_mmap = unsafe {
            MmapOptions::new()
                .map_mut(&node_file)
                .expect("Could not memory map nodes")
        };
        let tags_mmap = unsafe {
            MmapOptions::new()
                .map_mut(&tags_file)
                .expect("Could not memory map tags")
        };
        DbWriter {
            node_file: node_mmap,
            node_offset: 0,
            tags_file: tags_mmap,
            tags_offset: 0,
        }
    }

    fn write_node(&mut self, node: &Node) -> Offset {
        let disk_node = OnDiskNode::from(node, self.tags_offset);

        let offset = self.node_offset;
        disk_node.write_to(&mut self.node_file[offset..]);
        self.node_offset += OnDiskNode::LENGTH;

        let written = OnDiskTags::write_to(&mut self.tags_file[self.tags_offset..], node);
        self.tags_offset += written;

        offset
    }

    fn into_db(self, node_locations: FxHashMap<i64, Offset>) -> TempDiskDb {
        if cfg!(test) {
            println!(
                "Wrote DB with {}MB of nodes and {}MB of tags",
                self.node_offset / 1000000,
                self.tags_offset / 1000000
            );
        }
        TempDiskDb {
            node_locations,
            node_file: self
                .node_file
                .make_read_only()
                .expect("Could not make node file read-only"),
            tags_file: self
                .tags_file
                .make_read_only()
                .expect("Could not make tags file read-only"),
        }
    }
}

pub(super) struct OnDiskNode {
    id: i64,
    lat: i32,
    lon: i32,
    tags_offset: Offset,
}

impl OnDiskNode {
    const LENGTH: usize = 24;

    fn from(node: &Node, tags_offset: Offset) -> Self {
        let lat = if let Latitude::Tight(v) = node.lat.to_tight() {
            v
        } else {
            unreachable!("Must be tight now!")
        };
        let lon = if let Longitude::Tight(v) = node.lon.to_tight() {
            v
        } else {
            unreachable!("Must be tight now!")
        };
        OnDiskNode {
            id: node.id,
            lat,
            lon,
            tags_offset,
        }
    }

    fn write_to(&self, mut buf: impl BufMut) {
        buf.put_i64(self.id);
        buf.put_i32(self.lat);
        buf.put_i32(self.lon);
        buf.put_u64(self.tags_offset as u64);
    }

    fn read_from(mut buf: impl Buf) -> Self {
        let id = buf.get_i64();
        let lat = buf.get_i32();
        let lon = buf.get_i32();
        let tags_offset = buf.get_u64() as usize;
        OnDiskNode {
            id,
            lat,
            lon,
            tags_offset,
        }
    }

    pub(super) fn into_osm(self) -> Node {
        let lat = Latitude::Tight(self.lat);
        let lon = Longitude::Tight(self.lon);
        Node {
            id: self.id,
            lat,
            lon,
            tags: Vec::new(),
        }
    }

    fn into_osm_with(self, tags: OnDiskTags) -> Node {
        let lat = Latitude::Tight(self.lat);
        let lon = Longitude::Tight(self.lon);
        Node {
            id: self.id,
            lat,
            lon,
            tags: tags.tags.into_iter().map(|(key, value)| Tag { key, value }).collect(),
        }
    }

    fn position(&self) -> Position {
        let lat = Latitude::Tight(self.lat);
        let lon = Longitude::Tight(self.lon);
        Position { lat, lon }
    }
}

struct OnDiskTags {
    tags: Vec<(String, String)>,
}

impl OnDiskTags {
    fn write_to(mut buf: impl BufMut, node: &Node) -> usize {
        let mut written = 0;
        buf.put_u8(node.tags.len().try_into().expect("too many tags"));
        written += 1;
        for tag in node.tags.iter() {
            let key_bytes = tag.key.as_bytes();
            let key_len: u16 = key_bytes.len().try_into().expect("key too long");
            buf.put_u16(key_len);
            written += 2;
            buf.put_slice(key_bytes);
            written += key_bytes.len();

            let value_bytes = tag.value.as_bytes();
            let value_len: u16 = value_bytes.len().try_into().expect("value too long");
            buf.put_u16(value_len);
            written += 2;
            buf.put_slice(value_bytes);
            written += value_bytes.len();
        }
        written
    }

    fn read_from(mut buf: impl Buf) -> Self {
        let mut tags: Vec<(String, String)> = Vec::new();
        let num_tags = buf.get_u8();
        for _i in 0..num_tags {
            let key_len = buf.get_u16() as usize;
            let mut key_data = vec![0u8; key_len];
            buf.copy_to_slice(&mut key_data);
            let key = unsafe { String::from_utf8_unchecked(key_data) }; // I put them in there, they should still be legal

            let value_len = buf.get_u16() as usize;
            let mut value_data = vec![0u8; value_len];
            buf.copy_to_slice(&mut value_data);
            let value = unsafe { String::from_utf8_unchecked(value_data) }; // I put them in there, they should still be legal
            tags.push((key, value));
        }
        OnDiskTags { tags }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn test_load_and_write() {
    //     let db = from_file(crate::tests::TEST_OSM_FILE).expect("Database!");
    //     assert!(!db.is_empty());
    // }

    #[test]
    fn test_filter_tags() {
        let db = from_file(crate::tests::TEST_OSM_FILE).expect("load db");
        crate::tests::test_filter_tags_owned(db);
    }

    #[test]
    fn test_nodes_in_distance() {
        let db = from_file(crate::tests::TEST_OSM_FILE).expect("load db");
        crate::tests::test_nodes_in_distance_owned(db);
    }
}

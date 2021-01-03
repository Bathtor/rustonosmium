use super::*;
use crate::geometry::planar::{Point, Rectangle};
use bytes::{Buf, BufMut};
use lru::LruCache;
use memmap::*;
use std::{cell::RefCell, convert::TryInto, fmt, marker::PhantomData, rc::Rc};
use tempfile::tempfile;

pub type RTree<V> = nodes::RTree<V, OnDiskNodeManager<V>>;

pub fn new<V>() -> RTree<V>
where
    V: FixedSizeSerde + fmt::Debug,
{
    nodes::RTree::new(OnDiskNodeManager::new())
}

pub fn with_cache<V>(cache_size: usize) -> RTree<V>
where
    V: FixedSizeSerde + fmt::Debug,
{
    nodes::RTree::new(OnDiskNodeManager::with_cache(cache_size))
}

pub fn with_capacity<V>(capacity: usize) -> RTree<V>
where
    V: FixedSizeSerde + fmt::Debug,
{
    // assume all leaves are half-filled
    let avg_fill_degree = NODE_WIDTH / 2;
    let mut num_nodes = capacity / avg_fill_degree;
    let mut last_summand = num_nodes;
    let mut level = 1;
    while last_summand > 0 {
        level += 1;
        last_summand = capacity / avg_fill_degree.pow(level);
        num_nodes += last_summand;
    }
    nodes::RTree::new(OnDiskNodeManager::with_capacity(num_nodes))
}

pub trait FixedSizeSerde {
    const SIZE: usize;
    fn ser_into(&self, buf: impl BufMut);
    fn deser_from(buf: impl Buf) -> Self;
}

impl FixedSizeSerde for Point {
    const SIZE: usize = 16;

    fn ser_into(&self, mut buf: impl BufMut) {
        buf.put_f64(self.x);
        buf.put_f64(self.y);
    }

    fn deser_from(mut buf: impl Buf) -> Self {
        let x = buf.get_f64();
        let y = buf.get_f64();
        Point { x, y }
    }
}

impl FixedSizeSerde for ChildEntry {
    const SIZE: usize = 40;

    fn ser_into(&self, mut buf: impl BufMut) {
        buf.put_f64(self.0.low_corner.x);
        buf.put_f64(self.0.low_corner.y);
        buf.put_f64(self.0.high_corner.x);
        buf.put_f64(self.0.high_corner.y);
        buf.put_u64(self.1 as u64);
    }

    fn deser_from(mut buf: impl Buf) -> Self {
        let low_x = buf.get_f64();
        let low_y = buf.get_f64();
        let low_corner = Point { x: low_x, y: low_y };
        let high_x = buf.get_f64();
        let high_y = buf.get_f64();
        let high_corner = Point { x: high_x, y: high_y };
        let bounding_box = Rectangle {
            low_corner,
            high_corner,
        };
        let id: usize = buf.get_u64().try_into().expect("should fit");
        ChildEntry(bounding_box, id)
    }
}

type Offset = usize;
const FILE_SIZE: u64 = 256 * 1000 * 1000; // 256MB

pub struct OnDiskNodeManager<V>
where
    V: FixedSizeSerde + fmt::Debug,
{
    //nodes: Vec<NodeRef<V>>,
    node_file: MmapMut,
    num_nodes: usize,
    cache_size: usize,
    cache_data: RefCell<CacheData<V>>,
}

struct CacheData<V> {
    cache: LruCache<NodeId, NodeRef<V>>,
    must_flush: Vec<IdRef<V>>,
}

impl<V> CacheData<V> {
    fn new() -> Self {
        CacheData {
            cache: LruCache::unbounded(),
            must_flush: Vec::new(),
        }
    }
}

impl<V> OnDiskNodeManager<V>
where
    V: FixedSizeSerde + fmt::Debug,
{
    pub fn new() -> Self {
        let node_file = Self::create_file(FILE_SIZE);
        OnDiskNodeManager {
            node_file,
            num_nodes: 0,
            cache_size: NODE_WIDTH + 1,
            cache_data: RefCell::new(CacheData::new()),
        }
    }

    pub fn with_cache(cache_size: usize) -> Self {
        let node_file = Self::create_file(FILE_SIZE);
        OnDiskNodeManager {
            node_file,
            num_nodes: 0,
            cache_size,
            cache_data: RefCell::new(CacheData::new()),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let file_size = (capacity as u64) * (Record::<V>::SIZE as u64);
        let node_file = Self::create_file(file_size);
        OnDiskNodeManager {
            node_file,
            num_nodes: 0,
            cache_size: NODE_WIDTH + 1,
            cache_data: RefCell::new(CacheData::new()),
        }
    }

    fn create_file(size: u64) -> MmapMut {
        let node_file = tempfile().expect("Could not create temporary file for nodes");
        node_file.set_len(size).expect("Could not set file length for nodes");
        unsafe {
            MmapOptions::new()
                .map_mut(&node_file)
                .expect("Could not memory map nodes")
        }
    }

    fn record_offset(&self, id: NodeId) -> Offset {
        id * Record::<V>::SIZE
    }

    fn cache_node(&self, node: IdRef<V>) {
        if self.cache_size == 0 {
            return;
        }
        let mut mut_cache_data = self.cache_data.borrow_mut();
        if mut_cache_data.cache.len() >= self.cache_size {
            let uncached = mut_cache_data
                .cache
                .pop_lru()
                .expect("should be something in there if it's full");
            if uncached.1.is_dirty() {
                mut_cache_data.must_flush.push(uncached.1.with_id(uncached.0));
            }
        }
        mut_cache_data.cache.put(node.id, node.node);
    }

    fn load_record(&self, offset: Offset) -> Record<V> {
        Record::read_from(&self.node_file[offset..])
    }

    fn record_to_node(&self, record: Record<V>, offset: Offset) -> NodeRef<V> {
        let mut entry_offset = offset + Record::<V>::MY_SIZE;
        if record.is_leaf() {
            let mut entries: [Option<V>; NODE_WIDTH] = Default::default();
            for entry in entries.iter_mut().take(record.len as usize) {
                let v = V::deser_from(&self.node_file[entry_offset..]);
                *entry = Some(v);
                entry_offset += Record::<V>::ENTRY_SIZE;
            }
            let leaf = Leaf::with(record.get_parent(), record.len as usize, entries);
            NodeRef::Leaf(Rc::new(RefCell::new(leaf)))
        } else {
            let mut entries: [Option<ChildEntry>; NODE_WIDTH] = Default::default();
            for entry in entries.iter_mut().take(record.len as usize) {
                let child = ChildEntry::deser_from(&self.node_file[entry_offset..]);
                *entry = Some(child);
                entry_offset += Record::<V>::ENTRY_SIZE;
            }
            let internal = InternalNode::with(record.get_parent(), record.len as usize, entries);
            NodeRef::Internal(Rc::new(RefCell::new(internal)))
        }
    }

    fn has_space(&self) -> bool {
        let last_offset = self.record_offset(self.num_nodes);
        let last_end = last_offset + Record::<V>::SIZE;
        last_end < self.node_file.len()
    }

    fn write_record(&mut self, node: IdRef<V>, expected_ref_count: usize) {
        let id = node.id;
        let offset = self.record_offset(id);
        match node.node {
            NodeRef::Leaf(l) => {
                assert_eq!(
                    expected_ref_count,
                    Rc::strong_count(&l),
                    "Don't hold on to references during flush!"
                );
                let mut leaf = l.borrow_mut();
                let record = Record::from_leaf(&leaf);
                record.write_to(&mut self.node_file[offset..]);
                let mut entry_offset = offset + Record::<V>::MY_SIZE;
                for entry in leaf.filled_entries() {
                    entry.ser_into(&mut self.node_file[entry_offset..]);
                    entry_offset += Record::<V>::ENTRY_SIZE;
                }
                leaf.set_clean();
            }
            NodeRef::Internal(i) => {
                assert_eq!(
                    expected_ref_count,
                    Rc::strong_count(&i),
                    "Don't hold on to references during flush!"
                );
                let mut inode = i.borrow_mut();
                let record = Record::<V>::from_inode(&inode);
                record.write_to(&mut self.node_file[offset..]);
                let mut entry_offset = offset + Record::<V>::MY_SIZE;
                for entry in inode.filled_entries() {
                    entry.ser_into(&mut self.node_file[entry_offset..]);
                    entry_offset += Record::<V>::ENTRY_SIZE;
                }
                inode.set_clean();
            }
        }
    }
}

impl<V> NodeManager<V> for OnDiskNodeManager<V>
where
    V: FixedSizeSerde + fmt::Debug,
{
    fn get(&self, id: NodeId) -> NodeRef<V> {
        if id < self.num_nodes {
            let cache_res: Option<NodeRef<V>> = { self.cache_data.borrow_mut().cache.get(&id).cloned() };
            if let Some(node) = cache_res {
                node
            } else {
                let pos_res = { self.cache_data.borrow().must_flush.iter().position(|n| n.id == id) };
                if let Some(position) = pos_res {
                    let id_node = self.cache_data.borrow_mut().must_flush.swap_remove(position);
                    self.cache_node(id_node.clone());
                    id_node.node
                } else {
                    let offset = self.record_offset(id);
                    let record = self.load_record(offset);
                    let node = self.record_to_node(record, offset);
                    self.cache_node(node.clone().with_id(id));
                    node
                }
            }
        } else {
            panic!(
                "Never ask for node ids that don't exist! id={} vs. len={}",
                id, self.num_nodes
            )
        }
    }

    fn fresh_leaf(&mut self) -> IdRef<V> {
        assert!(self.has_space(), "Index file ran out of space");
        let mut leaf = Leaf::new();
        leaf.set_dirty();
        let leaf = Rc::new(RefCell::new(leaf));
        let node = NodeRef::Leaf(leaf);
        let id = self.num_nodes;
        self.num_nodes += 1;
        let id_node = IdRef { id, node };
        self.cache_node(id_node.clone());
        id_node
    }

    fn fresh_internal(&mut self) -> IdRef<V> {
        assert!(self.has_space(), "Index file ran out of space");
        let mut inode = InternalNode::new();
        inode.set_dirty();
        let inode = Rc::new(RefCell::new(inode));
        let node = NodeRef::Internal(inode);
        let id = self.num_nodes;
        self.num_nodes += 1;
        let id_node = IdRef { id, node };
        self.cache_node(id_node.clone());
        id_node
    }

    fn flush(&mut self) {
        let cache_data_mut = self.cache_data.get_mut();
        let mut must_write = Vec::new();
        std::mem::swap(&mut must_write, &mut cache_data_mut.must_flush);
        for node in must_write {
            self.write_record(node, 1);
        }
        let mut dirties: Vec<IdRef<V>> = Vec::new();
        for (id, node) in self.cache_data.borrow().cache.iter() {
            if node.is_dirty() {
                dirties.push(node.clone().with_id(*id));
            }
        }
        for node in dirties {
            self.write_record(node, 2);
        }
    }

    fn print_stats(&self) {
        println!(
            "On-disk RTree Index holding {} nodes in {}MB. Also {}/{} nodes in cache.",
            self.num_nodes,
            self.num_nodes * Record::<V>::SIZE / 10000000,
            self.cache_data.borrow().cache.len(),
            self.cache_size
        );
    }
}

impl<V> Default for OnDiskNodeManager<V>
where
    V: FixedSizeSerde + fmt::Debug,
{
    fn default() -> Self {
        OnDiskNodeManager::new()
    }
}

struct Record<V>
where
    V: FixedSizeSerde,
{
    /// 0. record type (0: leaf, 1: internal)
    /// 1. has_parent (boolean)
    /// 2-7 unused
    record_bits: u8,
    parent: u64,
    /// make sure NODE_WIDTH fits into u16!
    len: u16,
    // next would be entries
    //entries: [[u8; V::SIZE]; NODE_WIDTH],
    marker: PhantomData<V>,
}

impl<V> Record<V>
where
    V: FixedSizeSerde,
{
    const ENTRY_SIZE: usize = [ChildEntry::SIZE, V::SIZE][(ChildEntry::SIZE < V::SIZE) as usize];
    const HAS_PARENT_MASK: u8 = 0b00000010;
    const MY_SIZE: usize = 11;
    const RECORD_TYPE_MASK: u8 = 0b00000001;
    // const max
    const SIZE: usize = Self::MY_SIZE + Self::ENTRY_SIZE * NODE_WIDTH;

    fn from_leaf(leaf: &Leaf<V>) -> Self {
        let mut record_bits = 0u8;
        let parent = if let Some(parent) = leaf.get_parent() {
            record_bits |= Self::HAS_PARENT_MASK;
            parent as u64
        } else {
            0u64
        };
        let len: u16 = leaf.len().try_into().expect("Too many entries");
        Record {
            record_bits,
            parent,
            len,
            marker: PhantomData,
        }
    }

    fn from_inode(inode: &InternalNode) -> Self {
        let mut record_bits = Self::RECORD_TYPE_MASK;
        let parent = if let Some(parent) = inode.get_parent() {
            record_bits |= Self::HAS_PARENT_MASK;
            parent as u64
        } else {
            0u64
        };
        let len: u16 = inode.len().try_into().expect("Too many entries");
        Record {
            record_bits,
            parent,
            len,
            marker: PhantomData,
        }
    }

    fn is_leaf(&self) -> bool {
        (self.record_bits & Self::RECORD_TYPE_MASK) == 0
    }

    fn is_internal(&self) -> bool {
        (self.record_bits & Self::RECORD_TYPE_MASK) == 1
    }

    fn get_parent(&self) -> Option<NodeId> {
        if (self.record_bits & Self::HAS_PARENT_MASK) != 0 {
            Some(self.parent.try_into().unwrap())
        } else {
            None
        }
    }

    fn read_from(mut buf: impl Buf) -> Self {
        let record_bits = buf.get_u8();
        let parent = buf.get_u64();
        let len = buf.get_u16();
        Record {
            record_bits,
            parent,
            len,
            marker: PhantomData,
        }
    }

    fn write_to(&self, mut buf: impl BufMut) {
        buf.put_u8(self.record_bits);
        buf.put_u64(self.parent);
        buf.put_u16(self.len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let tree = new();
        crate::databases::indices::r_tree::tests::test_basic(tree);
    }

    #[test]
    fn test_ser_deser() {
        let mut buf: Vec<u8> = Vec::new();
        let r = Record::<Point> {
            record_bits: 0b00000011,
            parent: 42,
            len: 5,
            marker: PhantomData,
        };
        assert!(r.is_internal());
        assert_eq!(Some(42), r.get_parent());
        r.write_to(&mut buf);
        let deser = Record::<Point>::read_from(buf.as_slice());
        assert!(deser.is_internal());
        assert_eq!(Some(42), deser.get_parent());
        assert_eq!(5, deser.len);

        let p = Point { x: 10.0, y: -24.0 };
        let mut manager = OnDiskNodeManager::new();
        let leaf_id = {
            let leaf = manager.fresh_leaf();
            let mut mut_leaf = leaf.node.as_leaf_mut();
            mut_leaf.set_parent(42);
            mut_leaf.insert(p);
            leaf.id
        };
        manager.flush();
        {
            let leaf = manager.get(leaf_id);
            let leaf = leaf.as_leaf();
            assert_eq!(Some(42), leaf.get_parent());
            assert_eq!(1, leaf.len());
            assert!(leaf.filled_entries().any(|e| *e == p));
        }
        let inode_id = {
            let inode = manager.fresh_internal();
            let mut mut_inode = inode.node.as_internal_mut();
            mut_inode.insert(
                Rectangle {
                    low_corner: p,
                    high_corner: p,
                },
                leaf_id,
            );
            inode.id
        };
        {
            let leaf = manager.get(leaf_id);
            leaf.set_parent(inode_id);
        }
        manager.flush();
        {
            let inode = manager.get(inode_id);
            let inode = inode.as_internal();
            assert_eq!(None, inode.get_parent());
            assert_eq!(1, inode.len());
            assert!(inode.filled_entries().any(|e| e.1 == leaf_id));
        }
        {
            let leaf = manager.get(leaf_id);
            let leaf = leaf.as_leaf();
            assert_eq!(Some(inode_id), leaf.get_parent());
            assert_eq!(1, leaf.len());
            assert!(leaf.filled_entries().any(|e| *e == p));
        }
    }
}

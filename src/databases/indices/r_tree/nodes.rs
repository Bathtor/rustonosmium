use super::*;

use crate::geometry::{planar::Rectangle, Bounding, Extending, HasArea, Intersecting};

use std::{
    cell::{Ref, RefCell, RefMut},
    cmp::Ordering,
    collections::VecDeque,
    fmt,
    marker::PhantomData,
    rc::Rc,
};

pub struct RTree<V, M>
where
    M: NodeManager<V>,
{
    root: NodeId,
    manager: M,
    marker: PhantomData<V>,
}

impl<V, M> RTree<V, M>
where
    M: NodeManager<V>,
{
    pub fn new(mut manager: M) -> Self {
        let root = manager.fresh_leaf();
        let root_id = root.id;
        drop(root);
        manager.flush();
        RTree {
            root: root_id,
            manager,
            marker: PhantomData,
        }
    }

    // /// Creates an empty tree designed to take `capacity elements`
    // pub fn with_capacity(mut manager: M, capacity: usize) -> Self {
    //     // assume all leaves are half-filled
    //     let avg_fill_degree = NODE_WIDTH / 2;
    //     let mut num_nodes = capacity / avg_fill_degree;
    //     let mut last_summand = num_nodes;
    //     let mut level = 1;
    //     while last_summand > 0 {
    //         level += 1;
    //         last_summand = capacity / avg_fill_degree.pow(level);
    //         num_nodes += last_summand;
    //     }
    //     let mut manager = NodeManager::with_capacity(num_nodes);
    //     let root = manager.fresh_leaf();
    //     RTree { root, manager }
    // }

    pub fn is_empty(&self) -> bool {
        let root = self.manager.get(self.root);
        root.len() == 0
    }

    /// Checks the whole tree to determine it is sound
    pub fn assert_invariants(&self) {
        let mut to_search: VecDeque<IdRef<V>> = VecDeque::with_capacity(1);
        to_search.push_back(self.manager.get(self.root).with_id(self.root));
        let mut is_root = true;
        while let Some(IdRef {
            node: current_node,
            id: current_id,
        }) = to_search.pop_front()
        {
            match current_node {
                NodeRef::Internal(node) => {
                    let borrowed = node.borrow();
                    borrowed.assert_invariants(is_root);
                    let parent_bbox = borrowed.calculate_bounding_box();
                    for ChildEntry(bbox, id) in borrowed.filled_entries() {
                        assert!(parent_bbox.contains(bbox));
                        let child = self.manager.get(*id);
                        assert_eq!(child.get_parent().expect("must have parent"), current_id);
                        to_search.push_back(child.with_id(*id));
                    }
                }
                NodeRef::Leaf(leaf) => {
                    leaf.borrow().assert_invariants(is_root);
                }
            }
            is_root = false;
        }
    }

    /// Prints statistics about the tree to stdout
    pub fn print_stats(&self) {
        self.manager.print_stats();
    }
}

impl<V, M> Default for RTree<V, M>
where
    M: NodeManager<V> + Default,
{
    fn default() -> Self {
        RTree::new(M::default())
    }
}

impl<V, M> RTree<V, M>
where
    V: Clone + Extending<Rectangle> + Bounding + AsRef<<V as Bounding>::SplitUse>,
    M: NodeManager<V>,
{
    /// Search for all values in this tree that *may* overlap with `geometry`
    ///
    /// # Note
    ///
    /// Values at the leaves are not compared with `geometry`,
    /// but all values at selected leaves are returned instead.
    ///
    /// If `V` can be compared for inclusion in `geometry`, you must
    /// perform another selection on the resulting vector.
    ///
    /// If `V` can overlap with multiple regions (i.e. is not a point)
    /// then you may want to filter out duplicates before performing the
    /// second selection described above.
    pub fn search<G>(&self, geometry: &G) -> Vec<V>
    where
        G: Intersecting<Rectangle> + fmt::Debug,
    {
        //println!("### Starting search for {:?}", geometry);
        let mut results: Vec<V> = Vec::new();

        let mut to_search: VecDeque<NodeRef<V>> = VecDeque::with_capacity(1);
        to_search.push_back(self.manager.get(self.root));

        while let Some(current) = to_search.pop_front() {
            match current {
                NodeRef::Internal(node) => {
                    //println!("Inspecting node {:?}", node);
                    for ChildEntry(bounds, id) in node.borrow().filled_entries() {
                        if geometry.intersects(bounds) {
                            //println!("Child {} intersects with needle ({} ⌒ {:?})", id, bounds, geometry);
                            to_search.push_back(self.manager.get(*id));
                        }
                        // else {
                        //     println!(
                        //         "Child {} does not intersect with needle ({} ⌒/ {:?})",
                        //         id, bounds, geometry
                        //     );
                        // }
                    }
                }
                NodeRef::Leaf(leaf) => {
                    //println!("Found leaf with {} entries!", leaf.borrow().len());
                    for entry in leaf.borrow().filled_entries() {
                        results.push(entry.clone())
                    }
                }
            }
        }
        //println!("### Completed search for {:?}", geometry);
        results
    }

    /// Updates the tree to include `value`
    pub fn insert(&mut self, value: V) {
        let target_leaf = self.choose_leaf(&value);
        if target_leaf.node.as_leaf().has_space() {
            target_leaf.node.as_leaf_mut().insert(value);
            self.adjust_tree(target_leaf, None);
        } else {
            let new_leaf = self.split_leaf(target_leaf.clone(), value);
            self.adjust_tree(target_leaf, Some(new_leaf))
        }
        self.manager.flush();
    }

    fn choose_leaf(&mut self, value: &V) -> IdRef<V> {
        let mut current = self.manager.get(self.root).with_id(self.root);
        while current.node.is_internal() {
            let best_id = current.node.as_internal().best_fit(value);
            current = self.manager.get(best_id).with_id(best_id);
        }
        current
    }

    fn adjust_tree(&mut self, mut node: IdRef<V>, mut split_node_opt: Option<IdRef<V>>) {
        while let Some(parent_id) = node.get_parent() {
            let parent = self.manager.get(parent_id);
            let mut mut_parent = parent.as_internal_mut();
            mut_parent.update_bounding_box(node.id, node.node.calculate_bounding_box());
            if let Some(split_node) = split_node_opt {
                if mut_parent.has_space() {
                    mut_parent.insert(split_node.node.calculate_bounding_box(), split_node.id);
                    split_node.node.set_parent(parent_id);
                    drop(mut_parent);
                    // set up next iteration
                    node = parent.with_id(parent_id);
                    split_node_opt = None;
                } else {
                    drop(mut_parent);
                    let id_parent = parent.with_id(parent_id);
                    let new_node = self.split_internal(id_parent.clone(), split_node);
                    // set up next iteration
                    node = id_parent;
                    split_node_opt = Some(new_node);
                }
            } else {
                drop(mut_parent);
                // set up next iteration
                node = parent.with_id(parent_id);
            }
        }
        // done when node is root
        if let Some(split_node) = split_node_opt {
            // grow tree
            let new_root = self.manager.fresh_internal();
            node.node.set_parent(new_root.id);
            split_node.node.set_parent(new_root.id);
            let mut mut_root = new_root.node.as_internal_mut();
            mut_root.insert(node.node.calculate_bounding_box(), node.id);
            mut_root.insert(split_node.node.calculate_bounding_box(), split_node.id);
            drop(mut_root);
            self.root = new_root.id;
        }
    }

    /// Take a full target `leaf` and a value to be added
    /// and create and return a new sibling leaf,
    /// spreading the old values plus `value` over both leaves
    fn split_leaf(&mut self, leaf: IdRef<V>, value: V) -> IdRef<V> {
        let mut values: Vec<V> = Vec::with_capacity(1);
        let mut mut_leaf = leaf.node.as_leaf_mut();
        mut_leaf.take_values(&mut values);
        values.push(value);
        let (left, right) = V::find_split(values, MIN_FILL, NODE_WIDTH);
        mut_leaf.set_values(left);
        let new_leaf = self.manager.fresh_leaf();
        new_leaf.node.as_leaf_mut().set_values(right);
        new_leaf
    }

    /// Take a full target `node` and a `new_child` to be added
    /// and create and return a new sibling node,
    /// spreading the old children plus `new_child` over both leaves
    fn split_internal(&mut self, node: IdRef<V>, new_child: IdRef<V>) -> IdRef<V> {
        let mut nodes: Vec<ChildEntry> = Vec::with_capacity(1);
        let mut mut_node = node.node.as_internal_mut();
        mut_node.take_children(&mut nodes);
        nodes.push(ChildEntry(
            new_child.node.calculate_bounding_box(),
            new_child.id,
        ));
        let (left, right) = Rectangle::find_split(nodes, MIN_FILL, NODE_WIDTH);
        mut_node.set_children(left);
        for ChildEntry(_, id) in mut_node.filled_entries() {
            let child = self.manager.get(*id);
            child.set_parent(node.id);
        }
        let new_child = self.manager.fresh_internal();
        let mut mut_new_child = new_child.node.as_internal_mut();
        mut_new_child.set_children(right);
        for ChildEntry(_, id) in mut_new_child.filled_entries() {
            let child = self.manager.get(*id);
            child.set_parent(new_child.id);
        }
        drop(mut_new_child);
        new_child
    }
}

pub(crate) type NodeId = usize;

pub struct IdRef<V> {
    pub(crate) id: NodeId,
    pub(crate) node: NodeRef<V>,
}

impl<V> IdRef<V> {
    fn is_root(&self) -> bool {
        self.node.is_root()
    }

    pub(super) fn is_dirty(&self) -> bool {
        self.node.is_dirty()
    }

    fn get_parent(&self) -> Option<NodeId> {
        self.node.get_parent()
    }

    fn len(&self) -> usize {
        self.node.len()
    }
}

impl<V> Clone for IdRef<V> {
    fn clone(&self) -> Self {
        IdRef {
            id: self.id,
            node: self.node.clone(),
        }
    }
}

#[derive(Debug)]
pub enum NodeRef<V> {
    Internal(Rc<RefCell<InternalNode>>),
    Leaf(Rc<RefCell<Leaf<V>>>),
}
impl<V> NodeRef<V> {
    pub(super) fn with_id(self, id: NodeId) -> IdRef<V> {
        IdRef { id, node: self }
    }

    fn is_root(&self) -> bool {
        match self {
            NodeRef::Internal(n) => n.borrow().is_root(),
            NodeRef::Leaf(l) => l.borrow().is_root(),
        }
    }

    pub(super) fn is_dirty(&self) -> bool {
        match self {
            NodeRef::Internal(n) => match n.try_borrow() {
                Ok(inode) => inode.is_dirty(),
                Err(_) => true, // literally currently borrowed mutably
            },
            NodeRef::Leaf(l) => match l.try_borrow() {
                Ok(leaf) => leaf.is_dirty(),
                Err(_) => true, // literally currently borrowed mutably
            },
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            NodeRef::Internal(n) => n.borrow().len(),
            NodeRef::Leaf(l) => l.borrow().len(),
        }
    }

    pub(super) fn get_parent(&self) -> Option<NodeId> {
        match self {
            NodeRef::Internal(n) => n.borrow().get_parent(),
            NodeRef::Leaf(l) => l.borrow().get_parent(),
        }
    }

    pub(super) fn set_parent(&self, parent_id: NodeId) {
        match self {
            NodeRef::Internal(n) => n.borrow_mut().set_parent(parent_id),
            NodeRef::Leaf(l) => l.borrow_mut().set_parent(parent_id),
        }
    }

    pub(super) fn is_leaf(&self) -> bool {
        matches!(self, NodeRef::Leaf(_))
    }

    pub(super) fn is_internal(&self) -> bool {
        matches!(self, NodeRef::Internal(_))
    }

    pub(super) fn as_leaf(&self) -> Ref<'_, Leaf<V>> {
        match self {
            NodeRef::Leaf(l) => l.borrow(),
            _ => panic!("Cannot borrow as leaf"),
        }
    }

    pub(super) fn as_leaf_mut(&self) -> RefMut<'_, Leaf<V>> {
        match self {
            NodeRef::Leaf(l) => l.borrow_mut(),
            _ => panic!("Cannot borrow as leaf"),
        }
    }

    pub(super) fn as_internal(&self) -> Ref<'_, InternalNode> {
        match self {
            NodeRef::Internal(inode) => inode.borrow(),
            _ => panic!("Cannot borrow as internal node"),
        }
    }

    pub(super) fn as_internal_mut(&self) -> RefMut<'_, InternalNode> {
        match self {
            NodeRef::Internal(inode) => inode.borrow_mut(),
            _ => panic!("Cannot borrow as internal node"),
        }
    }
}
impl<V> Clone for NodeRef<V> {
    fn clone(&self) -> Self {
        match self {
            NodeRef::Internal(n) => NodeRef::Internal(n.clone()),
            NodeRef::Leaf(l) => NodeRef::Leaf(l.clone()),
        }
    }
}
impl<V> NodeRef<V>
where
    V: Bounding,
{
    fn calculate_bounding_box(&self) -> Rectangle {
        match self {
            NodeRef::Internal(n) => n.borrow().calculate_bounding_box(),
            NodeRef::Leaf(l) => l.borrow().calculate_bounding_box(),
        }
    }
}

#[derive(Debug)]
pub struct Leaf<V> {
    dirty: bool,
    parent: Option<NodeId>,
    len: usize,
    entries: [Option<V>; NODE_WIDTH],
}
impl<V> Leaf<V> {
    pub(crate) fn new() -> Self {
        Leaf {
            dirty: false,
            parent: None,
            len: 0,
            entries: Default::default(),
        }
    }

    pub(super) fn with(
        parent: Option<NodeId>,
        len: usize,
        entries: [Option<V>; NODE_WIDTH],
    ) -> Self {
        Leaf {
            dirty: false,
            parent,
            len,
            entries,
        }
    }

    pub(super) fn filled_entries(&self) -> impl Iterator<Item = &V> {
        self.entries[0..self.len]
            .iter()
            .map(|o| o.as_ref().unwrap())
    }

    fn is_root(&self) -> bool {
        self.parent.is_none()
    }

    pub(super) fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub(super) fn set_dirty(&mut self) {
        self.dirty = true;
    }

    pub(super) fn set_clean(&mut self) {
        self.dirty = false;
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    fn has_space(&self) -> bool {
        self.len < NODE_WIDTH
    }

    pub(super) fn get_parent(&self) -> Option<NodeId> {
        self.parent
    }

    pub(super) fn set_parent(&mut self, parent_id: NodeId) {
        self.dirty = true;
        self.parent = Some(parent_id);
    }

    fn take_values(&mut self, target: &mut Vec<V>) {
        self.dirty = true;
        for entry_opt in self.entries[0..self.len].iter_mut() {
            target.push(entry_opt.take().unwrap()); // must be filled if node is legal
        }
        self.len = 0;
    }

    fn set_values(&mut self, source: Vec<V>) {
        self.dirty = true;
        debug_assert!(source.len() <= self.entries.len()); // just to make sure
        let mut index = 0;
        for entry in source {
            self.entries[index] = Some(entry);
            index += 1;
        }
        self.len = index;
    }

    pub(super) fn insert(&mut self, value: V) {
        self.dirty = true;
        self.entries[self.len] = Some(value);
        self.len += 1;
    }

    fn assert_invariants(&self, is_root: bool) {
        // consistency
        let actual_len = self.entries.iter().filter(|e| e.is_some()).count();
        assert_eq!(actual_len, self.len);
        // (1)
        if !is_root {
            assert!(MIN_FILL <= self.len);
        }
    }
}

impl<V> Leaf<V>
where
    V: Bounding,
{
    fn calculate_bounding_box(&self) -> Rectangle {
        Bounding::bound_all(self.filled_entries())
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ChildEntry(pub(super) Rectangle, pub(super) NodeId);

impl AsRef<Rectangle> for ChildEntry {
    fn as_ref(&self) -> &Rectangle {
        &self.0
    }
}

#[derive(Debug)]
pub struct InternalNode {
    dirty: bool,
    parent: Option<NodeId>,
    len: usize,
    entries: [Option<ChildEntry>; NODE_WIDTH],
}
impl InternalNode {
    pub(crate) fn new() -> Self {
        InternalNode {
            dirty: false,
            parent: None,
            len: 0,
            entries: Default::default(),
        }
    }

    pub(super) fn with(
        parent: Option<NodeId>,
        len: usize,
        entries: [Option<ChildEntry>; NODE_WIDTH],
    ) -> Self {
        InternalNode {
            dirty: false,
            parent,
            len,
            entries,
        }
    }

    pub(super) fn filled_entries(&self) -> impl Iterator<Item = &ChildEntry> {
        self.entries[0..self.len]
            .iter()
            .map(|o| o.as_ref().unwrap())
    }

    fn is_root(&self) -> bool {
        self.parent.is_none()
    }

    pub(super) fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub(super) fn set_dirty(&mut self) {
        self.dirty = true;
    }

    pub(super) fn set_clean(&mut self) {
        self.dirty = false;
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn get_parent(&self) -> Option<NodeId> {
        self.parent
    }

    pub(super) fn set_parent(&mut self, parent_id: NodeId) {
        self.dirty = true;
        self.parent = Some(parent_id);
    }

    fn take_children(&mut self, target: &mut Vec<ChildEntry>) {
        self.dirty = true;
        for entry_opt in self.entries[0..self.len].iter_mut() {
            target.push(entry_opt.take().unwrap()); // must be filled if node is legal
        }
        self.len = 0;
    }

    // remember to update the parent after calling this!
    fn set_children(&mut self, source: Vec<ChildEntry>) {
        self.dirty = true;
        debug_assert!(source.len() <= self.entries.len()); // just to make sure
        let mut index = 0;
        for entry in source {
            self.entries[index] = Some(entry);
            index += 1;
        }
        self.len = index;
    }

    fn has_space(&self) -> bool {
        self.len < NODE_WIDTH
    }

    pub(super) fn insert(&mut self, bounding_box: Rectangle, node_id: NodeId) {
        self.dirty = true;
        self.entries[self.len] = Some(ChildEntry(bounding_box, node_id));
        self.len += 1;
    }

    fn update_bounding_box(&mut self, at_node_id: NodeId, new_bounding_box: Rectangle) {
        self.dirty = true;
        for entry in self.entries.iter_mut() {
            if let Some(ChildEntry(bbox, id)) = entry {
                if *id == at_node_id {
                    *bbox = new_bounding_box;
                    return;
                }
            }
        }
        panic!(
            "No entry matched node id={}, got entries for {:?}",
            at_node_id,
            self.entries
                .iter()
                .map(|e| e.map(|entry| entry.1))
                .collect::<Vec<Option<usize>>>()
        );
    }

    fn best_fit<V>(&self, value: &V) -> NodeId
    where
        V: Extending<Rectangle>,
    {
        let mut current: &ChildEntry =
            &self.entries[0].expect("Don't invoke best_fit on an uninitialised node");
        let mut current_enlargement = value.extend_area(current.0);
        for entry_opt in self.entries[1..self.len].iter() {
            let entry = entry_opt
                .as_ref()
                .expect("All entries < len should be Some!");
            let entry_enlargement = value.extend_area(entry.0);
            match current_enlargement.partial_cmp(&entry_enlargement) {
                Some(Ordering::Less) | None => continue,
                Some(Ordering::Equal) => {
                    if current.0.area() > entry.0.area() {
                        current = entry;
                        current_enlargement = entry_enlargement;
                    }
                }
                Some(Ordering::Greater) => {
                    current = entry;
                    current_enlargement = entry_enlargement;
                }
            }
        }
        current.1
    }

    fn calculate_bounding_box(&self) -> Rectangle {
        Bounding::bound_all(self.filled_entries().map(|t| &t.0))
    }

    fn assert_invariants(&self, is_root: bool) {
        // consistency
        let actual_len = self.entries.iter().filter(|e| e.is_some()).count();
        assert_eq!(actual_len, self.len);
        if !is_root {
            // (3)
            assert!(MIN_FILL <= self.len);
        } else {
            // (5)
            assert!(2 <= self.len);
        }
    }
}

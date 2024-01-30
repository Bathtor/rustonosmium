use super::*;

use std::{cell::RefCell, rc::Rc};

pub type RTree<V> = nodes::RTree<V, InMemoryNodeManager<V>>;

pub fn new<V>() -> RTree<V> {
    nodes::RTree::new(InMemoryNodeManager::new())
}

pub fn with_capacity<V>(capacity: usize) -> RTree<V> {
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
    nodes::RTree::new(InMemoryNodeManager::with_capacity(num_nodes))
}

pub struct InMemoryNodeManager<V> {
    nodes: Vec<NodeRef<V>>,
}
impl<V> InMemoryNodeManager<V> {
    fn new() -> Self {
        InMemoryNodeManager { nodes: Vec::new() }
    }

    fn with_capacity(capacity: usize) -> Self {
        InMemoryNodeManager {
            nodes: Vec::with_capacity(capacity),
        }
    }
}

impl<V> NodeManager<V> for InMemoryNodeManager<V> {
    fn get(&self, id: NodeId) -> NodeRef<V> {
        let res: &NodeRef<V> = self
            .nodes
            .get(id)
            .expect("Never ask for node ids that don't exist!");
        let node: NodeRef<V> = Clone::clone(res);
        node
    }

    fn fresh_leaf(&mut self) -> IdRef<V> {
        let leaf = Rc::new(RefCell::new(Leaf::new()));
        let node = NodeRef::Leaf(leaf);
        let id = self.nodes.len();
        self.nodes.push(node.clone());
        IdRef { id, node }
    }

    fn fresh_internal(&mut self) -> IdRef<V> {
        let inode = Rc::new(RefCell::new(InternalNode::new()));
        let node = NodeRef::Internal(inode);
        let id = self.nodes.len();
        self.nodes.push(node.clone());
        IdRef { id, node }
    }

    fn flush(&mut self) {
        // no-op in memory
    }

    fn print_stats(&self) {
        println!("In-memory RTree Index holding {} nodes", self.nodes.len());
    }
}

impl<V> Default for InMemoryNodeManager<V> {
    fn default() -> Self {
        InMemoryNodeManager::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_basic() {
        let mut tree = new();
        crate::databases::indices::r_tree::tests::test_basic(tree);
    }
}

pub mod in_memory;
mod nodes;
pub mod on_disk;

use nodes::*;

pub trait NodeManager<V> {
    fn get(&self, id: NodeId) -> NodeRef<V>;

    fn fresh_leaf(&mut self) -> IdRef<V>;

    fn fresh_internal(&mut self) -> IdRef<V>;

    /// Flush all pending changes to disk (where used)
    fn flush(&mut self);

    fn print_stats(&self);
}

const NODE_WIDTH: usize = 32;
const MIN_FILL: usize = 8;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::planar::{Point, Rectangle};

    pub(super) fn test_basic<M>(mut tree: RTree<Point, M>)
    where
        M: NodeManager<Point>,
    {
        tree.assert_invariants();
        assert!(tree.is_empty());
        let centre = Point { x: 0.0, y: 0.0 };
        tree.insert(centre);
        tree.assert_invariants();
        assert!(!tree.is_empty());
        let unit_rec = Rectangle {
            low_corner: Point { x: -1.0, y: -1.0 },
            high_corner: Point { x: 1.0, y: 1.0 },
        };

        {
            let res = tree.search(&unit_rec);
            assert_eq!(1, res.len());
            assert_eq!(centre, res[0]);
        }
        {
            tree.insert(Point { x: 2.0, y: -2.0 });
            // gotta filter after, since search will return all entries in the same leaf
            let res: Vec<Point> = tree.search(&unit_rec).into_iter().filter(|e| e == &centre).collect();
            assert_eq!(1, res.len());
            assert_eq!(centre, res[0]);
        }

        {
            // grow the tree
            for i in 3..300 {
                let point = Point {
                    x: i as f64,
                    y: i as f64,
                };
                //println!("Adding point: {:?}", point);
                tree.insert(point);
                tree.assert_invariants();
            }
            for i in 3..300 {
                let point = Point {
                    x: (i as f64) / 100.0,
                    y: (i as f64) / -50.0,
                };
                //println!("Adding point: {:?}", point);
                tree.insert(point);
                tree.assert_invariants();
            }
            let res = tree.search(&unit_rec);
            println!("Orginal result: {:?}", res);
            let filtered: Vec<Point> = res.into_iter().filter(|e| e == &centre).collect();
            println!("Filtered result: {:?}", filtered);
            assert_eq!(1, filtered.len());
            assert_eq!(centre, filtered[0]);
        }
    }
}

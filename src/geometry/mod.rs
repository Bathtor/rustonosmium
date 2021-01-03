pub mod planar;
pub mod spherical;

/// Types implementing this trait can be checked for intersections with `Rhs`
pub trait Intersecting<Rhs = Self> {
    type IntersectionShape;
    /// Returns `true` if `self` intersects with `other`.
    fn intersects(&self, other: &Rhs) -> bool;
    /// Produce the shape of the intersection, if possible
    fn intersection(&self, other: &Rhs) -> Option<Self::IntersectionShape>;
    /// Returns true if `other` not only intersects `self`, but is completely
    /// contained within the confines of this shape
    fn contains(&self, other: &Rhs) -> bool;
}

/// Can produce a unitless area
pub trait HasArea {
    /// Unitless agrea of this geometry
    ///
    /// Has not meaning, except for comparisons
    fn area(&self) -> f64;
}

pub trait Extending<Rhs = Self>
where
    Rhs: HasArea,
{
    /// Produces a new geometry of the given type
    /// that is like `geometry`, but extended to fit `self`
    fn extend(&self, geometry: Rhs) -> Rhs;

    /// Same is extend, but only return the new area
    fn extend_area(&self, geometry: Rhs) -> f64 {
        let original_area = geometry.area();
        let extended = self.extend(geometry);
        let extended_area = extended.area();
        extended_area - original_area
    }
}

pub trait Bounding: Sized + 'static {
    type SplitUse = Self;

    /// Calculate a rectangle that contains all entries in a slice of `T`
    fn bound_all<'a>(entries: impl Iterator<Item = &'a Self>) -> planar::Rectangle;

    /// Produce a geometrically "good" split
    ///
    /// `min_fill` is the minimum number of entries in each node after split
    /// `max_fill` is the maximum number of entries that fit into a node
    fn find_split<T>(entries: Vec<T>, min_fill: usize, max_fill: usize) -> (Vec<T>, Vec<T>)
    where
        T: AsRef<Self::SplitUse>;
}

use can2k_geo::abbreviations::*;

pub mod simplify;

/// A lazy implementation of distance that allows two different models to be used.
///
/// For operations that can be compared to bounds with error, the cheap model will be used first,
/// and if an operation is already unambigious with the cheap model, then there's no need to
/// calculate the expensive one as well.
pub struct LazyDistance<Cheap, Expensive>
where
    Cheap: 'static,
    Expensive: 'static,
{
    /// A cheap model to use, if it's unambiguous.
    cheap_model: &'static Cheap,
    /// The more accurate, but expensive model.
    expensive_model: &'static Expensive,
    /// Defines the uncertainty range of cheap_distance+-error.
    max_relative_error_of_cheap: f64,
    result_with_cheap_model: Option<Meters>,
    result_with_expensive_model: Option<Meters>,
}

// TODO: Continue this.q
// impl<Cheap, Expensive> LazyDistance<Cheap, Expensive>
// where
//     Cheap: GeodesicDistance + 'static,
//     Expensive: GeodesicDistance + 'static,
// {
// }

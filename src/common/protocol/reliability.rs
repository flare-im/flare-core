#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reliability {
    AtLeastOnce,
    BestEffort,
}

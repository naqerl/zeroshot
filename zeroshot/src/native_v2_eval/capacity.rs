/// Slice 2 supplies a capacity source for platform/local runners. Slice 1 declares only the seam.
pub trait CapacitySource: Send + Sync {
    fn available_slots(&self) -> u32;
    fn queue_wait_ms(&self) -> Option<u64>;
}

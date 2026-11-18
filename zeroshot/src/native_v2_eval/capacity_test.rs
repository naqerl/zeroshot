use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::{
    Capacity, CapacityError, CapacitySource, CapacitySourceKind, StaticCapacity, resolve_capacity,
    run_bounded,
};

#[test]
fn platform_capacity_wins_over_explicit_concurrency() {
    let capacity = resolve_capacity(Some(&StaticCapacity(3)), Some(8)).unwrap();
    assert_eq!(capacity, Capacity::new(3, CapacitySourceKind::Platform));
}

#[test]
fn concurrency_is_used_when_platform_has_no_capacity() {
    let capacity = resolve_capacity(None, Some(4)).unwrap();
    assert_eq!(capacity, Capacity::new(4, CapacitySourceKind::Concurrency));
    assert_eq!(
        serde_json::to_value(capacity).unwrap(),
        serde_json::json!({"concurrency": 4, "capacitySource": "concurrency"})
    );
}

#[test]
fn missing_capacity_falls_back_to_one_and_rejects_zero() {
    assert_eq!(
        resolve_capacity(None, None).unwrap(),
        Capacity::new(1, CapacitySourceKind::Fallback)
    );
    assert_eq!(
        resolve_capacity(None, Some(0)),
        Err(CapacityError::OutOfRange)
    );
    assert_eq!(StaticCapacity(0).capacity(), None);
}

struct RawCapacity(usize);

impl CapacitySource for RawCapacity {
    fn capacity(&self) -> Option<usize> {
        Some(self.0)
    }
}

#[test]
fn invalid_platform_capacity_falls_back_instead_of_being_trusted() {
    assert_eq!(
        resolve_capacity(Some(&RawCapacity(0)), Some(3)).unwrap(),
        Capacity::new(3, CapacitySourceKind::Concurrency)
    );
    assert_eq!(
        resolve_capacity(Some(&RawCapacity(4_097)), None).unwrap(),
        Capacity::new(1, CapacitySourceKind::Fallback)
    );
    assert_eq!(
        resolve_capacity(Some(&RawCapacity(usize::MAX)), Some(5)).unwrap(),
        Capacity::new(5, CapacitySourceKind::Concurrency)
    );
}

#[tokio::test]
async fn bounded_scheduler_preserves_input_order_and_observes_the_limit() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let results = run_bounded((0..6).collect(), 2, {
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        move |index, value| {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            async move {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5 + (5 - index) as u64)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                value * 2
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        results,
        vec![(0, 0), (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)]
    );
    assert!(peak.load(Ordering::SeqCst) <= 2);
}

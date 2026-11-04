//! Blocked pools: classification, backoff, expiry and what a queued job is told.

use serde_json::json;

use super::super::clock::now_ms;
use super::super::model::HubState;
use super::super::pool::{BlockedKind, PoolEventKind, PoolPhase, block_backoff_ms, classify_block};
use super::super::provision::Refusal;
use super::pool::{Rig, job_named, rig};

/// Every create the fake provider saw.
fn attempts(rig: &Rig) -> usize {
    rig.cloud.with(|cloud| cloud.attempts.len())
}

fn count(state: &HubState, phase: PoolPhase) -> usize {
    state
        .pool_events
        .iter()
        .filter(|event| event.kind == PoolEventKind::Pool && event.phase == phase)
        .count()
}

/// The time left on the pool's block and its consecutive-block count, measured after a pass.
async fn blocked(rig: &Rig) -> (u64, u32) {
    let state = rig.state().await;
    let pool = &state.pools[0];
    assert!(pool.blocked.is_some(), "pool is not blocked");
    assert_eq!(pool.blocked_kind, Some(BlockedKind::Quota), "{pool:?}");
    (
        pool.blocked_until_ms.expect("expiry") - now_ms(),
        pool.blocked_attempts,
    )
}

fn expect_wait(actual: u64, expected: u64) {
    assert!(
        actual <= expected && actual + 5_000 >= expected,
        "wait {actual}ms is not about {expected}ms"
    );
}

/// Make the current block due now, as a dispatcher pass after wall-clock time would see it.
async fn expire(rig: &Rig) {
    rig.set(|state| state.pools[0].blocked_until_ms = Some(1))
        .await;
}

async fn block_on_quota(rig: &Rig) {
    rig.cloud
        .with(|cloud| cloud.refuse_all = Some(Refusal::Quota));
    rig.enqueue("hj_one").await;
    rig.pass().await;
}

#[tokio::test]
async fn a_quota_block_expires_after_a_minute_and_only_then_retries() {
    let rig = rig();
    rig.add_pool(1).await;
    block_on_quota(&rig).await;

    let (wait, consecutive) = blocked(&rig).await;
    expect_wait(wait, 60_000);
    assert_eq!(consecutive, 1);
    assert_eq!(
        attempts(&rig),
        1,
        "the first attempt is the one that blocked"
    );

    rig.pass().await;
    assert_eq!(attempts(&rig), 1, "no create before the expiry");
    assert_eq!(count(&rig.state().await, PoolPhase::Blocked), 1);

    expire(&rig).await;
    rig.pass().await;
    assert_eq!(
        attempts(&rig),
        2,
        "the next pass after the expiry retries the lease"
    );
    assert_eq!(blocked(&rig).await.1, 2, "the retry is the second block");
}

#[tokio::test]
async fn consecutive_failures_walk_the_backoff_and_a_success_resets_it() {
    let rig = rig();
    rig.add_pool(1).await;
    block_on_quota(&rig).await;
    expect_wait(blocked(&rig).await.0, 60_000);

    expire(&rig).await;
    rig.pass().await;
    expect_wait(blocked(&rig).await.0, 120_000);

    expire(&rig).await;
    rig.pass().await;
    expect_wait(blocked(&rig).await.0, 300_000);

    rig.cloud.with(|cloud| cloud.refuse_all = None);
    expire(&rig).await;
    rig.pass().await;
    let state = rig.state().await;
    let pool = &state.pools[0];
    assert!(
        pool.blocked.is_none(),
        "a successful lease clears the block"
    );
    assert!(pool.blocked_kind.is_none());
    assert!(pool.blocked_until_ms.is_none());
    assert_eq!(
        pool.blocked_attempts, 0,
        "the next block starts at a minute"
    );
    assert_eq!(rig.cloud.servers(), 1);
}

#[tokio::test]
async fn resume_clears_the_block_and_the_next_dispatch_retries() {
    let rig = rig();
    rig.add_pool(1).await;
    block_on_quota(&rig).await;
    let before = attempts(&rig);

    rig.cloud.with(|cloud| cloud.refuse_all = None);
    let resumed = rig.service.resume_pool("burst").await.expect("resume");
    assert!(resumed["blocked"].is_null(), "{resumed}");
    assert!(resumed["blockedKind"].is_null(), "{resumed}");
    assert!(resumed["blockedUntil"].is_null(), "{resumed}");
    let state = rig.state().await;
    assert!(state.pools[0].blocked.is_none());
    assert_eq!(state.pools[0].blocked_attempts, 0);

    rig.pass().await;
    assert_eq!(attempts(&rig), before + 1);
    assert_eq!(rig.cloud.servers(), 1);
}

#[tokio::test]
async fn the_pool_view_and_the_waiting_job_name_the_kind_and_expiry() {
    let rig = rig();
    rig.add_pool(1).await;
    block_on_quota(&rig).await;
    rig.pass().await;

    let listed = rig.service.list_pools().await;
    let pool = &listed["pools"][0];
    assert_eq!(pool["blockedKind"], json!("quota"), "{listed}");
    let until = pool["blockedUntil"].as_str().expect("expiry in the view");
    let blocked = pool["blocked"].as_str().expect("blocked in the view");
    assert!(blocked.contains("create refused: Quota"), "{blocked}");
    let waiting = pool["waiting"].as_str().expect("waiting in the view");
    assert!(waiting.contains("blocked until"), "{waiting}");
    assert!(waiting.contains("(quota)"), "{waiting}");
    assert!(waiting.contains(until), "{waiting}");

    let state = rig.state().await;
    let job = job_named(&state, "hj_one");
    let waiting = job.pool_wait.clone().expect("waiting reason");
    assert!(waiting.contains("blocked until"), "{waiting}");
    assert!(waiting.contains("(quota)"), "{waiting}");
    assert!(waiting.contains(until), "{waiting}");
    assert_eq!(job.view(now_ms())["waiting"], json!(waiting));
}

#[tokio::test]
async fn one_event_per_block_and_one_when_it_unblocks() {
    let rig = rig();
    rig.add_pool(1).await;
    block_on_quota(&rig).await;
    assert_eq!(attempts(&rig), 1);
    let state = rig.state().await;
    assert_eq!(count(&state, PoolPhase::Blocked), 1);
    let blocked = state
        .pool_events
        .iter()
        .find(|event| event.kind == PoolEventKind::Pool && event.phase == PoolPhase::Blocked)
        .expect("blocked event");
    assert_eq!(blocked.blocked_kind, Some(BlockedKind::Quota));
    assert!(blocked.blocked_until.is_some(), "{blocked:?}");

    for _ in 0..3 {
        rig.pass().await;
    }
    assert_eq!(attempts(&rig), 1, "a blocked pool makes no create calls");
    assert_eq!(
        count(&rig.state().await, PoolPhase::Blocked),
        1,
        "suppressed attempts record no extra pool event"
    );

    expire(&rig).await;
    rig.pass().await;
    assert_eq!(attempts(&rig), 2);
    assert_eq!(
        count(&rig.state().await, PoolPhase::Blocked),
        2,
        "one event for the retry boundary that failed again"
    );
    assert_eq!(
        count(&rig.state().await, PoolPhase::Resumed),
        1,
        "one event for the expiry that allowed the retry"
    );

    rig.service.resume_pool("burst").await.expect("resume");
    assert_eq!(
        count(&rig.state().await, PoolPhase::Resumed),
        2,
        "the manual override records one unblock event"
    );
    assert_eq!(count(&rig.state().await, PoolPhase::Blocked), 2);
}

#[test]
fn provider_text_selects_the_block_kind_and_the_backoff_schedule() {
    for (text, kind) in [
        ("403 Primary IP limit exceeded", BlockedKind::Quota),
        ("401 Unauthorized: invalid credential", BlockedKind::Auth),
        ("no capacity for cpx31 in fsn1", BlockedKind::Capacity),
        ("503 Service Unavailable", BlockedKind::Provider),
        ("the provider said no", BlockedKind::Unknown),
    ] {
        assert_eq!(classify_block(text), kind, "{text}");
    }

    for (previous, wait) in [(0, 60_000), (1, 120_000), (2, 300_000), (3, 900_000)] {
        assert_eq!(block_backoff_ms(BlockedKind::Quota, previous), wait);
    }
    assert_eq!(block_backoff_ms(BlockedKind::Quota, 4), 1_800_000);
    assert_eq!(block_backoff_ms(BlockedKind::Quota, 50), 1_800_000);
    assert_eq!(block_backoff_ms(BlockedKind::Auth, 0), 1_800_000);
    assert_eq!(block_backoff_ms(BlockedKind::Unknown, 5), 1_800_000);
}

/// A state file written before blocks had an expiry must not park the pool forever: the first
/// pass after the upgrade treats it as due, clears it and retries the queued job.
#[tokio::test]
async fn a_legacy_block_without_an_expiry_retries_on_the_first_pass() {
    let rig = rig();
    rig.add_pool(1).await;
    rig.set(|state| {
        state.pools[0].blocked = Some("403 Primary IP limit exceeded".to_owned());
        state.pools[0].blocked_kind = None;
        state.pools[0].blocked_until_ms = None;
    })
    .await;
    rig.enqueue("hj_one").await;

    rig.pass().await;
    let state = rig.state().await;
    let pool = &state.pools[0];
    assert!(pool.blocked.is_none(), "{pool:?}");
    assert!(pool.blocked_until_ms.is_none());
    assert_eq!(rig.cloud.servers(), 1, "the queued job got its server");
    assert_eq!(count(&state, PoolPhase::Resumed), 1);
}

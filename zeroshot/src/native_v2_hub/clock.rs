use std::time::{SystemTime, UNIX_EPOCH};

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::error::HubError;

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Parse `90s`, `10m`, `2h`, `7d`, `1w`, or combinations such as `1h30m`.
pub(crate) fn parse_duration_ms(text: &str) -> Result<u64, HubError> {
    let invalid = || {
        HubError::Usage(format!(
            "invalid duration {text:?}: use a number with s, m, h, d, or w, such as 90s or 1h30m"
        ))
    };
    let mut total: u64 = 0;
    let mut digits = String::new();
    for character in text.trim().chars() {
        if character.is_ascii_digit() {
            digits.push(character);
            continue;
        }
        let unit: u64 = match character {
            's' => 1_000,
            'm' => 60_000,
            'h' => 3_600_000,
            'd' => 86_400_000,
            'w' => 604_800_000,
            _ => return Err(invalid()),
        };
        let amount: u64 = digits.parse().map_err(|_| invalid())?;
        digits.clear();
        total = amount
            .checked_mul(unit)
            .and_then(|value| total.checked_add(value))
            .ok_or_else(invalid)?;
    }
    if !digits.is_empty() || total == 0 {
        return Err(invalid());
    }
    Ok(total)
}

/// Parse an RFC 3339 instant into Unix milliseconds.
pub(crate) fn parse_rfc3339_ms(text: &str) -> Option<u64> {
    let instant = OffsetDateTime::parse(text, &Rfc3339).ok()?;
    u64::try_from(instant.unix_timestamp_nanos() / 1_000_000).ok()
}

/// RFC 3339 time, or `+DURATION` (or bare DURATION) after `now_ms`.
pub(crate) fn parse_future_ms(text: &str, now_ms: u64) -> Result<u64, HubError> {
    if let Some(instant) = parse_rfc3339_ms(text) {
        return Ok(instant);
    }
    let duration = parse_duration_ms(text.strip_prefix('+').unwrap_or(text))?;
    Ok(now_ms.saturating_add(duration))
}

/// Parse an `--expires` value, which must name a time after `now_ms`. A duration is measured
/// from now, so a zero or malformed duration is invalid just like an instant in the past.
pub(crate) fn parse_expiry_ms(text: &str, now_ms: u64) -> Result<u64, HubError> {
    let at = parse_future_ms(text, now_ms)?;
    if at <= now_ms {
        return Err(HubError::Usage(format!(
            "invalid --expires {text:?}: the deadline must be in the future"
        )));
    }
    Ok(at)
}

/// RFC 3339 time, or DURATION before `now_ms`.
pub(crate) fn parse_past_ms(text: &str, now_ms: u64) -> Result<u64, HubError> {
    if let Some(instant) = parse_rfc3339_ms(text) {
        return Ok(instant);
    }
    let duration = parse_duration_ms(text.strip_prefix('-').unwrap_or(text))?;
    Ok(now_ms.saturating_sub(duration))
}

fn instant(ms: u64) -> OffsetDateTime {
    i128::from(ms)
        .checked_mul(1_000_000)
        .and_then(|nanos| OffsetDateTime::from_unix_timestamp_nanos(nanos).ok())
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

pub(crate) fn rfc3339(ms: u64) -> String {
    instant(ms)
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

/// UTC calendar day used by `stats --by day`.
pub(crate) fn utc_day(ms: u64) -> String {
    instant(ms).date().to_string()
}

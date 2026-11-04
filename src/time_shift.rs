use crate::error::ReplayError;
use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Utc};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Unit of a numeric epoch timestamp (or the textual RFC 3339 form).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ValueEnum)]
pub enum TimestampUnit {
    /// Detect the unit from the magnitude of the value, or parse text formats.
    #[default]
    #[serde(rename = "auto")]
    #[value(name = "auto")]
    Auto,
    /// Epoch seconds.
    #[serde(rename = "s")]
    #[value(name = "s")]
    Seconds,
    /// Epoch milliseconds.
    #[serde(rename = "ms")]
    #[value(name = "ms")]
    Millis,
    /// Epoch microseconds.
    #[serde(rename = "us")]
    #[value(name = "us")]
    Micros,
    /// Epoch nanoseconds.
    #[serde(rename = "ns")]
    #[value(name = "ns")]
    Nanos,
    /// Textual timestamps only (RFC 3339 and common database formats).
    #[serde(rename = "rfc3339")]
    #[value(name = "rfc3339")]
    Rfc3339,
}

/// How payload timestamps behave when the replay speed is not 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum EventTimeMode {
    /// Payload timestamps follow the compressed schedule (offsets divided by the speed).
    #[default]
    Compress,
    /// Payload timestamps keep the original spacing; only pacing follows the speed.
    Preserve,
}

fn parse_text_timestamp(trimmed: &str) -> Option<DateTime<Utc>> {
    // RFC 3339 / ISO 8601 with timezone (e.g., 2026-10-30T10:00:00Z)
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.with_timezone(&Utc));
    }

    // Timezone-naive database formats are treated as UTC.
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
    ];

    for fmt in &formats {
        if let Ok(naive) = NaiveDateTime::parse_from_str(trimmed, fmt) {
            return Some(Utc.from_utc_datetime(&naive));
        }
    }
    None
}

/// Guess the epoch unit of an integer from its magnitude.
pub fn detect_epoch_unit(ts: i64) -> TimestampUnit {
    let magnitude = ts.unsigned_abs();
    if magnitude > 1_000_000_000_000_000_000 {
        TimestampUnit::Nanos
    } else if magnitude > 1_000_000_000_000_000 {
        TimestampUnit::Micros
    } else if magnitude > 1_000_000_000_000 {
        TimestampUnit::Millis
    } else {
        TimestampUnit::Seconds
    }
}

fn resolve_unit(ts: i64, unit: TimestampUnit) -> TimestampUnit {
    match unit {
        TimestampUnit::Auto | TimestampUnit::Rfc3339 => detect_epoch_unit(ts),
        other => other,
    }
}

/// Convert an integer epoch value to a timestamp. `Auto` detects the unit.
pub fn epoch_to_datetime(ts: i64, unit: TimestampUnit) -> Option<DateTime<Utc>> {
    match resolve_unit(ts, unit) {
        TimestampUnit::Seconds => Utc.timestamp_opt(ts, 0).single(),
        TimestampUnit::Millis => Utc.timestamp_millis_opt(ts).single(),
        TimestampUnit::Micros => Utc.timestamp_micros(ts).single(),
        TimestampUnit::Nanos => Utc
            .timestamp_opt(
                ts.div_euclid(1_000_000_000),
                ts.rem_euclid(1_000_000_000) as u32,
            )
            .single(),
        TimestampUnit::Auto | TimestampUnit::Rfc3339 => None,
    }
}

/// Convert a timestamp to an integer epoch value in the given (concrete) unit.
pub fn datetime_to_epoch(dt: DateTime<Utc>, unit: TimestampUnit) -> i64 {
    match unit {
        TimestampUnit::Millis => dt.timestamp_millis(),
        TimestampUnit::Micros => dt.timestamp_micros(),
        TimestampUnit::Nanos => dt.timestamp_nanos_opt().unwrap_or(i64::MAX),
        _ => dt.timestamp(),
    }
}

/// Parse a textual or integer-epoch timestamp, auto-detecting its format and unit.
pub fn parse_timestamp(s: &str) -> Result<DateTime<Utc>, ReplayError> {
    parse_timestamp_with_unit(s, TimestampUnit::Auto)
}

/// Parse a textual or integer-epoch timestamp using an explicit unit.
///
/// With a numeric unit the integer is interpreted in that unit; with `Rfc3339` only
/// textual forms are accepted; with `Auto` the unit is detected from the magnitude.
pub fn parse_timestamp_with_unit(
    s: &str,
    unit: TimestampUnit,
) -> Result<DateTime<Utc>, ReplayError> {
    let trimmed = s.trim();

    if let Some(dt) = parse_text_timestamp(trimmed) {
        return Ok(dt);
    }

    if unit != TimestampUnit::Rfc3339 {
        if let Ok(ts) = trimmed.parse::<i64>() {
            if let Some(dt) = epoch_to_datetime(ts, unit) {
                return Ok(dt);
            }
        }
    }

    Err(ReplayError::TimestampParse(format!(
        "Cannot parse timestamp: '{s}'"
    )))
}

/// Interpret a JSON value (string or integer) as a timestamp.
pub fn value_to_datetime(value: &Value, unit: TimestampUnit) -> Option<DateTime<Utc>> {
    match value {
        Value::String(s) => parse_timestamp_with_unit(s, unit).ok(),
        Value::Number(n) => n.as_i64().and_then(|ts| epoch_to_datetime(ts, unit)),
        _ => None,
    }
}

/// Render `new_time` in the same representation as `original`: integer epochs stay
/// integer epochs in the same unit, anything else becomes an RFC 3339 string.
pub fn render_like(original: &Value, new_time: DateTime<Utc>, unit: TimestampUnit) -> Value {
    if let Value::Number(n) = original {
        if let Some(ts) = n.as_i64() {
            return Value::from(datetime_to_epoch(new_time, resolve_unit(ts, unit)));
        }
    }
    Value::String(new_time.to_rfc3339())
}

/// Shift a timestamp value by `offset`, keeping its representation.
/// Returns `None` when the value cannot be interpreted as a timestamp.
pub fn shift_value(original: &Value, offset: Duration, unit: TimestampUnit) -> Option<Value> {
    let dt = value_to_datetime(original, unit)?;
    Some(render_like(original, dt + offset, unit))
}

fn duration_as_nanos_f64(d: Duration) -> f64 {
    d.num_seconds() as f64 * 1e9 + d.subsec_nanos() as f64
}

fn duration_from_nanos_f64(ns: f64) -> Duration {
    let secs = (ns / 1e9).trunc();
    let rem = ns - secs * 1e9;
    Duration::seconds(secs as i64) + Duration::nanoseconds(rem as i64)
}

/// Divide a duration by the speed. A speed of exactly 1 returns the duration untouched.
fn scale_by_speed(d: Duration, speed: f64) -> Duration {
    if speed == 1.0 {
        d
    } else {
        duration_from_nanos_f64(duration_as_nanos_f64(d) / speed)
    }
}

#[derive(Debug, Clone)]
pub struct TimeShiftEngine {
    pub start_time: DateTime<Utc>,
    pub dataset_t0: Option<DateTime<Utc>>,
    pub dataset_duration: Duration,
    pub loop_count: u64,
    pub advance_loop_timestamps: bool,
    pub loop_id_salt: bool,
    pub shift_timestamps: bool,
    pub last_emitted_time: Option<DateTime<Utc>>,
    /// Speed multiplier `s`; `0.0` means unthrottled (no pacing).
    pub speed: f64,
    pub event_time: EventTimeMode,
}

impl TimeShiftEngine {
    pub fn new(shift_timestamps: bool, advance_loop_timestamps: bool, loop_id_salt: bool) -> Self {
        Self {
            start_time: Utc::now(),
            dataset_t0: None,
            dataset_duration: Duration::zero(),
            loop_count: 0,
            advance_loop_timestamps,
            loop_id_salt,
            shift_timestamps,
            last_emitted_time: None,
            speed: 1.0,
            event_time: EventTimeMode::Compress,
        }
    }

    pub fn with_speed(mut self, speed: f64) -> Self {
        self.speed = speed;
        self
    }

    pub fn with_event_time(mut self, mode: EventTimeMode) -> Self {
        self.event_time = mode;
        self
    }

    pub fn set_dataset_bounds(&mut self, t0: DateTime<Utc>, t_last: DateTime<Utc>) {
        self.dataset_t0 = Some(t0);
        let span = t_last - t0;
        self.dataset_duration = if span > Duration::zero() {
            span
        } else {
            Duration::seconds(1)
        };
    }

    fn loop_offset(&self) -> Duration {
        let count = i32::try_from(self.loop_count).unwrap_or(i32::MAX);
        self.dataset_duration * count
    }

    fn relative_offset(&mut self, raw_time: DateTime<Utc>) -> Duration {
        let t0 = *self.dataset_t0.get_or_insert(raw_time);
        raw_time - t0
    }

    /// Offset from the replay start at which the record is due on the wall clock:
    /// `(delta + loop_count * dataset_duration) / speed`. `None` when unthrottled.
    pub fn schedule_offset(&mut self, raw_time: DateTime<Utc>) -> Option<Duration> {
        if self.speed <= 0.0 {
            return None;
        }
        let total = self.relative_offset(raw_time) + self.loop_offset();
        Some(scale_by_speed(total, self.speed))
    }

    /// The timestamp written into the payload for a record.
    ///
    /// With shifting disabled the raw timestamp is returned unchanged. Otherwise:
    /// `compress` gives `start + (delta + loops * dataset_duration) / speed`, `preserve`
    /// gives `start + delta + loops * dataset_duration`. The loop term applies only when
    /// loop timestamps advance. Unthrottled `compress` follows the wall clock.
    pub fn calculate_emitted_time(&mut self, raw_time: DateTime<Utc>) -> DateTime<Utc> {
        if !self.shift_timestamps {
            return raw_time;
        }

        let mut total = self.relative_offset(raw_time);
        if self.advance_loop_timestamps && self.loop_count > 0 {
            total += self.loop_offset();
        }

        let mut emitted = match self.event_time {
            EventTimeMode::Preserve => self.start_time + total,
            EventTimeMode::Compress if self.speed > 0.0 => {
                self.start_time + scale_by_speed(total, self.speed)
            }
            EventTimeMode::Compress => Utc::now(),
        };

        // Enforce strict monotonicity (no backward jitter)
        if self.advance_loop_timestamps || self.loop_count == 0 {
            if let Some(last) = self.last_emitted_time {
                if emitted <= last {
                    emitted = last + Duration::nanoseconds(1);
                }
            }
            self.last_emitted_time = Some(emitted);
        }

        emitted
    }

    pub fn salt_entity_id(&self, id: &str) -> String {
        if self.loop_id_salt && self.loop_count > 0 {
            format!("{}_L{}", id, self.loop_count)
        } else {
            id.to_string()
        }
    }

    pub fn advance_loop(&mut self) {
        self.loop_count += 1;
        if !self.advance_loop_timestamps {
            self.last_emitted_time = None;
        }
    }
}

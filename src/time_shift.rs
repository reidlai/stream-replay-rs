use crate::error::ReplayError;
use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Utc};

pub fn parse_timestamp(s: &str) -> Result<DateTime<Utc>, ReplayError> {
    let trimmed = s.trim();

    // 1. Try RFC 3339 / ISO 8601 with timezone (e.g., 2026-10-30T10:00:00Z)
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(dt.with_timezone(&Utc));
    }

    // 2. Try standard database formats: "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DDTHH:MM:SS"
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
            return Ok(Utc.from_utc_datetime(&naive));
        }
    }

    // 3. Try integer Unix epoch timestamps (seconds, ms, us, ns)
    if let Ok(ts) = trimmed.parse::<i64>() {
        // Guess scale by magnitude
        if ts > 1_000_000_000_000_000_000 {
            // Nanoseconds
            if let Some(dt) = Utc
                .timestamp_opt(ts / 1_000_000_000, (ts % 1_000_000_000) as u32)
                .single()
            {
                return Ok(dt);
            }
        } else if ts > 1_000_000_000_000_000 {
            // Microseconds
            if let Some(dt) = Utc.timestamp_micros(ts).single() {
                return Ok(dt);
            }
        } else if ts > 1_000_000_000_000 {
            // Milliseconds
            if let Some(dt) = Utc.timestamp_millis_opt(ts).single() {
                return Ok(dt);
            }
        } else {
            // Seconds
            if let Some(dt) = Utc.timestamp_opt(ts, 0).single() {
                return Ok(dt);
            }
        }
    }

    Err(ReplayError::TimestampParse(format!(
        "Cannot parse timestamp: '{s}'"
    )))
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
        }
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

    pub fn calculate_emitted_time(&mut self, raw_time: DateTime<Utc>) -> DateTime<Utc> {
        if !self.shift_timestamps {
            if self.advance_loop_timestamps && self.loop_count > 0 {
                let loop_offset = self.dataset_duration * (self.loop_count as i32);
                let mut emitted = raw_time + loop_offset;
                if let Some(last) = self.last_emitted_time {
                    if emitted <= last {
                        emitted = last + Duration::nanoseconds(1);
                    }
                }
                self.last_emitted_time = Some(emitted);
                return emitted;
            }
            return raw_time;
        }

        let t0 = match self.dataset_t0 {
            Some(t) => t,
            None => {
                self.dataset_t0 = Some(raw_time);
                raw_time
            }
        };

        // Delta between this record and dataset start
        let delta_record = raw_time - t0;

        // Base shifted timestamp: start_time + delta_record
        let mut emitted = self.start_time + delta_record;

        // Advance timestamps across loops: loop_count * dataset_duration
        if self.advance_loop_timestamps && self.loop_count > 0 {
            let loop_offset = self.dataset_duration * (self.loop_count as i32);
            emitted += loop_offset;
        }

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

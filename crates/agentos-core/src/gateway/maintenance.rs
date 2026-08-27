//! Bounded process-maintenance cadence, independent of shard idleness.
//!
//! The gateway owns the work itself because cron delivery and retention paths
//! are deployment concerns. Core owns the clock: it makes missed-tick behavior,
//! retention cadence, and lag evidence identical for every channel supervisor.

use std::time::Duration;
use thiserror::Error;
use tokio::time::{Instant, Interval, MissedTickBehavior};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum MaintenanceScheduleError {
    #[error("maintenance scan interval must be greater than zero")]
    ZeroScanInterval,
    #[error("retention interval must be at least the maintenance scan interval")]
    RetentionBeforeScan,
}

/// The two clocks a channel maintenance supervisor follows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaintenanceCadence {
    scan_interval: Duration,
    retention_interval: Duration,
}

impl MaintenanceCadence {
    pub fn new(
        scan_interval: Duration,
        retention_interval: Duration,
    ) -> Result<Self, MaintenanceScheduleError> {
        if scan_interval.is_zero() {
            return Err(MaintenanceScheduleError::ZeroScanInterval);
        }
        if retention_interval < scan_interval {
            return Err(MaintenanceScheduleError::RetentionBeforeScan);
        }
        Ok(Self {
            scan_interval,
            retention_interval,
        })
    }

    /// Start after one complete scan interval. Maintenance never runs as an
    /// unbounded startup side effect while channels are still registering.
    pub fn start(self) -> MaintenanceTicker {
        let now = Instant::now();
        let first_scan = now + self.scan_interval;
        let mut scans = tokio::time::interval_at(first_scan, self.scan_interval);
        // A suspended or overloaded process runs one delayed pass and resumes
        // its cadence from there; it never bursts through every missed tick.
        scans.set_missed_tick_behavior(MissedTickBehavior::Delay);
        MaintenanceTicker {
            scans,
            retention_interval: self.retention_interval,
            next_retention: now + self.retention_interval,
            sequence: 0,
        }
    }
}

/// One scheduled maintenance wake-up and its diagnostic timing evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaintenanceTick {
    pub sequence: u64,
    pub scheduled_at: Instant,
    pub started_at: Instant,
    pub lag: Duration,
    /// Retention, settled-ingress pruning, and completed-job reaping share the
    /// slower retention cadence and one lease.
    pub retention_due: bool,
}

pub struct MaintenanceTicker {
    scans: Interval,
    retention_interval: Duration,
    next_retention: Instant,
    sequence: u64,
}

impl MaintenanceTicker {
    pub async fn tick(&mut self) -> MaintenanceTick {
        let scheduled_at = self.scans.tick().await;
        let started_at = Instant::now();
        let retention_due = started_at >= self.next_retention;
        if retention_due {
            // Delay rather than catch up in a burst after suspension. The lag
            // below tells the operator how late this pass actually began.
            self.next_retention = started_at + self.retention_interval;
        }
        self.sequence = self.sequence.saturating_add(1);
        MaintenanceTick {
            sequence: self.sequence,
            scheduled_at,
            started_at,
            lag: started_at.saturating_duration_since(scheduled_at),
            retention_due,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_cadences_fail_before_a_timer_is_built() {
        assert_eq!(
            MaintenanceCadence::new(Duration::ZERO, Duration::from_secs(1)),
            Err(MaintenanceScheduleError::ZeroScanInterval)
        );
        assert_eq!(
            MaintenanceCadence::new(Duration::from_secs(2), Duration::from_secs(1)),
            Err(MaintenanceScheduleError::RetentionBeforeScan)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retention_follows_its_slower_cadence_without_catch_up() {
        let mut ticker = MaintenanceCadence::new(Duration::from_secs(10), Duration::from_secs(30))
            .expect("the cadence is valid")
            .start();

        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(!ticker.tick().await.retention_due);
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(!ticker.tick().await.retention_due);
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(ticker.tick().await.retention_due);
    }
}

use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

/// Reconnect delay policy: exponential growth with jitter, optional retry limit.
#[derive(Debug, Clone, PartialEq)]
pub struct Backoff {
    initial: Duration,
    factor: f64,
    max: Duration,
    jitter: f64,
    max_retries: Option<u32>,
    reset_after: Duration,
}

impl Backoff {
    /// Doubles from `initial` up to `max`, 20 % jitter, unlimited retries, reset after 60 s healthy.
    pub fn exponential(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            factor: 2.0,
            max: max.max(initial),
            jitter: 0.2,
            max_retries: None,
            reset_after: Duration::from_secs(60),
        }
    }

    /// Always waits `delay`, without jitter.
    pub fn constant(delay: Duration) -> Self {
        Self::exponential(delay, delay).factor(1.0).jitter(0.0)
    }

    /// Growth factor per attempt, at least 1.
    pub fn factor(mut self, factor: f64) -> Self {
        self.factor = factor.max(1.0);
        self
    }

    /// Fraction in `0.0..=1.0` by which a delay is randomly shortened.
    pub fn jitter(mut self, jitter: f64) -> Self {
        self.jitter = jitter.clamp(0.0, 1.0);
        self
    }

    /// Consecutive failed attempts after which the collector gives up and the engine
    /// ends with [`ExitReason::CollectorFailed`](crate::ExitReason::CollectorFailed).
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = Some(retries);
        self
    }

    /// Connection uptime after which the delay starts again at the initial value.
    pub fn reset_after(mut self, healthy: Duration) -> Self {
        self.reset_after = healthy;
        self
    }

    /// Uptime after which the backoff resets.
    pub fn healthy_after(&self) -> Duration {
        self.reset_after
    }

    pub(crate) fn start(&self) -> BackoffState {
        BackoffState {
            policy: self.clone(),
            attempt: 0,
            current: self.initial,
            rng: seed(),
        }
    }
}

impl Default for Backoff {
    /// `exponential(100 ms, 30 s)`.
    fn default() -> Self {
        Self::exponential(Duration::from_millis(100), Duration::from_secs(30))
    }
}

pub(crate) struct BackoffState {
    policy: Backoff,
    attempt: u32,
    current: Duration,
    rng: u64,
}

impl BackoffState {
    /// The next delay, or `None` once the retry limit is reached.
    pub(crate) fn next_delay(&mut self) -> Option<Duration> {
        if self
            .policy
            .max_retries
            .is_some_and(|max| self.attempt >= max)
        {
            return None;
        }
        self.attempt += 1;
        let base = self.current;
        self.current = base.mul_f64(self.policy.factor).min(self.policy.max);
        let cut = self.policy.jitter * self.next_unit();
        Some(base.mul_f64(1.0 - cut))
    }

    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
        self.current = self.policy.initial;
    }

    pub(crate) fn attempts(&self) -> u32 {
        self.attempt
    }

    fn next_unit(&mut self) -> f64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        (self.rng >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn seed() -> u64 {
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u8(0);
    hasher.finish() | 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_until_max_and_resets() {
        let mut state =
            Backoff::exponential(Duration::from_millis(100), Duration::from_millis(350))
                .jitter(0.0)
                .start();
        let delays: Vec<_> = (0..4)
            .map(|_| state.next_delay().unwrap().as_millis())
            .collect();
        assert_eq!(delays, [100, 200, 350, 350]);
        state.reset();
        assert_eq!(state.next_delay(), Some(Duration::from_millis(100)));
    }

    #[test]
    fn stops_after_max_retries() {
        let mut state = Backoff::constant(Duration::from_millis(5))
            .max_retries(2)
            .start();
        assert!(state.next_delay().is_some());
        assert!(state.next_delay().is_some());
        assert_eq!(state.next_delay(), None);
    }

    #[test]
    fn jitter_only_shortens() {
        let mut state = Backoff::constant(Duration::from_millis(100))
            .jitter(0.5)
            .start();
        assert!(
            std::iter::repeat_with(|| state.next_delay().unwrap())
                .take(100)
                .all(|d| d <= Duration::from_millis(100) && d >= Duration::from_millis(50))
        );
    }
}

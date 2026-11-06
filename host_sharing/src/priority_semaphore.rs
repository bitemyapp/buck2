/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Weighted local admission, ordered by priority before the configured tie policy.
//! Priority never preempts running work or changes the number of permits charged.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use parking_lot::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

type Key = (Reverse<i32>, u64);

struct Waiter {
    permits: usize,
    granted: bool,
    waker: Option<Waker>,
}

struct State {
    available: usize,
    next: u64,
    fifo: bool,
    waiters: BTreeMap<Key, Waiter>,
}

impl State {
    // Callers wake the returned tasks only after releasing the mutex.
    fn dispatch(&mut self) -> Vec<Waker> {
        let mut wake = Vec::new();
        let mut blocked_priority = None;
        for ((priority, _), waiter) in &mut self.waiters {
            if waiter.granted {
                continue;
            }
            if blocked_priority.is_some_and(|blocked| blocked != *priority) {
                break;
            }
            if waiter.permits > self.available {
                if self.fifo {
                    break;
                }
                blocked_priority = Some(*priority);
                continue;
            }
            self.available -= waiter.permits;
            waiter.granted = true;
            if let Some(waker) = waiter.waker.take() {
                wake.push(waker);
            }
        }
        wake
    }
}

fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

pub(crate) struct PrioritySemaphore {
    state: Arc<Mutex<State>>,
}

impl PrioritySemaphore {
    pub(crate) fn new(fifo: bool, permits: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                available: permits,
                next: 0,
                fifo,
                waiters: BTreeMap::new(),
            })),
        }
    }

    pub(crate) fn acquire(&self, permits: usize, priority: i32) -> Acquire<'_> {
        Acquire {
            semaphore: self,
            permits,
            priority,
            key: None,
            complete: false,
        }
    }
}

pub(crate) struct Acquire<'a> {
    semaphore: &'a PrioritySemaphore,
    permits: usize,
    priority: i32,
    key: Option<Key>,
    complete: bool,
}

impl Future for Acquire<'_> {
    type Output = PrioritySemaphoreGuard;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.complete, "acquisition polled after completion");
        let mut state = this.semaphore.state.lock();
        // Uncontended actions need no queue node allocation.
        if this.key.is_none() && state.waiters.is_empty() && this.permits <= state.available {
            state.available -= this.permits;
            this.complete = true;
            return Poll::Ready(PrioritySemaphoreGuard {
                state: Arc::clone(&this.semaphore.state),
                permits: this.permits,
            });
        }
        let key = *this.key.get_or_insert_with(|| {
            let key = (Reverse(this.priority), state.next);
            state.next = state.next.checked_add(1).expect("host-sharing sequence exhausted");
            state.waiters.insert(key, Waiter {
                permits: this.permits,
                granted: false,
                waker: None,
            });
            key
        });
        let wake = state.dispatch();
        let waiter = state.waiters.get_mut(&key).unwrap();
        let result = if waiter.granted {
            state.waiters.remove(&key);
            this.key = None;
            this.complete = true;
            Poll::Ready(PrioritySemaphoreGuard {
                state: Arc::clone(&this.semaphore.state),
                permits: this.permits,
            })
        } else {
            match &mut waiter.waker {
                Some(waker) => waker.clone_from(cx.waker()),
                slot @ None => *slot = Some(cx.waker().clone()),
            }
            Poll::Pending
        };
        drop(state);
        wake_all(wake);
        result
    }
}

impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        let Some(key) = self.key else { return };
        let mut state = self.semaphore.state.lock();
        let waiter = state.waiters.remove(&key).unwrap();
        // A canceled pending waiter owns nothing. A granted but not yet polled
        // waiter owns a reservation, which must be returned exactly once.
        if waiter.granted {
            state.available += waiter.permits;
        }
        let wake = state.dispatch();
        drop(state);
        wake_all(wake);
    }
}

pub(crate) struct PrioritySemaphoreGuard {
    state: Arc<Mutex<State>>,
    permits: usize,
}

impl Drop for PrioritySemaphoreGuard {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        state.available += self.permits;
        let wake = state.dispatch();
        drop(state);
        wake_all(wake);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn poll(acquire: &mut Acquire<'_>) -> Poll<PrioritySemaphoreGuard> {
        Pin::new(acquire).poll(&mut Context::from_waker(Waker::noop()))
    }

    fn ready(acquire: &mut Acquire<'_>) -> PrioritySemaphoreGuard {
        match poll(acquire) {
            Poll::Ready(guard) => guard,
            Poll::Pending => panic!("expected admission"),
        }
    }

    #[test]
    fn priority_passes_queued_work_without_preemption_and_ties_are_fifo() {
        let semaphore = PrioritySemaphore::new(true, 2);
        let active = ready(&mut semaphore.acquire(2, 0));
        let mut first = semaphore.acquire(2, 0);
        let mut second = semaphore.acquire(2, 0);
        let mut critical = semaphore.acquire(2, 8);
        assert!(poll(&mut first).is_pending());
        assert!(poll(&mut second).is_pending());
        assert!(poll(&mut critical).is_pending());
        assert_eq!(semaphore.state.lock().available, 0);
        drop(active);
        assert!(poll(&mut first).is_pending());
        let critical = ready(&mut critical);
        assert!(poll(&mut second).is_pending());
        drop(critical);
        assert!(poll(&mut second).is_pending());
        drop(ready(&mut first));
        drop(ready(&mut second));
        assert_eq!(semaphore.state.lock().available, 2);
    }

    #[test]
    fn cancellation_returns_only_owned_permits_including_unpolled_grants() {
        let semaphore = PrioritySemaphore::new(true, 3);
        let active = ready(&mut semaphore.acquire(2, 0));
        let mut canceled = semaphore.acquire(3, 9);
        let mut next = semaphore.acquire(1, 0);
        assert!(poll(&mut canceled).is_pending());
        assert!(poll(&mut next).is_pending());
        drop(canceled);
        // Removing an ungranted waiter must not release its requested weight.
        let next = ready(&mut next);
        assert_eq!(semaphore.state.lock().available, 0);
        let mut unpolled = semaphore.acquire(3, 0);
        let mut survivor = semaphore.acquire(3, 0);
        assert!(poll(&mut unpolled).is_pending());
        assert!(poll(&mut survivor).is_pending());
        drop(active);
        drop(next);
        drop(unpolled);
        drop(ready(&mut survivor));
        assert_eq!(semaphore.state.lock().available, 3);
    }

    #[test]
    fn fitting_ties_preserve_weight_bounds_but_do_not_bypass_higher_priority() {
        let semaphore = PrioritySemaphore::new(false, 3);
        let active = ready(&mut semaphore.acquire(2, 0));
        let mut large = semaphore.acquire(2, 5);
        let mut lower = semaphore.acquire(1, 4);
        let mut small = semaphore.acquire(1, 5);
        assert!(poll(&mut large).is_pending());
        assert!(poll(&mut lower).is_pending());
        let small = ready(&mut small);
        assert_eq!(semaphore.state.lock().available, 0);
        drop(active);
        let large = ready(&mut large);
        assert!(poll(&mut lower).is_pending());
        drop(small);
        drop(ready(&mut lower));
        drop(large);
        assert_eq!(semaphore.state.lock().available, 3);
    }

    #[test]
    fn granting_wakes_after_unlocking_and_unpolled_cancellation_releases() {
        struct CheckUnlocked {
            state: Arc<Mutex<State>>,
            wakes: std::sync::atomic::AtomicUsize,
        }
        impl std::task::Wake for CheckUnlocked {
            fn wake(self: Arc<Self>) {
                assert!(self.state.try_lock().is_some(), "wake called under scheduler lock");
                self.wakes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let semaphore = PrioritySemaphore::new(true, 1);
        let active = ready(&mut semaphore.acquire(1, 0));
        let check = Arc::new(CheckUnlocked {
            state: Arc::clone(&semaphore.state),
            wakes: std::sync::atomic::AtomicUsize::new(0),
        });
        let waker = Waker::from(Arc::clone(&check));
        let mut pending = semaphore.acquire(1, -1);
        assert!(Pin::new(&mut pending).poll(&mut Context::from_waker(&waker)).is_pending());
        drop(active);
        assert_eq!(check.wakes.load(std::sync::atomic::Ordering::SeqCst), 1);
        drop(pending);
        assert_eq!(semaphore.state.lock().available, 1);
    }
}

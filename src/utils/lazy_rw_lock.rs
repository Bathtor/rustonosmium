use std::sync::PoisonError;
pub use std::sync::RwLock;

pub type LockResult<T> = Result<T, PoisonError<()>>;

pub fn new_lazy_fill_rw_lock<T>() -> RwLock<Option<T>> {
    RwLock::new(None)
}

pub trait LazyFillRwLock<T> {
    fn get_or_load<G, L, R>(&self, getter: G, loader: L) -> LockResult<R>
    where
        G: Fn(&T) -> R,
        L: FnOnce() -> T;

    /// Clear out the content if `predicate` evaluates to `true.
    ///
    /// Return `true` if content was actually cleared.
    fn clear_when<P>(&self, predicate: P) -> LockResult<bool>
    where
        P: Fn(&T) -> bool;

    /// Reset the lock to its default state, whether its poisoned or not.
    fn reset(&self);
}

impl<T> LazyFillRwLock<T> for RwLock<Option<T>> {
    fn get_or_load<G, L, R>(&self, getter: G, loader: L) -> LockResult<R>
    where
        G: Fn(&T) -> R,
        L: FnOnce() -> T,
    {
        let read_lock = self.read().map_err(|_| {
            log::error!("A read lock was poisoned.");
            PoisonError::new(())
        })?;
        if let Some(instance) = read_lock.as_ref() {
            Ok(getter(instance))
        } else {
            drop(read_lock);
            let mut write_lock = self.write().map_err(|_| {
                log::error!("A write lock was poisoned.");
                PoisonError::new(())
            })?;
            if let Some(instance) = write_lock.as_ref() {
                Ok(getter(instance))
            } else {
                let instance = loader();
                let res = getter(&instance);
                *write_lock = Some(instance);
                Ok(res)
            }
        }
    }

    fn clear_when<P>(&self, predicate: P) -> LockResult<bool>
    where
        P: Fn(&T) -> bool,
    {
        let should_clear = {
            let read_lock = self.read().map_err(|_| {
                log::error!("A read lock was poisoned.");
                PoisonError::new(())
            })?;
            if let Some(instance) = read_lock.as_ref() {
                predicate(instance)
            } else {
                false
            }
        };
        if should_clear {
            let mut write_lock = self.write().map_err(|_| {
                log::error!("A write lock was poisoned.");
                PoisonError::new(())
            })?;
            // Check again, now that we have the write-lock.
            let actually_clear = if let Some(instance) = write_lock.as_ref() {
                predicate(instance)
            } else {
                false // was already cleared
            };
            if actually_clear {
                *write_lock = None;
            }
            Ok(actually_clear)
        } else {
            Ok(false)
        }
    }

    fn reset(&self) {
        match self.write() {
            Ok(mut guard) => *guard = None,
            Err(e) => {
                let mut guard = e.into_inner();
                *guard = None;
            }
        }
        self.clear_poison();
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use super::*;

    #[test]
    fn lazy_fill_lock_poison_recovery() {
        let lock = Arc::new(new_lazy_fill_rw_lock::<usize>());
        let res = lock.get_or_load(|v| *v, || 42).expect("No poison");
        assert_eq!(res, 42);
        lock.reset();
        let lock2 = lock.clone();
        let handle = thread::spawn(move || {
            // Ignore the result, since this should panic.
            let _ = lock2.get_or_load(|v: &usize| *v, || panic!("test panic please ignore"));
        });
        handle.join().expect_err("Thread should have failed");
        let _error = lock
            .get_or_load(|v: &usize| *v, || 43)
            .expect_err("Should have been poisoned");
        lock.reset();
        let res = lock.get_or_load(|v| *v, || 44).expect("No poison");
        assert_eq!(res, 44);
    }
}

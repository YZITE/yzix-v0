use async_channel::{unbounded, Receiver, Sender};
use futures_util::future::FutureExt;
use std::future::Future;

#[derive(Clone)]
pub struct Pool<T> {
    push: Sender<T>,
    pop: Receiver<T>,
}

impl<T> Default for Pool<T> {
    #[inline]
    fn default() -> Self {
        let (push, pop) = unbounded();
        Self { push, pop }
    }
}

impl<T: Send> Pool<T> {
    #[inline]
    pub fn pop(&self) -> impl Future<Output = T> + '_ {
        self.pop
            .recv()
            .map(|r| r.expect("unable to receive from pool"))
    }

    #[inline]
    pub fn push(&self, x: T) -> impl Future<Output = ()> + '_ {
        self.push
            .send(x)
            .map(|r| r.expect("unable to send to pool"))
    }
}

//! A connection wrapper that gives up on a peer that stops reading.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{sleep, Sleep};

/// Fails a write with `TimedOut` when the peer has not accepted any data for `limit`.
pub struct WriteTimeout<T> {
    inner: T,
    limit: Duration,
    stalled: Option<Pin<Box<Sleep>>>,
}

impl<T> WriteTimeout<T> {
    pub fn new(inner: T, limit: Duration) -> Self {
        WriteTimeout {
            inner,
            limit,
            stalled: None,
        }
    }

    fn check<R>(&mut self, cx: &mut Context<'_>, r: Poll<io::Result<R>>) -> Poll<io::Result<R>> {
        if r.is_ready() {
            self.stalled = None;
            return r;
        }
        let limit = self.limit;
        let timer = self.stalled.get_or_insert_with(|| Box::pin(sleep(limit)));
        match timer.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(io::ErrorKind::TimedOut.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for WriteTimeout<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for WriteTimeout<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let r = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.check(cx, r)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let r = Pin::new(&mut self.inner).poll_flush(cx);
        self.check(cx, r)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn a_peer_that_stops_reading_times_out() {
        let (a, mut b) = tokio::io::duplex(64);
        let mut w = WriteTimeout::new(a, Duration::from_millis(200));
        let err = w.write_all(&[0u8; 1000]).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        // a peer that does read is fine
        let (a, mut b2) = tokio::io::duplex(64);
        let mut w = WriteTimeout::new(a, Duration::from_millis(200));
        let reader = tokio::spawn(async move {
            let mut n = 0;
            let mut buf = [0u8; 100];
            while n < 1000 {
                n += b2.read(&mut buf).await.unwrap();
            }
        });
        w.write_all(&[0u8; 1000]).await.unwrap();
        reader.await.unwrap();
        let _ = b.shutdown().await;
    }
}

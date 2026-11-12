//! Hostile or broken clients: stalled requests, connection flood, oversized WebSocket frames,
//! a flooded topic next to a quiet one.

use std::time::Duration;

use collie_whistle::{serve, Config};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

async fn start(config: Config) -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(serve(l, config));
    addr
}

fn fast() -> Config {
    Config {
        header_timeout: Duration::from_millis(500),
        body_timeout: Duration::from_millis(500),
        publish_rate: 0,
        topic_publish_rate: 0,
        subscribe_rate: 0,
        ..Config::default()
    }
}

/// Reads until the server closes the connection; panics if that takes longer than `within`.
async fn read_until_closed(s: &mut TcpStream, within: Duration) -> Vec<u8> {
    let mut out = Vec::new();
    timeout(within, s.read_to_end(&mut out))
        .await
        .expect("the server kept the connection open")
        .ok();
    out
}

#[tokio::test]
async fn a_half_sent_header_is_closed() {
    let addr = start(fast()).await;
    let mut s = TcpStream::connect(&addr).await.unwrap();
    s.write_all(b"GET /uptopic?up=1 HTTP/1.1\r\nHost: x\r\nX-Slow: ")
        .await
        .unwrap();
    read_until_closed(&mut s, Duration::from_secs(5)).await;
}

#[tokio::test]
async fn a_connection_that_sends_nothing_is_closed() {
    let addr = start(fast()).await;
    let mut s = TcpStream::connect(&addr).await.unwrap();
    read_until_closed(&mut s, Duration::from_secs(5)).await;
}

#[tokio::test]
async fn a_stalled_publish_body_is_closed() {
    let addr = start(fast()).await;
    let mut s = TcpStream::connect(&addr).await.unwrap();
    s.write_all(b"POST /uptopic?up=1 HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n0123456789")
        .await
        .unwrap();
    let out = read_until_closed(&mut s, Duration::from_secs(5)).await;
    assert!(
        String::from_utf8_lossy(&out).starts_with("HTTP/1.1 408"),
        "{}",
        String::from_utf8_lossy(&out)
    );
}

#[tokio::test]
async fn connections_are_capped_and_freed() {
    let addr = start(Config {
        max_connections: 2,
        header_timeout: Duration::from_secs(30),
        ..fast()
    })
    .await;
    let a = TcpStream::connect(&addr).await.unwrap();
    let _b = TcpStream::connect(&addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut c = TcpStream::connect(&addr).await.unwrap(); // the kernel accepts, the server does not
    c.write_all(b"GET /v1/health HTTP/1.0\r\n\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 64];
    assert!(
        timeout(Duration::from_millis(500), c.read(&mut buf))
            .await
            .is_err(),
        "a third connection was served while two were open"
    );
    drop(a);
    let n = timeout(Duration::from_secs(5), c.read(&mut buf))
        .await
        .expect("not served after a slot freed")
        .unwrap();
    assert!(String::from_utf8_lossy(&buf[..n]).contains(" 200 "));
}

async fn ws_connect(addr: &str, path: &str) -> TcpStream {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        let mut b = [0u8; 512];
        let n = timeout(Duration::from_secs(5), s.read(&mut b))
            .await
            .unwrap()
            .unwrap();
        assert!(n > 0);
        buf.extend_from_slice(&b[..n]);
    }
    assert!(
        buf.starts_with(b"HTTP/1.1 101"),
        "{}",
        String::from_utf8_lossy(&buf)
    );
    s
}

fn masked_binary_frame(len: usize) -> Vec<u8> {
    let mut f = vec![0x82, 0x80 | 126];
    f.extend_from_slice(&(len as u16).to_be_bytes());
    f.extend_from_slice(&[1, 2, 3, 4]);
    f.extend((0..len).map(|i| (i as u8) ^ [1, 2, 3, 4][i % 4]));
    f
}

#[tokio::test]
async fn websocket_frames_over_4096_bytes_close_the_connection() {
    let addr = start(fast()).await;
    let mut s = ws_connect(&addr, "/upws/ws").await;
    s.write_all(&masked_binary_frame(5000)).await.unwrap();
    read_until_closed(&mut s, Duration::from_secs(5)).await;
}

#[tokio::test]
async fn websocket_frames_of_4096_bytes_are_fine() {
    let addr = start(Config {
        keepalive: Duration::from_secs(60),
        ..fast()
    })
    .await;
    let mut s = ws_connect(&addr, "/upws/ws").await;
    s.write_all(&masked_binary_frame(4096)).await.unwrap();
    // the connection stays open: reads either time out or return the open event, never EOF
    let mut buf = [0u8; 512];
    while let Ok(r) = timeout(Duration::from_secs(1), s.read(&mut buf)).await {
        assert!(
            r.unwrap() > 0,
            "the server closed a connection that sent a 4096-byte frame"
        );
    }
}

#[tokio::test]
async fn a_websocket_that_sends_no_frames_is_dropped() {
    let addr = start(Config {
        keepalive: Duration::from_millis(300),
        ..fast()
    })
    .await;
    let mut s = ws_connect(&addr, "/upws/ws").await;
    // it gets pings but never answers: dropped after two keepalive intervals
    read_until_closed(&mut s, Duration::from_secs(5)).await;
}

#[tokio::test]
async fn a_flood_on_one_topic_leaves_another_topics_subscriber_alone() {
    let addr = start(fast()).await;
    let mut quiet = TcpStream::connect(&addr).await.unwrap();
    quiet
        .write_all(b"GET /upquiet/json HTTP/1.0\r\n\r\n")
        .await
        .unwrap();
    let mut seen = String::new();
    let mut buf = [0u8; 4096];
    let publish = |topic: &'static str, n: usize| {
        let addr = addr.clone();
        async move {
            for _ in 0..n {
                let mut s = TcpStream::connect(&addr).await.unwrap();
                let req = format!("POST /{topic}?up=1 HTTP/1.0\r\nContent-Length: 1\r\n\r\nx");
                s.write_all(req.as_bytes()).await.unwrap();
                let mut o = Vec::new();
                s.read_to_end(&mut o).await.unwrap();
                assert!(String::from_utf8_lossy(&o).contains(" 200 "));
            }
        }
    };
    publish("upflood", 600).await;
    publish("upquiet", 1).await;
    while !seen.contains("\"event\":\"message\"") {
        let n = timeout(Duration::from_secs(5), quiet.read(&mut buf))
            .await
            .expect("no message")
            .unwrap();
        assert!(n > 0, "the quiet subscriber was disconnected: {seen}");
        seen.push_str(&String::from_utf8_lossy(&buf[..n]));
    }
    assert!(seen.contains("\"topic\":\"upquiet\""));
}

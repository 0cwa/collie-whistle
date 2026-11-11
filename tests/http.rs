//! End-to-end checks over a real socket. HTTP/1.0 requests keep the replies un-chunked.

use std::time::Duration;

use base64::Engine;
use collie_whistle::{serve, Config};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start() -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(serve(l, Config::default()));
    addr.to_string()
}

/// Returns (status, body).
async fn http(addr: &str, method: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "{method} {path} HTTP/1.0\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    s.write_all(head.as_bytes()).await.unwrap();
    s.write_all(body).await.unwrap();
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut out))
        .await
        .expect("reply did not end")
        .unwrap();
    let split = out.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let status = std::str::from_utf8(&out[..split])
        .unwrap()
        .split(' ')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    (status, out[split..].to_vec())
}

fn json(b: &[u8]) -> Value {
    serde_json::from_slice(b).unwrap()
}

async fn poll(addr: &str, topics: &str, query: &str) -> Vec<Value> {
    let (st, body) = http(addr, "GET", &format!("/{topics}/json?poll=1{query}"), b"").await;
    assert_eq!(st, 200);
    String::from_utf8(body)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test]
async fn binary_payload_round_trips() {
    let addr = start().await;
    let payload: Vec<u8> = (0..4096u32).map(|i| (i * 7 + 3) as u8).collect();
    let (st, body) = http(&addr, "POST", "/upbinary?up=1", &payload).await;
    assert_eq!(st, 200);
    assert_eq!(json(&body)["encoding"], "base64");
    let got = poll(&addr, "upbinary", "").await;
    assert_eq!(got.len(), 1);
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(got[0]["message"].as_str().unwrap())
        .unwrap();
    assert_eq!(bytes, payload);
}

#[tokio::test]
async fn text_is_stored_as_is_and_binary_needs_up() {
    let addr = start().await;
    let (st, body) = http(&addr, "POST", "/uptext?up=1", "caf\u{e9}".as_bytes()).await;
    assert_eq!(st, 200);
    assert_eq!(json(&body)["message"], "caf\u{e9}");
    assert!(json(&body).get("encoding").is_none());
    let (st, _) = http(&addr, "POST", "/uptext", &[0xff, 0xfe]).await;
    assert_eq!(st, 400);
}

#[tokio::test]
async fn longer_bodies_are_cut_to_4096_bytes() {
    let addr = start().await;
    let payload = vec![0xffu8; 5000];
    let (st, body) = http(&addr, "PUT", "/upbig?up=1", &payload).await;
    assert_eq!(st, 200);
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(json(&body)["message"].as_str().unwrap())
        .unwrap();
    assert_eq!(bytes, &payload[..4096]);
}

#[tokio::test]
async fn since_selects_messages_across_topics() {
    let addr = start().await;
    let mut ids = Vec::new();
    for (t, m) in [("upa", "1"), ("upb", "2"), ("upa", "3")] {
        let (_, body) = http(&addr, "POST", &format!("/{t}?up=1"), m.as_bytes()).await;
        ids.push(json(&body)["id"].as_str().unwrap().to_string());
    }
    let texts = |v: Vec<Value>| {
        v.iter()
            .map(|m| m["message"].as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(texts(poll(&addr, "upa,upb", "").await), ["1", "2", "3"]); // poll defaults to all
    assert_eq!(
        texts(poll(&addr, "upa", "&since=none").await),
        Vec::<String>::new()
    );
    assert_eq!(texts(poll(&addr, "upa,upb", "&since=latest").await), ["3"]);
    // the id belongs to upb, the subscription asks for upa only
    assert_eq!(
        texts(poll(&addr, "upa", &format!("&since={}", ids[1])).await),
        ["3"]
    );
    assert_eq!(
        texts(poll(&addr, "upa", "&since=unknownid").await),
        ["1", "3"]
    );
}

#[tokio::test]
async fn discovery_and_bad_topics() {
    let addr = start().await;
    let (st, body) = http(&addr, "GET", "/uptopic?up=1", b"").await;
    assert_eq!(st, 200);
    assert_eq!(
        json(&body),
        serde_json::json!({"unifiedpush": {"version": 1}})
    );
    let (st, _) = http(&addr, "GET", "/bad%20topic/json?poll=1", b"").await;
    assert_eq!(st, 400);
}

#[tokio::test]
async fn live_subscriber_gets_open_keepalive_and_message() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let config = Config {
        keepalive: Duration::from_secs(1),
        ..Config::default()
    };
    tokio::spawn(serve(l, config));
    let mut s = TcpStream::connect(&addr).await.unwrap();
    s.write_all(b"GET /uplive/sse?up=1 HTTP/1.0\r\n\r\n")
        .await
        .unwrap();
    let mut seen = String::new();
    let mut published = false;
    let mut buf = [0u8; 4096];
    while !(seen.contains("event: keepalive")
        && seen.contains("data: {")
        && seen.contains("\"message\":\"hi\""))
    {
        let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
            .await
            .expect("stalled")
            .unwrap();
        assert!(n > 0, "closed early: {seen}");
        seen.push_str(&String::from_utf8_lossy(&buf[..n]));
        if !published && seen.contains("event: open") {
            published = true;
            http(&addr, "POST", "/uplive?up=1", b"hi").await;
        }
    }
    assert!(seen.find("event: open").unwrap() < seen.find("\"message\":\"hi\"").unwrap());
}

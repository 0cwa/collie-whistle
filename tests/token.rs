//! The optional token: subscribers present it the ways ntfy clients do, publishers need none,
//! and it never reaches the logs.

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use collie_whistle::{serve, Config};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const TOKEN: &str = "tk_Whistle-test-0123456789";

async fn start() -> String {
    start_with(Some(TOKEN)).await
}

async fn start_with(token: Option<&str>) -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let config = Config {
        token: token.map(|t| t.parse().unwrap()),
        ..Config::default()
    };
    tokio::spawn(serve(l, config));
    addr
}

/// Sends `head` (request line and headers, without the blank line) and returns (status, body).
async fn request(addr: &str, head: &str, body: &[u8]) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let head = format!("{head}\r\nContent-Length: {}\r\n\r\n", body.len());
    s.write_all(head.as_bytes()).await.unwrap();
    s.write_all(body).await.unwrap();
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    // read the header, then the body by Content-Length, or to the end
    let (split, len) = loop {
        let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
            .await
            .expect("reply stalled")
            .unwrap();
        assert!(n > 0, "closed before the header ended");
        out.extend_from_slice(&buf[..n]);
        if let Some(p) = out.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&out[..p]).to_ascii_lowercase();
            let len = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length: "))
                .map(|v| v.trim().parse::<usize>().unwrap());
            break (p + 4, len);
        }
    };
    if !out.starts_with(b"HTTP/1.1 101") {
        while len.is_none_or(|l| out.len() < split + l) {
            let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
                .await
                .expect("reply did not end")
                .unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
    }
    let status = String::from_utf8_lossy(&out[..split])
        .split(' ')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    (status, String::from_utf8_lossy(&out[split..]).into_owned())
}

async fn get(addr: &str, path: &str, auth: Option<&str>) -> (u16, String) {
    let auth = auth.map_or(String::new(), |a| format!("\r\nAuthorization: {a}"));
    request(addr, &format!("GET {path} HTTP/1.0{auth}"), b"").await
}

fn code(body: &str) -> Value {
    serde_json::from_str::<Value>(body).unwrap()["code"].clone()
}

#[tokio::test]
async fn publishing_and_discovery_need_no_token() {
    let addr = start().await;
    let (st, _) = request(&addr, "POST /upopen?up=1 HTTP/1.0", b"hi").await;
    assert_eq!(st, 200);
    // CollieSocket and other application servers send VAPID in the Authorization header
    let vapid = "POST /upopen?up=1 HTTP/1.0\r\nAuthorization: vapid t=x.y.z, k=abc";
    assert_eq!(request(&addr, vapid, b"hi").await.0, 200);
    assert_eq!(get(&addr, "/upopen?up=1", None).await.0, 200);
    assert_eq!(get(&addr, "/v1/health", None).await.0, 200);
}

#[tokio::test]
async fn subscribing_without_the_token_is_refused_as_ntfy_does() {
    let addr = start().await;
    // no credentials: 403 forbidden, as ntfy answers an anonymous client
    for path in ["/upsub/json?poll=1", "/upsub/json", "/upsub/sse?up=1"] {
        let (st, body) = get(&addr, path, None).await;
        assert_eq!((st, code(&body)), (403, 40301.into()), "{path}");
    }
    let ws = "GET /upsub/ws HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
              Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==";
    assert_eq!(request(&addr, ws, b"").await.0, 403);
    assert_eq!(
        get(&addr, "/upsub/json?poll=1", Some("vapid t=a, k=b"))
            .await
            .0,
        403
    );
    // wrong credentials: 401 unauthorized
    let wrong = [
        "Bearer tk_Whistle-test-012345678".to_string(),
        "Bearer tk_Whistle-test-01234567890".to_string(),
        format!("Basic {}", STANDARD.encode("user:wrong")),
        format!("Basic {}", STANDARD.encode(TOKEN)), // no colon, no password
        "Basic !!!".to_string(),
    ];
    for auth in &wrong {
        let (st, body) = get(&addr, "/upsub/json?poll=1", Some(auth)).await;
        assert_eq!((st, code(&body)), (401, 40101.into()), "{auth}");
    }
    let bad_query = URL_SAFE_NO_PAD.encode("Bearer nope");
    let (st, _) = get(&addr, &format!("/upsub/json?poll=1&auth={bad_query}"), None).await;
    assert_eq!(st, 401);
    // like ntfy, the query parameter wins over a correct header
    let header = format!("Bearer {TOKEN}");
    let path = format!("/upsub/json?poll=1&auth={bad_query}");
    assert_eq!(get(&addr, &path, Some(&header)).await.0, 401);
}

#[tokio::test]
async fn subscribers_present_the_token_the_ways_ntfy_clients_do() {
    let addr = start().await;
    request(&addr, "POST /upauth?up=1 HTTP/1.0", b"hello").await;
    let bearer = format!("Bearer {TOKEN}");
    let query = format!("auth={}", URL_SAFE_NO_PAD.encode(&bearer));
    let ways = [
        ("/upauth/json?poll=1".to_string(), Some(bearer.clone())), // KUnifiedPush Bearer
        (
            "/upauth/sse?poll=1".to_string(),
            Some(format!("bearer  {TOKEN} ")),
        ),
        // ntfy Android user credentials; ntfy itself takes an empty user name with a token
        (
            "/upauth/json?poll=1".to_string(),
            Some(format!(
                "Basic {}",
                STANDARD.encode(format!("phone:{TOKEN}"))
            )),
        ),
        (
            "/upauth/json?poll=1".to_string(),
            Some(format!("Basic {}", STANDARD.encode(format!(":{TOKEN}")))),
        ),
        (format!("/upauth/json?poll=1&{query}"), None),
        (format!("/upauth/json?poll=1&{query}=="), None), // padded base64 too
        // like ntfy: `authorization` before `auth`, and the first value of a repeated key
        (
            format!(
                "/upauth/json?poll=1&auth=bad&{}",
                query.replacen("auth", "authorization", 1)
            ),
            None,
        ),
        (format!("/upauth/json?poll=1&{query}&auth=bad"), None),
    ];
    for (path, auth) in &ways {
        let (st, body) = get(&addr, path, auth.as_deref()).await;
        assert_eq!(st, 200, "{path} {auth:?}");
        assert!(
            body.contains("\"message\":\"hello\""),
            "{path} {auth:?}: {body}"
        );
    }
    let ws = format!(
        "GET /upauth/ws HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Authorization: {bearer}"
    );
    assert_eq!(request(&addr, &ws, b"").await.0, 101);
}

#[tokio::test]
async fn without_a_token_credentials_are_ignored() {
    let addr = start_with(None).await;
    let (st, _) = request(&addr, "POST /upfree?up=1&auth=a&auth=b HTTP/1.0", b"hi").await;
    assert_eq!(st, 200);
    let path = "/upfree/json?poll=1&auth=a&auth=b&authorization=c";
    let (st, body) = get(&addr, path, Some("Bearer wrong")).await;
    assert_eq!(st, 200);
    assert!(body.contains("\"message\":\"hi\""), "{body}");
}

#[test]
fn the_token_stays_out_of_logs() {
    let config = Config {
        token: Some(TOKEN.parse().unwrap()),
        ..Config::default()
    };
    assert!(!format!("{config:?}").contains(TOKEN));

    // a refused token is not echoed, unlike other settings that fail to parse
    let out = Command::new(env!("CARGO_BIN_EXE_collie-whistle"))
        .env("WHISTLE_TOKEN", "leaky secret")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("WHISTLE_TOKEN") && !stderr.contains("leaky"),
        "{stderr}"
    );

    // a running server logs nothing about subscribers with right or wrong tokens
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_collie-whistle"))
        .env("WHISTLE_LISTEN", format!("127.0.0.1:{port}"))
        .env("WHISTLE_TOKEN", TOKEN)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    let mut first = String::new();
    stderr.read_line(&mut first).unwrap();
    assert!(first.contains("listening"), "{first}");
    for auth in [format!("Bearer {TOKEN}"), format!("Bearer {TOKEN}x")] {
        let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        write!(
            s,
            "GET /uplog/json?poll=1 HTTP/1.0\r\nAuthorization: {auth}\r\n\r\n"
        )
        .unwrap();
        s.read_to_end(&mut Vec::new()).unwrap();
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let mut rest = String::new();
    stderr.read_to_string(&mut rest).unwrap();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut rest)
        .unwrap();
    assert!(!format!("{first}{rest}").contains(TOKEN));
}

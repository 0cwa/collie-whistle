//! Collie Whistle: a small UnifiedPush server that speaks the ntfy subset UnifiedPush
//! distributors use. Payloads are opaque: they are stored and relayed, never parsed.
//!
//! ```no_run
//! # async fn run() -> std::io::Result<()> {
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
//! collie_whistle::serve(listener, collie_whistle::Config::default()).await
//! # }
//! ```

// The error side of these helpers is a ready-made HTTP response; it is only built on bad requests.
#![allow(clippy::result_large_err)]

mod store;

use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use base64::Engine;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::time::{interval_at, Instant, Interval, MissedTickBehavior};

pub use store::{Message, Since, Store};

/// The largest body a UnifiedPush message may have.
pub const MAX_BODY: usize = 4096;

#[derive(Debug, Clone)]
pub struct Config {
    /// Messages kept in total; the oldest go first.
    pub max_messages: usize,
    /// How long a message stays available for `since=`.
    pub cache_duration: Duration,
    /// Gap between keepalive events and WebSocket pings on idle subscriptions.
    pub keepalive: Duration,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            max_messages: 10_000,
            cache_duration: Duration::from_secs(12 * 3600),
            keepalive: Duration::from_secs(45),
        }
    }
}

#[derive(Clone)]
struct App {
    store: Arc<Store>,
    keepalive: Duration,
}

/// Builds the router. Must be called inside a tokio runtime (it starts the cache expiry task).
pub fn router(config: Config) -> Router {
    let store = Arc::new(Store::new(
        config.max_messages,
        config.cache_duration.as_secs(),
    ));
    let weak = Arc::downgrade(&store);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            match weak.upgrade() {
                Some(s) => s.expire(),
                None => break,
            }
        }
    });
    Router::new()
        .route(
            "/v1/health",
            get(|| async { Json(json!({"healthy": true})) }),
        )
        .route("/{topic}", any(topic_root))
        .route("/{topics}/json", get(sub_json))
        .route("/{topics}/sse", get(sub_sse))
        .route("/{topics}/ws", get(sub_ws))
        .with_state(App {
            store,
            keepalive: config.keepalive,
        })
}

/// Serves on `listener` until the future is dropped or the listener fails.
pub async fn serve(listener: tokio::net::TcpListener, config: Config) -> std::io::Result<()> {
    axum::serve(listener, router(config)).await
}

// ---- errors

fn error(status: StatusCode, code: u32, msg: &str) -> Response {
    (
        status,
        Json(json!({"code": code, "http": status.as_u16(), "error": msg})),
    )
        .into_response()
}

fn valid_topic(t: &str) -> bool {
    !t.is_empty()
        && t.len() <= 64
        && t.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn parse_topics(s: &str) -> Result<HashSet<String>, Response> {
    let set: HashSet<String> = s.split(',').map(str::to_string).collect();
    if set.iter().all(|t| valid_topic(t)) && set.len() <= 50 {
        Ok(set)
    } else {
        Err(error(StatusCode::BAD_REQUEST, 40010, "invalid topic name"))
    }
}

// ---- publish and discovery

#[derive(Deserialize)]
struct Params {
    up: Option<String>,
    since: Option<String>,
    poll: Option<String>,
}

fn flag(v: &Option<String>) -> bool {
    matches!(v.as_deref(), Some("1" | "true" | "yes"))
}

async fn topic_root(
    State(app): State<App>,
    method: Method,
    Path(topic): Path<String>,
    Query(q): Query<Params>,
    body: Body,
) -> Response {
    if !valid_topic(&topic) {
        return error(StatusCode::BAD_REQUEST, 40010, "invalid topic name");
    }
    match method {
        // UnifiedPush discovery, called by application servers
        Method::GET if flag(&q.up) => Json(json!({"unifiedpush": {"version": 1}})).into_response(),
        Method::POST | Method::PUT => publish(&app, &topic, flag(&q.up), body).await,
        _ => error(StatusCode::NOT_FOUND, 40401, "page not found"),
    }
}

async fn publish(app: &App, topic: &str, up: bool, body: Body) -> Response {
    // Like stock ntfy, keep the first 4096 bytes of a longer body and drop the rest.
    let mut bytes = Vec::new();
    let mut data = body.into_data_stream();
    while bytes.len() < MAX_BODY {
        match data.next().await {
            Some(Ok(chunk)) => bytes.extend_from_slice(&chunk),
            Some(Err(_)) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    40001,
                    "cannot read the request body",
                )
            }
            None => break,
        }
    }
    bytes.truncate(MAX_BODY);
    let (message, encoding) = match String::from_utf8(bytes) {
        Ok(s) if s.is_empty() => ("triggered".to_string(), None),
        Ok(s) => (s, None),
        // like stock ntfy, binary is only accepted from UnifiedPush application servers
        Err(_) if !up => return error(StatusCode::BAD_REQUEST, 40019, "binary body needs up=1"),
        Err(e) => (
            base64::engine::general_purpose::STANDARD.encode(e.into_bytes()),
            Some("base64"),
        ),
    };
    let m = app.store.publish(topic, message, encoding);
    Json(&*m).into_response()
}

// ---- subscribe

enum Event {
    Open,
    Message(Arc<Message>),
    Keepalive,
}

/// One subscriber: an open event, the cached backlog, then live messages and keepalives.
struct Sub {
    topics: HashSet<String>,
    label: String, // the topic list as the client wrote it, echoed in open and keepalive events
    opened: bool,
    poll: bool,
    backlog: std::vec::IntoIter<Arc<Message>>,
    rx: broadcast::Receiver<Arc<Message>>,
    ticker: Interval,
}

impl Sub {
    fn new(app: &App, label: String, q: &Params) -> Result<Sub, Response> {
        let topics = parse_topics(&label)?;
        let poll = flag(&q.poll);
        let (backlog, rx) = app
            .store
            .subscribe(&topics, &Since::parse(q.since.as_deref(), poll));
        let mut ticker = interval_at(Instant::now() + app.keepalive, app.keepalive);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Ok(Sub {
            topics,
            label,
            opened: poll,
            poll,
            backlog: backlog.into_iter(),
            rx,
            ticker,
        })
    }

    /// Next event, or None when the subscription is over: a poll that has sent its backlog, or
    /// a subscriber that fell too far behind (it reconnects with `since=`). Cancel safe.
    async fn next(&mut self) -> Option<Event> {
        if !self.opened {
            self.opened = true;
            return Some(Event::Open);
        }
        if let Some(m) = self.backlog.next() {
            return Some(Event::Message(m));
        }
        if self.poll {
            return None;
        }
        loop {
            tokio::select! {
                r = self.rx.recv() => match r {
                    Ok(m) if self.topics.contains(&m.topic) => return Some(Event::Message(m)),
                    Ok(_) => {}
                    Err(RecvError::Lagged(_) | RecvError::Closed) => return None,
                },
                _ = self.ticker.tick() => return Some(Event::Keepalive),
            }
        }
    }

    fn json(&self, ev: &Event) -> String {
        let control = |name: &str| {
            json!({"id": store::new_id(), "time": store::now(), "event": name, "topic": self.label})
                .to_string()
        };
        match ev {
            Event::Open => control("open"),
            Event::Keepalive => control("keepalive"),
            Event::Message(m) => serde_json::to_string(&**m).unwrap_or_default(),
        }
    }
}

fn stream_response(content_type: &'static str, body: Body) -> Response {
    let mut r = Response::new(body);
    r.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    r
}

fn event_stream(sub: Sub, sse: bool) -> Body {
    let s = futures_util::stream::unfold(sub, move |mut sub| async move {
        let ev = sub.next().await?;
        let data = sub.json(&ev);
        let chunk = if sse {
            match ev {
                Event::Open => format!("event: open\ndata: {data}\n\n"),
                Event::Keepalive => format!("event: keepalive\ndata: {data}\n\n"),
                Event::Message(_) => format!("data: {data}\n\n"),
            }
        } else {
            format!("{data}\n")
        };
        Some((Ok::<_, Infallible>(Bytes::from(chunk)), sub))
    });
    Body::from_stream(s)
}

async fn sub_json(
    State(app): State<App>,
    Path(topics): Path<String>,
    Query(q): Query<Params>,
) -> Response {
    match Sub::new(&app, topics, &q) {
        Ok(sub) => stream_response(
            "application/x-ndjson; charset=utf-8",
            event_stream(sub, false),
        ),
        Err(e) => e,
    }
}

async fn sub_sse(
    State(app): State<App>,
    Path(topics): Path<String>,
    Query(q): Query<Params>,
) -> Response {
    match Sub::new(&app, topics, &q) {
        Ok(sub) => stream_response("text/event-stream; charset=utf-8", event_stream(sub, true)),
        Err(e) => e,
    }
}

async fn sub_ws(
    State(app): State<App>,
    Path(topics): Path<String>,
    Query(q): Query<Params>,
    upgrade: WebSocketUpgrade,
) -> Response {
    match Sub::new(&app, topics, &q) {
        Ok(sub) => upgrade.on_upgrade(move |sock| run_ws(sock, sub)),
        Err(e) => e,
    }
}

/// Stock ntfy sends a WebSocket ping every keepalive interval instead of a JSON keepalive
/// event, and ntfy Android pings the server in turn, so pings are answered with pongs.
async fn run_ws(mut sock: WebSocket, mut sub: Sub) {
    loop {
        tokio::select! {
            ev = sub.next() => {
                let out = match ev {
                    None => break,
                    Some(Event::Keepalive) => WsMessage::Ping(Bytes::new()),
                    Some(ev) => WsMessage::Text(sub.json(&ev).into()),
                };
                if sock.send(out).await.is_err() {
                    break;
                }
            }
            frame = sock.recv() => match frame {
                Some(Ok(WsMessage::Ping(p))) => {
                    if sock.send(WsMessage::Pong(p)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
        }
    }
}

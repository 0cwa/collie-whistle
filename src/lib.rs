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

mod io;
mod store;

use std::collections::{HashSet, VecDeque};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use base64::Engine;
use futures_util::StreamExt;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{interval_at, sleep_until, timeout, Instant, Interval, MissedTickBehavior};

use io::WriteTimeout;
use store::{Limits, Live, RateLimited, SubscribeError, Subscription};
pub use store::{Message, Since, Store};

/// The largest body a UnifiedPush message may have.
pub const MAX_BODY: usize = 4096;

/// How long a WebSocket write may take before the client is dropped.
const WS_SEND_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct Config {
    /// Messages kept in total; the oldest go first.
    pub max_messages: usize,
    /// Messages kept per topic; a topic that goes over loses its own oldest message.
    pub max_per_topic: usize,
    /// Topics that may have live subscribers at once; more are refused with 429.
    pub max_live_topics: usize,
    /// Live subscribers of one topic; more are refused with 429.
    pub max_subscribers_per_topic: usize,
    /// How long a message stays available for `since=`.
    pub cache_duration: Duration,
    /// Publishes accepted per second over all topics, 0 for no limit. A high ceiling.
    pub publish_rate: u32,
    /// Publishes accepted per second to one topic, 0 for no limit.
    pub topic_publish_rate: u32,
    /// Subscribe requests accepted per second, 0 for no limit.
    pub subscribe_rate: u32,
    /// Gap between keepalive events and WebSocket pings on idle subscriptions.
    pub keepalive: Duration,
    /// Open connections at once; further ones wait to be accepted. Three quarters of them may
    /// be live subscriptions, so short requests (publish, health) always have room.
    pub max_connections: usize,
    /// Time a client gets to send a complete request header.
    pub header_timeout: Duration,
    /// Time a publisher gets to send its body.
    pub body_timeout: Duration,
    /// Time a client gets to accept data the server is sending before the connection is closed.
    pub write_timeout: Duration,
    /// When set, subscribing (`/json`, `/sse`, `/ws`, also with `poll=1`) needs this token.
    /// Publishing stays open: UnifiedPush application servers only have the endpoint URL.
    pub token: Option<Token>,
}

/// A token subscribers must present, the way ntfy clients send one: `Authorization: Bearer`,
/// Basic auth with the token as password, or the `auth` query parameter. Its `Debug` output
/// hides the value, so a logged [`Config`] does not leak it.
#[derive(Clone)]
pub struct Token(String);

impl std::str::FromStr for Token {
    type Err = &'static str;

    /// Refuses an empty token, and characters a client could not send in a header unchanged.
    fn from_str(s: &str) -> Result<Token, Self::Err> {
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_graphic()) {
            return Err("a token must be non-empty printable ASCII without spaces");
        }
        Ok(Token(s.to_string()))
    }
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Token(..)")
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            max_messages: 10_000,
            max_per_topic: 100,
            max_live_topics: 4096,
            max_subscribers_per_topic: 16,
            cache_duration: Duration::from_secs(3600),
            publish_rate: 100,
            topic_publish_rate: 5,
            subscribe_rate: 100,
            keepalive: Duration::from_secs(45),
            max_connections: 512,
            header_timeout: Duration::from_secs(10),
            body_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(60),
            token: None,
        }
    }
}

#[derive(Clone)]
struct App {
    store: Arc<Store>,
    keepalive: Duration,
    body_timeout: Duration,
    token: Option<Arc<Token>>,
    /// Permits for live subscriptions: a share of the connection cap, so that publishers and
    /// health checks always find room.
    subs: Arc<Semaphore>,
}

fn make_app(config: &Config) -> App {
    let store = Arc::new(Store::new(Limits {
        max_messages: config.max_messages,
        max_per_topic: config.max_per_topic,
        max_age_secs: config.cache_duration.as_secs(),
        publish_rate: config.publish_rate,
        topic_publish_rate: config.topic_publish_rate,
        subscribe_rate: config.subscribe_rate,
        max_live_topics: config.max_live_topics,
        max_subscribers_per_topic: config.max_subscribers_per_topic,
    }));
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
    let connections = config.max_connections.max(1);
    let subscription_connections = connections - connections.div_ceil(4);
    App {
        store,
        keepalive: config.keepalive,
        body_timeout: config.body_timeout,
        token: config.token.clone().map(Arc::new),
        subs: Arc::new(Semaphore::new(subscription_connections)),
    }
}

fn routes(app: App) -> Router {
    Router::new()
        .route(
            "/v1/health",
            get(|| async { Json(json!({"healthy": true})) }),
        )
        .route("/{topic}", any(topic_root))
        .route("/{topics}/json", get(sub_json))
        .route("/{topics}/sse", get(sub_sse))
        .route("/{topics}/ws", get(sub_ws))
        .with_state(app)
}

/// Builds the router. Must be called inside a tokio runtime (it starts the cache expiry task).
/// Connection limits and timeouts belong to [`serve`]; the router has none of its own.
pub fn router(config: Config) -> Router {
    routes(make_app(&config))
}

/// Serves on `listener` until the future is dropped. Connections are capped at
/// `max_connections`; one that does not send its request header within `header_timeout`, or
/// does not accept data for `write_timeout`, is closed.
pub async fn serve(listener: tokio::net::TcpListener, config: Config) -> std::io::Result<()> {
    let permits = Arc::new(Semaphore::new(config.max_connections.max(1)));
    let (header_timeout, write_timeout) = (config.header_timeout, config.write_timeout);
    let app = router(config);
    let mut last_error: Option<std::io::ErrorKind> = None;
    loop {
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        let stream = match listener.accept().await {
            Ok((stream, _)) => {
                last_error = None;
                stream
            }
            Err(e) => {
                // out of file descriptors, or a connection reset before accept: log each
                // distinct error once, then try again shortly
                if last_error != Some(e.kind()) {
                    eprintln!("collie-whistle: accept failed: {e}");
                    last_error = Some(e.kind());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let service = TowerToHyperService::new(app.clone());
        tokio::spawn(async move {
            let _ = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(header_timeout)
                .serve_connection(
                    TokioIo::new(WriteTimeout::new(stream, write_timeout)),
                    service,
                )
                .with_upgrades()
                .await;
            drop(permit);
        });
    }
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

// ---- token

/// Checks the token like stock ntfy reads credentials: the `authorization` or `auth` query
/// parameter (the header value, base64url encoded) wins over the `Authorization` header. No
/// credentials get 403, as ntfy answers an anonymous client; wrong ones get 401.
/// The query is read here rather than through [`Params`], so that a repeated `auth` key is no
/// error and nothing changes without a token.
fn authorize(app: &App, headers: &HeaderMap, query: Option<&str>) -> Result<(), Response> {
    let Some(token) = &app.token else {
        return Ok(());
    };
    let forbidden = || error(StatusCode::FORBIDDEN, 40301, "forbidden");
    let unauthorized = || error(StatusCode::UNAUTHORIZED, 40101, "unauthorized");
    // like ntfy: the first value of `authorization`, else of `auth`, if not blank
    let pairs: Vec<_> = form_urlencoded::parse(query.unwrap_or("").as_bytes()).collect();
    let query = ["authorization", "auth"].into_iter().find_map(|name| {
        let (_, v) = pairs.iter().find(|(k, _)| k == name)?;
        Some(v.trim()).filter(|v| !v.is_empty())
    });
    let value = match query {
        Some(v) => base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(v.trim_end_matches('='))
            .map_err(|_| unauthorized())?,
        None => match headers.get(header::AUTHORIZATION) {
            Some(h) => h.as_bytes().to_vec(),
            None => return Err(forbidden()),
        },
    };
    let value = value.trim_ascii();
    let scheme =
        |name: &[u8]| value.len() > name.len() && value[..name.len()].eq_ignore_ascii_case(name);
    let presented = if scheme(b"bearer ") {
        value[7..].trim_ascii().to_vec()
    } else if scheme(b"basic ") {
        // ntfy takes the password as a token when the user name is empty. ntfy Android
        // requires a user name, so any user name is accepted here.
        let pair = base64::engine::general_purpose::STANDARD
            .decode(value[6..].trim_ascii())
            .map_err(|_| unauthorized())?;
        match pair.iter().position(|&b| b == b':') {
            Some(i) => pair[i + 1..].to_vec(),
            None => return Err(unauthorized()),
        }
    } else {
        // no credentials, or a scheme ntfy ignores, such as a VAPID header
        return Err(forbidden());
    };
    if same_secret(&presented, token.0.as_bytes()) {
        Ok(())
    } else {
        Err(unauthorized())
    }
}

/// Compares every byte, so the time taken shows at most the lengths, not where the inputs differ.
fn same_secret(a: &[u8], b: &[u8]) -> bool {
    let mut diff = u8::from(a.len() != b.len());
    for i in 0..a.len().max(b.len()) {
        diff |= a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0);
    }
    std::hint::black_box(diff) == 0
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
    let read = async {
        let mut bytes = Vec::new();
        let mut data = body.into_data_stream();
        while bytes.len() < MAX_BODY {
            match data.next().await {
                Some(Ok(chunk)) => bytes.extend_from_slice(&chunk),
                Some(Err(_)) => return Err(StatusCode::BAD_REQUEST),
                None => break,
            }
        }
        Ok(bytes)
    };
    let mut bytes = match timeout(app.body_timeout, read).await {
        Ok(Ok(b)) => b,
        Ok(Err(_)) => {
            return error(
                StatusCode::BAD_REQUEST,
                40001,
                "cannot read the request body",
            )
        }
        Err(_) => return error(StatusCode::REQUEST_TIMEOUT, 40801, "request body too slow"),
    };
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
    match app.store.publish(topic, message, encoding) {
        Ok(m) => Json(&*m).into_response(),
        Err(RateLimited) => error(StatusCode::TOO_MANY_REQUESTS, 42901, "too many messages"),
    }
}

// ---- subscribe

enum Event {
    Open,
    Message(Arc<Message>),
    Keepalive,
}

/// One subscriber: an open event, the cached backlog, then live messages and keepalives.
struct Sub {
    label: String, // the topic list as the client wrote it, echoed in open and keepalive events
    opened: bool,
    backlog: std::vec::IntoIter<Arc<Message>>,
    /// Live messages already taken from the channels, in publish order.
    queue: VecDeque<Arc<Message>>,
    /// None for a poll, which ends after the backlog.
    live: Option<Live>,
    ticker: Interval,
    _permit: Option<OwnedSemaphorePermit>,
}

fn refused(msg: &str, code: u32) -> Response {
    error(StatusCode::TOO_MANY_REQUESTS, code, msg)
}

impl Sub {
    fn new(app: &App, label: String, q: &Params) -> Result<Sub, Response> {
        let topics = parse_topics(&label)?;
        let poll = flag(&q.poll);
        let permit = if poll {
            None
        } else {
            let p = app.subs.clone().try_acquire_owned();
            Some(p.map_err(|_| refused("too many subscriptions", 42903))?)
        };
        let since = Since::parse(q.since.as_deref(), poll);
        let Subscription { backlog, live } =
            app.store
                .subscribe(&topics, &since, !poll)
                .map_err(|e| match e {
                    SubscribeError::RateLimited => refused("too many requests", 42902),
                    SubscribeError::TooManyTopics => refused("too many subscribed topics", 42904),
                    SubscribeError::TooManySubscribers => {
                        refused("too many subscribers for a topic", 42905)
                    }
                })?;
        let mut ticker = interval_at(Instant::now() + app.keepalive, app.keepalive);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Ok(Sub {
            label,
            opened: poll,
            backlog: backlog.into_iter(),
            queue: VecDeque::new(),
            live,
            ticker,
            _permit: permit,
        })
    }

    /// Next event, or None when the subscription is over: a poll that has sent its backlog, or
    /// a subscriber that fell too far behind (it reconnects with `since=`). Cancel safe.
    async fn next(&mut self) -> Option<Event> {
        if !self.opened {
            self.opened = true;
            return Some(Event::Open);
        }
        if let Some(m) = self.backlog.next().or_else(|| self.queue.pop_front()) {
            return Some(Event::Message(m));
        }
        let live = self.live.as_mut()?;
        tokio::select! {
            r = recv_any(&mut live.rxs) => {
                // Publish and drain take the store mutex. The per-topic channels can therefore
                // be drained as one consistent snapshot before advancing the shared cursor.
                let batch = live.drain_ready(r.ok()?)?;
                self.queue.extend(batch);
                self.queue.pop_front().map(Event::Message)
            },
            _ = self.ticker.tick() => Some(Event::Keepalive),
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

/// The next message from any of the topic channels. Cancel safe.
async fn recv_any(
    rxs: &mut [broadcast::Receiver<Arc<Message>>],
) -> Result<Arc<Message>, RecvError> {
    if let [rx] = rxs {
        return rx.recv().await;
    }
    futures_util::future::select_all(rxs.iter_mut().map(|r| Box::pin(r.recv())))
        .await
        .0
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
    RawQuery(raw): RawQuery,
    headers: HeaderMap,
) -> Response {
    match authorize(&app, &headers, raw.as_deref()).and_then(|()| Sub::new(&app, topics, &q)) {
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
    RawQuery(raw): RawQuery,
    headers: HeaderMap,
) -> Response {
    match authorize(&app, &headers, raw.as_deref()).and_then(|()| Sub::new(&app, topics, &q)) {
        Ok(sub) => stream_response("text/event-stream; charset=utf-8", event_stream(sub, true)),
        Err(e) => e,
    }
}

async fn sub_ws(
    State(app): State<App>,
    Path(topics): Path<String>,
    Query(q): Query<Params>,
    RawQuery(raw): RawQuery,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    match authorize(&app, &headers, raw.as_deref()).and_then(|()| Sub::new(&app, topics, &q)) {
        Ok(sub) => upgrade
            .max_message_size(MAX_BODY)
            .max_frame_size(MAX_BODY)
            .on_upgrade(move |sock| run_ws(sock, sub, app.keepalive)),
        Err(e) => e,
    }
}

/// Stock ntfy sends a WebSocket ping every keepalive interval instead of a JSON keepalive
/// event, and ntfy Android pings the server in turn, so pings are answered with pongs. A client
/// that sends no frame (pongs count) for two keepalive intervals, or cannot take a message for
/// 30 s, is dropped.
async fn run_ws(mut sock: WebSocket, mut sub: Sub, keepalive: Duration) {
    let mut last_frame = Instant::now();
    loop {
        tokio::select! {
            ev = sub.next() => {
                let out = match ev {
                    None => break,
                    Some(Event::Keepalive) => WsMessage::Ping(Bytes::new()),
                    Some(ev) => WsMessage::Text(sub.json(&ev).into()),
                };
                if !matches!(timeout(WS_SEND_TIMEOUT, sock.send(out)).await, Ok(Ok(()))) {
                    break;
                }
            }
            frame = sock.recv() => {
                last_frame = Instant::now();
                match frame {
                    Some(Ok(WsMessage::Ping(p))) => {
                        if !matches!(timeout(WS_SEND_TIMEOUT, sock.send(WsMessage::Pong(p))).await, Ok(Ok(()))) {
                            break;
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                }
            }
            _ = sleep_until(last_frame + 2 * keepalive) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(config: Config) -> App {
        make_app(&Config {
            publish_rate: 0,
            topic_publish_rate: 0,
            subscribe_rate: 0,
            ..config
        })
    }

    fn params(poll: bool) -> Params {
        Params {
            up: None,
            since: None,
            poll: poll.then(|| "1".to_string()),
        }
    }

    async fn text(sub: &mut Sub) -> String {
        match sub.next().await {
            Some(Event::Message(m)) => m.message.clone(),
            _ => panic!("expected a message"),
        }
    }

    #[tokio::test]
    async fn messages_of_several_topics_arrive_in_publish_order() {
        let app = app(Config::default());
        let mut sub = Sub::new(&app, "a,b".into(), &params(false)).ok().unwrap();
        assert!(matches!(sub.next().await, Some(Event::Open)));
        // b first, then a: the channel of a is polled first, but b came earlier
        app.store.publish("b", "1".into(), None).unwrap();
        app.store.publish("a", "2".into(), None).unwrap();
        assert_eq!(
            (text(&mut sub).await, text(&mut sub).await),
            ("1".into(), "2".into())
        );
        for i in 0..50 {
            let topic = if i % 3 == 0 { "a" } else { "b" };
            app.store.publish(topic, i.to_string(), None).unwrap();
        }
        for i in 0..50 {
            assert_eq!(text(&mut sub).await, i.to_string());
        }
    }

    #[tokio::test]
    async fn a_cross_topic_cursor_does_not_skip_a_message_published_during_live_delivery() {
        let app = app(Config::default());
        let mut sub = Sub::new(&app, "a,b".into(), &params(false)).ok().unwrap();
        assert!(matches!(sub.next().await, Some(Event::Open)));

        let first = app.store.publish("a", "1".into(), None).unwrap();
        assert_eq!(text(&mut sub).await, "1");

        // Topic A's earlier message and topic B's later message are both pending when the
        // subscriber wakes. They must be delivered in sequence order before its shared cursor
        // can advance to B's ID.
        let second = app.store.publish("a", "2".into(), None).unwrap();
        let third = app.store.publish("b", "3".into(), None).unwrap();
        assert_eq!(text(&mut sub).await, "2");
        assert_eq!(text(&mut sub).await, "3");

        let resumed = app
            .store
            .subscribe(
                &parse_topics("a,b").unwrap(),
                &Since::Id(first.id.clone()),
                false,
            )
            .unwrap();
        assert_eq!(
            resumed
                .backlog
                .iter()
                .map(|m| m.id.as_str())
                .collect::<Vec<_>>(),
            [second.id.as_str(), third.id.as_str()]
        );
        let resumed = app
            .store
            .subscribe(
                &parse_topics("a,b").unwrap(),
                &Since::Id(third.id.clone()),
                false,
            )
            .unwrap();
        assert!(resumed.backlog.is_empty());
    }

    #[tokio::test]
    async fn a_flood_of_polls_and_subscribes_keeps_memory_bounded() {
        let app = app(Config {
            max_live_topics: 100,
            ..Config::default()
        });
        let list = |round: usize| {
            (0..50)
                .map(|i| format!("t{round}x{i}"))
                .collect::<Vec<_>>()
                .join(",")
        };
        for round in 0..1600 {
            assert!(Sub::new(&app, list(round), &params(true)).is_ok());
        }
        assert_eq!(app.store.topic_count(), 0);
        let mut held = Vec::new();
        for round in 0..1600 {
            if let Ok(sub) = Sub::new(&app, list(round), &params(false)) {
                held.push(sub);
            }
            assert!(app.store.topic_count() <= 100);
        }
        assert_eq!(held.len(), 2);
        drop(held);
        assert_eq!(app.store.topic_count(), 0);
    }

    #[tokio::test]
    async fn live_subscriptions_leave_room_for_short_requests() {
        let app = app(Config {
            max_connections: 4,
            ..Config::default()
        }); // 3 subscriptions
        let held: Vec<_> = (0..3)
            .map(|i| {
                Sub::new(&app, format!("t{i}"), &params(false))
                    .ok()
                    .unwrap()
            })
            .collect();
        let fourth = Sub::new(&app, "t9".into(), &params(false));
        assert_eq!(
            fourth.err().map(|r| r.status()),
            Some(StatusCode::TOO_MANY_REQUESTS)
        );
        assert!(Sub::new(&app, "t9".into(), &params(true)).is_ok()); // polls are short requests
        drop(held);
        assert!(Sub::new(&app, "t9".into(), &params(false)).is_ok());
    }

    #[tokio::test]
    async fn a_single_connection_is_reserved_for_short_requests() {
        let app = app(Config {
            max_connections: 1,
            ..Config::default()
        });
        assert_eq!(
            Sub::new(&app, "t".into(), &params(false))
                .err()
                .map(|r| r.status()),
            Some(StatusCode::TOO_MANY_REQUESTS)
        );
        assert!(Sub::new(&app, "t".into(), &params(true)).is_ok());
    }
}

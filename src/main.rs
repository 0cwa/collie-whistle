use std::time::Duration;

use collie_whistle::Config;

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(v) => v.parse().unwrap_or_else(|_| {
            eprintln!("collie-whistle: cannot parse {name}={v:?}");
            std::process::exit(2)
        }),
        Err(_) => default,
    }
}

/// Reads `WHISTLE_TOKEN`. Unlike [`env`], an error never prints the value.
fn token() -> Option<collie_whistle::Token> {
    let v = std::env::var_os("WHISTLE_TOKEN")?;
    match v.to_str().map(str::parse) {
        Some(Ok(t)) => Some(t),
        Some(Err(why)) => {
            eprintln!("collie-whistle: cannot use WHISTLE_TOKEN: {why}");
            std::process::exit(2)
        }
        None => {
            eprintln!("collie-whistle: cannot use WHISTLE_TOKEN: it is not valid UTF-8");
            std::process::exit(2)
        }
    }
}

#[tokio::main]
async fn main() {
    let d = Config::default();
    let listen: String = env("WHISTLE_LISTEN", "127.0.0.1:8080".to_string());
    let config = Config {
        max_messages: env("WHISTLE_MAX_MESSAGES", d.max_messages),
        max_per_topic: env("WHISTLE_MAX_PER_TOPIC", d.max_per_topic),
        max_live_topics: env("WHISTLE_MAX_LIVE_TOPICS", d.max_live_topics),
        max_subscribers_per_topic: env(
            "WHISTLE_MAX_SUBSCRIBERS_PER_TOPIC",
            d.max_subscribers_per_topic,
        ),
        publish_rate: env("WHISTLE_PUBLISH_RATE", d.publish_rate),
        topic_publish_rate: env("WHISTLE_TOPIC_PUBLISH_RATE", d.topic_publish_rate),
        subscribe_rate: env("WHISTLE_SUBSCRIBE_RATE", d.subscribe_rate),
        max_connections: env("WHISTLE_MAX_CONNECTIONS", d.max_connections),
        cache_duration: Duration::from_secs(env(
            "WHISTLE_CACHE_SECONDS",
            d.cache_duration.as_secs(),
        )),
        keepalive: Duration::from_secs(
            env("WHISTLE_KEEPALIVE_SECONDS", d.keepalive.as_secs()).max(1),
        ),
        token: token(),
        ..d
    };
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .unwrap_or_else(|e| {
            eprintln!("collie-whistle: cannot listen on {listen}: {e}");
            std::process::exit(1)
        });
    eprintln!("collie-whistle: listening on {listen}");
    tokio::select! {
        r = collie_whistle::serve(listener, config) => {
            if let Err(e) = r {
                eprintln!("collie-whistle: {e}");
                std::process::exit(1);
            }
        }
        _ = shutdown_signal() => {}
    }
}

/// Resolves on Ctrl-C or, on Unix, SIGTERM, so the process can exit cleanly under a supervisor.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

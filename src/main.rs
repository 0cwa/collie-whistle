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

#[tokio::main]
async fn main() {
    let d = Config::default();
    let listen: String = env("WHISTLE_LISTEN", "127.0.0.1:8080".to_string());
    let config = Config {
        max_messages: env("WHISTLE_MAX_MESSAGES", d.max_messages),
        max_per_topic: env("WHISTLE_MAX_PER_TOPIC", d.max_per_topic),
        publish_rate: env("WHISTLE_PUBLISH_RATE", d.publish_rate),
        max_connections: env("WHISTLE_MAX_CONNECTIONS", d.max_connections),
        cache_duration: Duration::from_secs(env(
            "WHISTLE_CACHE_SECONDS",
            d.cache_duration.as_secs(),
        )),
        keepalive: Duration::from_secs(
            env("WHISTLE_KEEPALIVE_SECONDS", d.keepalive.as_secs()).max(1),
        ),
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
        _ = tokio::signal::ctrl_c() => {}
    }
}

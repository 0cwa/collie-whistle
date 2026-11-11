//! The message cache: bounded in count, expiring by age, shared by all subscribers.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::distr::{Alphanumeric, SampleString};
use serde::Serialize;
use tokio::sync::broadcast;

/// A cached message, in the shape clients read from `/json`, `/sse` and `/ws`.
#[derive(Debug, Serialize)]
pub struct Message {
    pub id: String,
    pub time: u64,
    pub expires: u64,
    pub event: &'static str,
    pub topic: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<&'static str>,
    #[serde(skip)]
    seq: u64,
}

/// Which messages a new subscription starts with (`since=`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Since {
    None,
    All,
    Latest,
    Id(String),
}

impl Since {
    pub fn parse(s: Option<&str>, poll: bool) -> Since {
        match s {
            None | Some("") => {
                if poll {
                    Since::All
                } else {
                    Since::None
                }
            }
            Some("none") => Since::None,
            Some("all") => Since::All,
            Some("latest") => Since::Latest,
            Some(id) => Since::Id(id.to_string()),
        }
    }
}

struct Inner {
    msgs: VecDeque<Arc<Message>>,
    next_seq: u64,
}

pub struct Store {
    inner: Mutex<Inner>,
    tx: broadcast::Sender<Arc<Message>>,
    max_messages: usize,
    max_age_secs: u64,
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn new_id() -> String {
    Alphanumeric.sample_string(&mut rand::rng(), 12)
}

impl Store {
    pub fn new(max_messages: usize, max_age_secs: u64) -> Store {
        // A subscriber that falls this far behind is disconnected and recovers with `since=`.
        let (tx, _) = broadcast::channel(1024);
        Store {
            inner: Mutex::new(Inner {
                msgs: VecDeque::new(),
                next_seq: 1,
            }),
            tx,
            max_messages: max_messages.max(1),
            max_age_secs,
        }
    }

    fn purge(&self, inner: &mut Inner, now: u64) {
        while let Some(front) = inner.msgs.front() {
            if front.expires <= now || inner.msgs.len() > self.max_messages {
                inner.msgs.pop_front();
            } else {
                break;
            }
        }
    }

    /// Drops expired messages. Publishing and subscribing also do this; this is for idle servers.
    pub fn expire(&self) {
        let mut g = self.inner.lock().unwrap();
        self.purge(&mut g, now());
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().msgs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Stores a message and hands it to live subscribers. The send happens under the lock so
    /// that `subscribe` sees every message exactly once, either in its backlog or live.
    pub fn publish(
        &self,
        topic: &str,
        message: String,
        encoding: Option<&'static str>,
    ) -> Arc<Message> {
        let t = now();
        let mut g = self.inner.lock().unwrap();
        let seq = g.next_seq;
        g.next_seq += 1;
        let m = Arc::new(Message {
            id: new_id(),
            time: t,
            expires: t + self.max_age_secs,
            event: "message",
            topic: topic.to_string(),
            message,
            encoding,
            seq,
        });
        g.msgs.push_back(m.clone());
        self.purge(&mut g, t);
        let _ = self.tx.send(m.clone());
        m
    }

    /// Messages for `topics` selected by `since`, plus a receiver for everything published after.
    /// A `since` id may belong to any topic: it marks a point in time. An unknown id (never
    /// seen, or expired) acts like `all`.
    pub fn subscribe(
        &self,
        topics: &HashSet<String>,
        since: &Since,
    ) -> (Vec<Arc<Message>>, broadcast::Receiver<Arc<Message>>) {
        let mut g = self.inner.lock().unwrap();
        self.purge(&mut g, now());
        let rx = self.tx.subscribe();
        let mine = |m: &&Arc<Message>| topics.contains(&m.topic);
        let backlog = match since {
            Since::None => Vec::new(),
            Since::All => g.msgs.iter().filter(mine).cloned().collect(),
            Since::Latest => g
                .msgs
                .iter()
                .rev()
                .find(|m| mine(m))
                .cloned()
                .into_iter()
                .collect(),
            Since::Id(id) => match g.msgs.iter().find(|m| &m.id == id).map(|m| m.seq) {
                Some(seq) => g
                    .msgs
                    .iter()
                    .filter(|m| m.seq > seq)
                    .filter(mine)
                    .cloned()
                    .collect(),
                None => g.msgs.iter().filter(mine).cloned().collect(),
            },
        };
        (backlog, rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topics(t: &[&str]) -> HashSet<String> {
        t.iter().map(|s| s.to_string()).collect()
    }

    fn ids(v: &[Arc<Message>]) -> Vec<&str> {
        v.iter().map(|m| m.message.as_str()).collect()
    }

    #[test]
    fn since_variants() {
        let s = Store::new(100, 3600);
        let a = s.publish("t1", "a".into(), None);
        let b = s.publish("t2", "b".into(), None);
        s.publish("t1", "c".into(), None);
        let both = topics(&["t1", "t2"]);
        assert_eq!(ids(&s.subscribe(&both, &Since::None).0), Vec::<&str>::new());
        assert_eq!(ids(&s.subscribe(&both, &Since::All).0), ["a", "b", "c"]);
        assert_eq!(ids(&s.subscribe(&both, &Since::Latest).0), ["c"]);
        assert_eq!(
            ids(&s.subscribe(&both, &Since::Id(a.id.clone())).0),
            ["b", "c"]
        );
        // an id on another topic still marks a point in time
        assert_eq!(
            ids(&s.subscribe(&topics(&["t1"]), &Since::Id(b.id.clone())).0),
            ["c"]
        );
        // an unknown id acts like all
        assert_eq!(
            ids(&s.subscribe(&topics(&["t1"]), &Since::Id("nope".into())).0),
            ["a", "c"]
        );
    }

    #[test]
    fn bounded_and_expiring() {
        let s = Store::new(2, 3600);
        for x in ["a", "b", "c"] {
            s.publish("t", x.into(), None);
        }
        assert_eq!(
            ids(&s.subscribe(&topics(&["t"]), &Since::All).0),
            ["b", "c"]
        );
        let s = Store::new(10, 0);
        s.publish("t", "a".into(), None);
        s.expire();
        assert!(s.is_empty());
    }

    #[test]
    fn live_messages_arrive_once() {
        let s = Store::new(10, 3600);
        s.publish("t", "old".into(), None);
        let (backlog, mut rx) = s.subscribe(&topics(&["t"]), &Since::All);
        s.publish("t", "new".into(), None);
        assert_eq!(ids(&backlog), ["old"]);
        assert_eq!(rx.try_recv().unwrap().message, "new");
        assert!(rx.try_recv().is_err());
    }
}

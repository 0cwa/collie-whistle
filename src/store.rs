//! The message cache: bounded in count, expiring by age, shared by all subscribers.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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

/// Live messages for one topic's subscribers, per topic so that a flood on one topic cannot
/// make subscribers of another lag.
const CHANNEL_CAPACITY: usize = 256;

#[derive(Debug, PartialEq, Eq)]
pub struct RateLimited;

#[derive(Default)]
struct Topic {
    /// Sequence numbers of this topic's cached messages, oldest first.
    seqs: VecDeque<u64>,
    /// Exists while the topic has subscribers.
    tx: Option<broadcast::Sender<Arc<Message>>>,
}

struct Inner {
    /// Every cached message by sequence number, which is also publish order.
    msgs: BTreeMap<u64, Arc<Message>>,
    topics: HashMap<String, Topic>,
    next_seq: u64,
    tokens: f64,
    refilled: Instant,
}

impl Inner {
    /// Removes a message that has already left `msgs` from its topic.
    fn forget(&mut self, m: &Message) {
        if let Some(t) = self.topics.get_mut(&m.topic) {
            if t.seqs.front() == Some(&m.seq) {
                t.seqs.pop_front();
            } else {
                t.seqs.retain(|s| *s != m.seq);
            }
            if t.seqs.is_empty() && t.tx.is_none() {
                self.topics.remove(&m.topic);
            }
        }
    }

    fn drop_oldest(&mut self) {
        if let Some((_, m)) = self.msgs.pop_first() {
            self.forget(&m);
        }
    }
}

pub struct Limits {
    /// Messages kept in total.
    pub max_messages: usize,
    /// Messages kept per topic.
    pub max_per_topic: usize,
    /// How long a message is kept, in seconds.
    pub max_age_secs: u64,
    /// Publishes per second over all topics, 0 for no limit.
    pub publish_rate: u32,
}

pub struct Store {
    inner: Mutex<Inner>,
    limits: Limits,
}

/// What `subscribe` hands back: cached messages first, then everything live from `rxs`.
pub struct Subscription {
    pub backlog: Vec<Arc<Message>>,
    pub rxs: Vec<broadcast::Receiver<Arc<Message>>>,
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
    pub fn new(mut limits: Limits) -> Store {
        limits.max_messages = limits.max_messages.max(1);
        limits.max_per_topic = limits.max_per_topic.max(1);
        Store {
            inner: Mutex::new(Inner {
                msgs: BTreeMap::new(),
                topics: HashMap::new(),
                next_seq: 1,
                tokens: f64::from(limits.publish_rate),
                refilled: Instant::now(),
            }),
            limits,
        }
    }

    fn purge(&self, g: &mut Inner, now: u64) {
        while let Some((_, front)) = g.msgs.first_key_value() {
            if front.expires <= now || g.msgs.len() > self.limits.max_messages {
                g.drop_oldest();
            } else {
                break;
            }
        }
    }

    /// Drops expired messages and the channels of topics nobody listens to. Publishing and
    /// subscribing also expire messages; this is for idle servers.
    pub fn expire(&self) {
        let mut g = self.inner.lock().unwrap();
        self.purge(&mut g, now());
        g.topics.retain(|_, t| {
            if t.tx.as_ref().is_some_and(|tx| tx.receiver_count() == 0) {
                t.tx = None;
            }
            !t.seqs.is_empty() || t.tx.is_some()
        });
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
    ) -> Result<Arc<Message>, RateLimited> {
        let t = now();
        let mut g = self.inner.lock().unwrap();
        let rate = f64::from(self.limits.publish_rate);
        if rate > 0.0 {
            let elapsed = g.refilled.elapsed().as_secs_f64();
            g.refilled = Instant::now();
            g.tokens = (g.tokens + elapsed * rate).min(rate);
            if g.tokens < 1.0 {
                return Err(RateLimited);
            }
            g.tokens -= 1.0;
        }
        let seq = g.next_seq;
        g.next_seq += 1;
        let m = Arc::new(Message {
            id: new_id(),
            time: t,
            expires: t.saturating_add(self.limits.max_age_secs),
            event: "message",
            topic: topic.to_string(),
            message,
            encoding,
            seq,
        });
        let Inner { msgs, topics, .. } = &mut *g;
        let tp = topics.entry(topic.to_string()).or_default();
        if tp.seqs.len() >= self.limits.max_per_topic {
            if let Some(old) = tp.seqs.pop_front() {
                msgs.remove(&old);
            }
        }
        tp.seqs.push_back(seq);
        msgs.insert(seq, m.clone());
        match &tp.tx {
            Some(tx) if tx.receiver_count() == 0 => tp.tx = None,
            Some(tx) => {
                let _ = tx.send(m.clone());
            }
            None => {}
        }
        self.purge(&mut g, t);
        Ok(m)
    }

    /// Messages for `topics` selected by `since`, plus a receiver per topic for everything
    /// published after. A `since` id may belong to any topic: it marks a point in time. An
    /// unknown id (never seen, or expired) acts like `all`.
    pub fn subscribe(&self, topics: &HashSet<String>, since: &Since) -> Subscription {
        let mut g = self.inner.lock().unwrap();
        self.purge(&mut g, now());
        let after = match since {
            Since::Id(id) => g.msgs.values().find(|m| &m.id == id).map(|m| m.seq),
            _ => None,
        };
        let Inner {
            msgs, topics: all, ..
        } = &mut *g;
        let mut rxs = Vec::new();
        let mut seqs: Vec<u64> = Vec::new();
        for name in topics {
            let tp = all.entry(name.clone()).or_default();
            let tx = tp
                .tx
                .get_or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
            rxs.push(tx.subscribe());
            match since {
                Since::None => {}
                Since::Latest => seqs.extend(tp.seqs.back()),
                Since::All | Since::Id(_) => {
                    seqs.extend(tp.seqs.iter().filter(|s| after.is_none_or(|a| **s > a)))
                }
            }
        }
        seqs.sort_unstable();
        if *since == Since::Latest {
            seqs = seqs.pop().into_iter().collect();
        }
        let backlog = seqs.iter().filter_map(|s| msgs.get(s).cloned()).collect();
        Subscription { backlog, rxs }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast::error::TryRecvError;

    fn store(max: usize, per_topic: usize, age: u64, rate: u32) -> Store {
        Store::new(Limits {
            max_messages: max,
            max_per_topic: per_topic,
            max_age_secs: age,
            publish_rate: rate,
        })
    }

    fn topics(t: &[&str]) -> HashSet<String> {
        t.iter().map(|s| s.to_string()).collect()
    }

    fn texts(v: &[Arc<Message>]) -> Vec<&str> {
        v.iter().map(|m| m.message.as_str()).collect()
    }

    fn put(s: &Store, topic: &str, text: &str) -> Arc<Message> {
        s.publish(topic, text.into(), None).unwrap()
    }

    #[test]
    fn since_variants() {
        let s = store(100, 100, 3600, 0);
        let a = put(&s, "t1", "a");
        let b = put(&s, "t2", "b");
        put(&s, "t1", "c");
        let both = topics(&["t1", "t2"]);
        assert!(s.subscribe(&both, &Since::None).backlog.is_empty());
        assert_eq!(
            texts(&s.subscribe(&both, &Since::All).backlog),
            ["a", "b", "c"]
        );
        assert_eq!(texts(&s.subscribe(&both, &Since::Latest).backlog), ["c"]);
        let by_id = s.subscribe(&both, &Since::Id(a.id.clone()));
        assert_eq!(texts(&by_id.backlog), ["b", "c"]);
        // an id on another topic still marks a point in time
        let other = s.subscribe(&topics(&["t1"]), &Since::Id(b.id.clone()));
        assert_eq!(texts(&other.backlog), ["c"]);
        // an unknown id acts like all
        let unknown = s.subscribe(&topics(&["t1"]), &Since::Id("nope".into()));
        assert_eq!(texts(&unknown.backlog), ["a", "c"]);
    }

    #[test]
    fn bounded_and_expiring() {
        let s = store(2, 100, 3600, 0);
        for x in ["a", "b", "c"] {
            put(&s, "t", x);
        }
        assert_eq!(
            texts(&s.subscribe(&topics(&["t"]), &Since::All).backlog),
            ["b", "c"]
        );
        let s = store(10, 100, 0, 0);
        put(&s, "t", "a");
        s.expire();
        assert!(s.is_empty());
    }

    #[test]
    fn a_flooded_topic_cannot_evict_another_topic() {
        let s = store(10_000, 100, 3600, 0);
        put(&s, "quiet", "keep me");
        for i in 0..5000 {
            put(&s, "flood", &i.to_string());
        }
        assert_eq!(s.len(), 101);
        let q = s.subscribe(&topics(&["quiet"]), &Since::All).backlog;
        assert_eq!(texts(&q), ["keep me"]);
        let f = s.subscribe(&topics(&["flood"]), &Since::All).backlog;
        assert_eq!(f.len(), 100);
        assert_eq!(f[0].message, "4900"); // the oldest of that topic went first
        assert_eq!(f[99].message, "4999");
    }

    #[test]
    fn a_flooded_topic_does_not_lag_another_topics_subscriber() {
        let s = store(10_000, 100, 3600, 0);
        let mut flood = s.subscribe(&topics(&["flood"]), &Since::None);
        let mut quiet = s.subscribe(&topics(&["quiet"]), &Since::None);
        for i in 0..(CHANNEL_CAPACITY * 2) {
            put(&s, "flood", &i.to_string());
        }
        // the subscriber that was flooded is told it fell behind ...
        assert!(matches!(
            flood.rxs[0].try_recv(),
            Err(TryRecvError::Lagged(_))
        ));
        // ... the other one is untouched and still gets its own messages
        assert!(matches!(quiet.rxs[0].try_recv(), Err(TryRecvError::Empty)));
        put(&s, "quiet", "hello");
        assert_eq!(quiet.rxs[0].try_recv().unwrap().message, "hello");
    }

    #[test]
    fn channel_goes_away_with_the_last_subscriber() {
        let s = store(10, 10, 3600, 0);
        let sub = s.subscribe(&topics(&["t"]), &Since::None);
        assert_eq!(s.inner.lock().unwrap().topics.len(), 1);
        drop(sub);
        s.expire();
        assert!(s.inner.lock().unwrap().topics.is_empty());
    }

    #[test]
    fn publish_rate_is_limited_and_refills() {
        let s = store(100, 100, 3600, 2);
        assert!(s.publish("t", "1".into(), None).is_ok());
        assert!(s.publish("t", "2".into(), None).is_ok());
        assert_eq!(s.publish("t", "3".into(), None).unwrap_err(), RateLimited);
        std::thread::sleep(std::time::Duration::from_millis(600));
        assert!(s.publish("t", "4".into(), None).is_ok());
    }

    #[test]
    fn live_messages_arrive_once() {
        let s = store(10, 10, 3600, 0);
        put(&s, "t", "old");
        let mut sub = s.subscribe(&topics(&["t"]), &Since::All);
        put(&s, "t", "new");
        assert_eq!(texts(&sub.backlog), ["old"]);
        assert_eq!(sub.rxs[0].try_recv().unwrap().message, "new");
        assert!(sub.rxs[0].try_recv().is_err());
    }
}

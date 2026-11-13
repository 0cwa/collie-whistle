//! The message cache: bounded in count, expiring by age, shared by all subscribers.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rand::distr::{Alphanumeric, SampleString};
use serde::Serialize;
use tokio::sync::broadcast::{self, error::TryRecvError};

// Test-only scheduling point; each test thread owns its one-shot callback.
#[cfg(test)]
std::thread_local! {
    static AFTER_FIRST_RECEIVER_FOR_TEST: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}

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

/// Why a subscription was refused. All of them are answered with 429.
#[derive(Debug, PartialEq, Eq)]
pub enum SubscribeError {
    RateLimited,
    TooManyTopics,
    TooManySubscribers,
}

/// Token bucket: `rate` events per second, bursts of up to `rate`. A rate of 0 means no limit.
struct Bucket {
    rate: f64,
    tokens: f64,
    refilled: Instant,
}

impl Bucket {
    fn new(rate: u32) -> Bucket {
        Bucket {
            rate: f64::from(rate),
            tokens: f64::from(rate),
            refilled: Instant::now(),
        }
    }

    fn take(&mut self) -> bool {
        if self.rate <= 0.0 {
            return true;
        }
        let now = Instant::now();
        let elapsed = now.duration_since(self.refilled).as_secs_f64();
        self.refilled = now;
        self.tokens = (self.tokens + elapsed * self.rate).min(self.rate);
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

struct Topic {
    /// Sequence numbers of this topic's cached messages, oldest first.
    seqs: VecDeque<u64>,
    /// Exists exactly while the topic has live subscribers.
    tx: Option<broadcast::Sender<Arc<Message>>>,
    publishes: Bucket,
}

struct Inner {
    /// Every cached message by sequence number, which is also publish order.
    msgs: BTreeMap<u64, Arc<Message>>,
    topics: HashMap<String, Topic>,
    next_seq: u64,
    /// Topics that currently have a channel.
    channels: usize,
    publishes: Bucket,
    subscribes: Bucket,
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
    /// Publishes per second to one topic, 0 for no limit.
    pub topic_publish_rate: u32,
    /// Subscribe requests per second, 0 for no limit.
    pub subscribe_rate: u32,
    /// Topics that may have live subscribers at once.
    pub max_live_topics: usize,
    /// Live subscribers of one topic.
    pub max_subscribers_per_topic: usize,
}

pub struct Store {
    inner: Mutex<Inner>,
    limits: Limits,
}

/// The live part of a subscription. Dropping it releases the channels nobody else listens to.
pub struct Live {
    store: Arc<Store>,
    topics: Vec<String>,
    pub rxs: Vec<broadcast::Receiver<Arc<Message>>>,
}

impl Drop for Live {
    fn drop(&mut self) {
        self.rxs.clear(); // so that receiver_count() no longer includes this subscriber
        self.store.release(&self.topics);
    }
}

impl Live {
    pub(crate) fn drain_ready(&mut self, first: Arc<Message>) -> Option<Vec<Arc<Message>>> {
        self.store.drain_live(&mut self.rxs, first)
    }
}

/// What `subscribe` hands back: cached messages first, then everything live from `live`
/// (none for a poll, which never creates a channel).
pub struct Subscription {
    pub backlog: Vec<Arc<Message>>,
    pub live: Option<Live>,
}

impl Message {
    pub fn seq(&self) -> u64 {
        self.seq
    }
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
                channels: 0,
                publishes: Bucket::new(limits.publish_rate),
                subscribes: Bucket::new(limits.subscribe_rate),
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

    /// Takes a consistent snapshot of all ready live messages.
    ///
    /// Publishing and this drain share the store mutex, so a lower-sequence message cannot be
    /// published to a topic after that topic was drained but before a higher-sequence message
    /// from another topic was drained.
    pub(crate) fn drain_live(
        &self,
        rxs: &mut [broadcast::Receiver<Arc<Message>>],
        first: Arc<Message>,
    ) -> Option<Vec<Arc<Message>>> {
        let _g = self.inner.lock().unwrap();
        let mut batch = vec![first];
        for rx in rxs {
            loop {
                match rx.try_recv() {
                    Ok(m) => batch.push(m),
                    Err(TryRecvError::Empty | TryRecvError::Closed) => break,
                    Err(TryRecvError::Lagged(_)) => return None,
                }
            }
            #[cfg(test)]
            AFTER_FIRST_RECEIVER_FOR_TEST.with(|hook| {
                if let Some(after_scan) = hook.take() {
                    after_scan();
                }
            });
        }
        batch.sort_by_key(|m| m.seq());
        Some(batch)
    }

    /// Topics held in memory: those with cached messages or live subscribers.
    pub fn topic_count(&self) -> usize {
        self.inner.lock().unwrap().topics.len()
    }

    fn release(&self, topics: &[String]) {
        let mut g = self.inner.lock().unwrap();
        for name in topics {
            let Some(t) = g.topics.get_mut(name) else {
                continue;
            };
            if t.tx.as_ref().is_some_and(|tx| tx.receiver_count() == 0) {
                t.tx = None;
                let empty = t.seqs.is_empty();
                g.channels -= 1;
                if empty {
                    g.topics.remove(name);
                }
            }
        }
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
        if !g.publishes.take() {
            return Err(RateLimited);
        }
        let seq = g.next_seq;
        let tp = g.topics.entry(topic.to_string()).or_insert_with(|| Topic {
            seqs: VecDeque::new(),
            tx: None,
            publishes: Bucket::new(self.limits.topic_publish_rate),
        });
        if !tp.publishes.take() {
            return Err(RateLimited);
        }
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
        let tp = topics.get_mut(topic).expect("created above");
        if tp.seqs.len() >= self.limits.max_per_topic {
            if let Some(old) = tp.seqs.pop_front() {
                msgs.remove(&old);
            }
        }
        tp.seqs.push_back(seq);
        msgs.insert(seq, m.clone());
        if let Some(tx) = &tp.tx {
            let _ = tx.send(m.clone());
        }
        self.purge(&mut g, t);
        Ok(m)
    }

    /// Messages for `topics` selected by `since`. With `live`, also a receiver per topic for
    /// everything published after; a poll (`live` false) allocates nothing per topic. A `since`
    /// id may belong to any topic: it marks a point in time. An unknown id (never seen, or
    /// expired) acts like `all`.
    pub fn subscribe(
        self: &Arc<Self>,
        topics: &HashSet<String>,
        since: &Since,
        live: bool,
    ) -> Result<Subscription, SubscribeError> {
        let mut names: Vec<&String> = topics.iter().collect();
        names.sort(); // a stable order for the receivers
        let mut g = self.inner.lock().unwrap();
        if !g.subscribes.take() {
            return Err(SubscribeError::RateLimited);
        }
        self.purge(&mut g, now());
        if live {
            let mut new = 0;
            for n in &names {
                match g.topics.get(*n).and_then(|t| t.tx.as_ref()) {
                    Some(tx) if tx.receiver_count() >= self.limits.max_subscribers_per_topic => {
                        return Err(SubscribeError::TooManySubscribers)
                    }
                    Some(_) => {}
                    None => new += 1,
                }
            }
            if g.channels + new > self.limits.max_live_topics {
                return Err(SubscribeError::TooManyTopics);
            }
        }
        let after = match since {
            Since::Id(id) => g.msgs.values().find(|m| &m.id == id).map(|m| m.seq),
            _ => None,
        };
        let Inner {
            msgs,
            topics: all,
            channels,
            ..
        } = &mut *g;
        let mut rxs = Vec::new();
        let mut seqs: Vec<u64> = Vec::new();
        for name in &names {
            if live {
                let tp = all.entry((*name).clone()).or_insert_with(|| Topic {
                    seqs: VecDeque::new(),
                    tx: None,
                    publishes: Bucket::new(self.limits.topic_publish_rate),
                });
                if tp.tx.is_none() {
                    *channels += 1;
                }
                let tx = tp
                    .tx
                    .get_or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
                rxs.push(tx.subscribe());
            }
            let Some(tp) = all.get(*name) else { continue };
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
        let live = live.then(|| Live {
            store: self.clone(),
            topics: names.into_iter().cloned().collect(),
            rxs,
        });
        Ok(Subscription { backlog, live })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast::error::TryRecvError;

    fn limits() -> Limits {
        Limits {
            max_messages: 10_000,
            max_per_topic: 100,
            max_age_secs: 3600,
            publish_rate: 0,
            topic_publish_rate: 0,
            subscribe_rate: 0,
            max_live_topics: 4096,
            max_subscribers_per_topic: 16,
        }
    }

    fn store(l: Limits) -> Arc<Store> {
        Arc::new(Store::new(l))
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

    fn sub(s: &Arc<Store>, t: &[&str], since: &Since) -> Subscription {
        s.subscribe(&topics(t), since, true).unwrap()
    }

    fn poll(s: &Arc<Store>, t: &[&str], since: &Since) -> Vec<Arc<Message>> {
        s.subscribe(&topics(t), since, false).unwrap().backlog
    }

    #[test]
    fn since_variants() {
        let s = store(limits());
        let a = put(&s, "t1", "a");
        let b = put(&s, "t2", "b");
        put(&s, "t1", "c");
        let both = ["t1", "t2"];
        assert!(poll(&s, &both, &Since::None).is_empty());
        assert_eq!(texts(&poll(&s, &both, &Since::All)), ["a", "b", "c"]);
        assert_eq!(texts(&poll(&s, &both, &Since::Latest)), ["c"]);
        assert_eq!(
            texts(&poll(&s, &both, &Since::Id(a.id.clone()))),
            ["b", "c"]
        );
        // an id on another topic still marks a point in time
        assert_eq!(texts(&poll(&s, &["t1"], &Since::Id(b.id.clone()))), ["c"]);
        // an unknown id acts like all
        assert_eq!(
            texts(&poll(&s, &["t1"], &Since::Id("nope".into()))),
            ["a", "c"]
        );
    }

    #[test]
    fn bounded_and_expiring() {
        let s = store(Limits {
            max_messages: 2,
            ..limits()
        });
        for x in ["a", "b", "c"] {
            put(&s, "t", x);
        }
        assert_eq!(texts(&poll(&s, &["t"], &Since::All)), ["b", "c"]);
        let s = store(Limits {
            max_age_secs: 0,
            ..limits()
        });
        put(&s, "t", "a");
        s.expire();
        assert!(s.is_empty());
        assert_eq!(s.topic_count(), 0);
    }

    #[test]
    fn a_flooded_topic_cannot_evict_another_topic() {
        let s = store(limits());
        put(&s, "quiet", "keep me");
        for i in 0..5000 {
            put(&s, "flood", &i.to_string());
        }
        assert_eq!(s.len(), 101);
        assert_eq!(texts(&poll(&s, &["quiet"], &Since::All)), ["keep me"]);
        let f = poll(&s, &["flood"], &Since::All);
        assert_eq!(f.len(), 100);
        assert_eq!(f[0].message, "4900"); // the oldest of that topic went first
        assert_eq!(f[99].message, "4999");
    }

    #[test]
    fn a_flooded_topic_does_not_lag_another_topics_subscriber() {
        let s = store(limits());
        let mut flood = sub(&s, &["flood"], &Since::None).live.unwrap();
        let mut quiet = sub(&s, &["quiet"], &Since::None).live.unwrap();
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
    fn ordered_drain_rechecks_a_topic_drained_before_a_later_publish() {
        let s = store(limits());
        let mut live = sub(&s, &["a", "b"], &Since::None).live.unwrap();

        put(&s, "a", "1");
        let first = live.rxs[0].try_recv().unwrap();
        assert_eq!(first.seq(), 1);
        assert!(matches!(live.rxs[0].try_recv(), Err(TryRecvError::Empty)));

        // This is the gap that the former per-topic loop had between draining A and B.
        let second = put(&s, "a", "2");
        let third = put(&s, "b", "3");
        let batch = live.drain_ready(first).unwrap();
        assert_eq!(
            batch.iter().map(|m| m.seq()).collect::<Vec<_>>(),
            [1, second.seq(), third.seq()]
        );

        // A reconnect at the last delivered ID sees no older message left behind it.
        let resumed = s
            .subscribe(&topics(&["a", "b"]), &Since::Id(third.id.clone()), false)
            .unwrap();
        assert!(resumed.backlog.is_empty());
    }

    #[test]
    fn publication_between_receiver_scans_cannot_advance_the_cursor() {
        use std::sync::mpsc;
        use std::time::Duration;

        let s = store(limits());
        let mut live = sub(&s, &["a", "b"], &Since::None).live.unwrap();
        let first = put(&s, "a", "1");
        let received = live.rxs[0].try_recv().unwrap();
        let (scanned, wait_for_scan) = mpsc::channel();
        let (attempted, wait_for_attempt) = mpsc::channel();
        let publishing_store = s.clone();
        let publisher = std::thread::spawn(move || {
            wait_for_scan.recv_timeout(Duration::from_secs(5)).unwrap();
            // If publication is excluded, let the drain finish before blocking in publish.
            // Without that exclusion, finish both publishes in the A/B scan gap.
            let publication_available = publishing_store.inner.try_lock().is_ok();
            if !publication_available {
                attempted.send(()).unwrap();
            }
            let second = put(&publishing_store, "a", "2");
            let third = put(&publishing_store, "b", "3");
            if publication_available {
                attempted.send(()).unwrap();
            }
            (second, third)
        });
        AFTER_FIRST_RECEIVER_FOR_TEST.with(|hook| {
            hook.replace(Some(Box::new(move || {
                scanned.send(()).unwrap();
                wait_for_attempt
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            })));
        });

        let batch = live.drain_ready(received).unwrap();
        let (second, third) = publisher.join().unwrap();
        // A publication started after A's scan belongs entirely to the next cut.
        // Removing the drain's mutex lets this batch contain [1, 3] instead.
        assert_eq!(texts(&batch), ["1"]);
        let resumed = poll(&s, &["a", "b"], &Since::Id(first.id.clone()));
        assert_eq!(texts(&resumed), ["2", "3"]);
        let next = live.rxs[0].try_recv().unwrap();
        assert_eq!(next.id, second.id);
        let batch = live.drain_ready(next).unwrap();
        assert_eq!(texts(&batch), ["2", "3"]);
        assert!(poll(&s, &["a", "b"], &Since::Id(third.id.clone())).is_empty());
    }

    #[test]
    fn channel_goes_away_with_the_last_subscriber() {
        let s = store(limits());
        let one = sub(&s, &["t"], &Since::None);
        let two = sub(&s, &["t"], &Since::None);
        assert_eq!(s.topic_count(), 1);
        drop(one);
        assert_eq!(s.topic_count(), 1); // the second subscriber still listens
        put(&s, "t", "x");
        drop(two);
        assert_eq!(s.topic_count(), 1); // the cached message keeps the topic, the channel is gone
        assert_eq!(s.inner.lock().unwrap().channels, 0);
    }

    fn random_topics(n: usize, round: usize) -> HashSet<String> {
        (0..n).map(|i| format!("flood{round}x{i}")).collect()
    }

    #[test]
    fn polling_many_random_topics_allocates_nothing() {
        let s = store(limits());
        for round in 0..1600 {
            let sub = s
                .subscribe(&random_topics(50, round), &Since::All, false)
                .unwrap();
            assert!(sub.live.is_none() && sub.backlog.is_empty());
        }
        assert_eq!(s.topic_count(), 0);
    }

    #[test]
    fn live_topics_are_capped_and_released() {
        let s = store(Limits {
            max_live_topics: 200,
            ..limits()
        });
        let mut held = Vec::new();
        let mut refused = 0;
        for round in 0..1600 {
            match s.subscribe(&random_topics(50, round), &Since::None, true) {
                Ok(sub) => held.push(sub),
                Err(e) => {
                    assert_eq!(e, SubscribeError::TooManyTopics);
                    refused += 1;
                }
            }
            assert!(s.topic_count() <= 200);
        }
        assert_eq!(held.len(), 4);
        assert_eq!(refused, 1596);
        drop(held);
        assert_eq!(s.topic_count(), 0);
        assert_eq!(s.inner.lock().unwrap().channels, 0);
    }

    #[test]
    fn subscribers_per_topic_are_capped() {
        let s = store(Limits {
            max_subscribers_per_topic: 2,
            ..limits()
        });
        let _a = sub(&s, &["t"], &Since::None);
        let _b = sub(&s, &["t"], &Since::None);
        let third = s.subscribe(&topics(&["t"]), &Since::None, true);
        assert_eq!(third.err(), Some(SubscribeError::TooManySubscribers));
    }

    #[test]
    fn subscribe_rate_is_limited() {
        let s = store(Limits {
            subscribe_rate: 2,
            ..limits()
        });
        assert!(s.subscribe(&topics(&["t"]), &Since::All, false).is_ok());
        assert!(s.subscribe(&topics(&["t"]), &Since::All, false).is_ok());
        let third = s.subscribe(&topics(&["t"]), &Since::All, false);
        assert_eq!(third.err(), Some(SubscribeError::RateLimited));
    }

    #[test]
    fn publish_rate_is_limited_and_refills() {
        let s = store(Limits {
            publish_rate: 2,
            ..limits()
        });
        assert!(s.publish("t", "1".into(), None).is_ok());
        assert!(s.publish("t", "2".into(), None).is_ok());
        assert_eq!(s.publish("t", "3".into(), None).unwrap_err(), RateLimited);
        std::thread::sleep(std::time::Duration::from_millis(600));
        assert!(s.publish("t", "4".into(), None).is_ok());
    }

    #[test]
    fn one_topic_cannot_use_up_everyone_elses_publishes() {
        let s = store(Limits {
            topic_publish_rate: 5,
            publish_rate: 1000,
            ..limits()
        });
        let accepted = (0..50)
            .filter(|_| s.publish("flood", "x".into(), None).is_ok())
            .count();
        assert_eq!(accepted, 5);
        assert!(s.publish("other", "ok".into(), None).is_ok());
    }

    #[test]
    fn live_messages_arrive_once() {
        let s = store(limits());
        put(&s, "t", "old");
        let mut sub = sub(&s, &["t"], &Since::All);
        put(&s, "t", "new");
        assert_eq!(texts(&sub.backlog), ["old"]);
        let live = sub.live.as_mut().unwrap();
        assert_eq!(live.rxs[0].try_recv().unwrap().message, "new");
        assert!(live.rxs[0].try_recv().is_err());
    }
}

///! This implements a time-based LRU cache for checking gossipsub message duplicates.
use fnv::FnvHashMap;
use std::collections::hash_map::{
    self,
    Entry::{Occupied, Vacant},
};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

struct ExpiringElement<Element> {
    /// The element that expires
    element: Element,
    /// The expire time.
    expires: Instant,
}

pub struct TimeCache<Key, Value> {
    /// Mapping a key to its value together with its latest expire time (can be updated through
    /// reinserts).
    map: FnvHashMap<Key, ExpiringElement<Value>>,
    /// An ordered list of keys by expires time.
    list: VecDeque<ExpiringElement<Key>>,
    /// The time elements remain in the cache.
    ttl: Duration,
}

pub struct OccupiedEntry<'a, K, V> {
    expiration: Instant,
    entry: hash_map::OccupiedEntry<'a, K, ExpiringElement<V>>,
    list: &'a mut VecDeque<ExpiringElement<K>>,
}

impl<'a, K, V> OccupiedEntry<'a, K, V>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub fn into_mut(self) -> &'a mut V {
        &mut self.entry.into_mut().element
    }

    pub fn insert_without_updating_expiration(&mut self, value: V) -> V {
        //keep old expiration, only replace value of element
        ::std::mem::replace(&mut self.entry.get_mut().element, value)
    }

    pub fn insert_and_update_expiration(&mut self, value: V) -> V {
        //We push back an additional element, the first reference in the list will be ignored
        // since we also updated the expires in the map, see below.
        self.list.push_back(ExpiringElement {
            element: self.entry.key().clone(),
            expires: self.expiration,
        });
        self.entry
            .insert(ExpiringElement {
                element: value,
                expires: self.expiration,
            })
            .element
    }
}

pub struct VacantEntry<'a, K, V> {
    expiration: Instant,
    entry: hash_map::VacantEntry<'a, K, ExpiringElement<V>>,
    list: &'a mut VecDeque<ExpiringElement<K>>,
}

impl<'a, K, V> VacantEntry<'a, K, V>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub fn insert(self, value: V) -> &'a mut V {
        self.list.push_back(ExpiringElement {
            element: self.entry.key().clone(),
            expires: self.expiration,
        });
        &mut self
            .entry
            .insert(ExpiringElement {
                element: value,
                expires: self.expiration,
            })
            .element
    }
}

pub enum Entry<'a, K: 'a, V: 'a> {
    Occupied(OccupiedEntry<'a, K, V>),
    Vacant(VacantEntry<'a, K, V>),
}

impl<'a, K: 'a, V: 'a> Entry<'a, K, V>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(default()),
        }
    }
}

impl<Key, Value> TimeCache<Key, Value>
where
    Key: Eq + std::hash::Hash + Clone,
{
    pub fn new(ttl: Duration) -> Self {
        TimeCache {
            map: FnvHashMap::default(),
            list: VecDeque::new(),
            ttl,
        }
    }

    fn remove_expired_keys(&mut self, now: Instant) {
        while let Some(element) = self.list.pop_front() {
            if element.expires > now {
                self.list.push_front(element);
                break;
            }
            if let Occupied(entry) = self.map.entry(element.element.clone()) {
                if entry.get().expires <= now {
                    entry.remove();
                }
            }
        }
    }

    pub fn entry(&mut self, key: Key) -> Entry<Key, Value> {
        let now = Instant::now();
        self.remove_expired_keys(now);
        match self.map.entry(key) {
            Occupied(entry) => Entry::Occupied(OccupiedEntry {
                expiration: now + self.ttl,
                entry,
                list: &mut self.list,
            }),
            Vacant(entry) => Entry::Vacant(VacantEntry {
                expiration: now + self.ttl,
                entry,
                list: &mut self.list,
            }),
        }
    }
}

pub struct DuplicateCache<Key>(TimeCache<Key, ()>);

impl<Key> DuplicateCache<Key>
where
    Key: Eq + std::hash::Hash + Clone,
{
    pub fn new(ttl: Duration) -> Self {
        Self(TimeCache::new(ttl))
    }

    // Inserts new elements and removes any expired elements.
    //
    // If the key was not present this returns `true`. If the value was already present this
    // returns `false`.
    pub fn insert(&mut self, key: Key) -> bool {
        if let Entry::Vacant(entry) = self.0.entry(key) {
            entry.insert(());
            true
        } else {
            false
        }
    }
}

pub mod benchmark_helpers {
    use rand::rngs::SmallRng;
    use rand::seq::SliceRandom;
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use rand::{Rng, SeedableRng};

    use super::DuplicateCache;
    use lru_time_cache::{Entry, LruCache};
    use std::cmp::max;

    pub trait DuplicateCacheTrait<K> {
        fn insert(&mut self, key: K) -> bool;
    }

    impl<K> DuplicateCacheTrait<K> for DuplicateCache<K>
    where
        K: Eq + std::hash::Hash + Clone,
    {
        fn insert(&mut self, key: K) -> bool {
            DuplicateCache::insert(self, key)
        }
    }

    impl<K> DuplicateCacheTrait<K> for LruCache<K, ()>
    where
        K: Ord + Clone,
    {
        fn insert(&mut self, key: K) -> bool {
            match self.entry(key) {
                Entry::Occupied(_) => false,
                Entry::Vacant(entry) => {
                    entry.insert(());
                    true
                }
            }
        }
    }

    fn benchmark_duplicate_cache<T: DuplicateCacheTrait<u64>>(
        mut cache: T,
        inserts_per_sec: u64,
        reinsert_rate: f64,
        total_inserts: u64,
        seed: <SmallRng as SeedableRng>::Seed,
    ) -> (Duration, u64) {
        let mut last_sleep = Instant::now();
        let mut rng = SmallRng::from_seed(seed);
        let mut doubles = Vec::with_capacity(100002);
        //only store up to 100000 double keys
        let double_interval = max(1, total_inserts / 100000);
        let mut slept = Duration::from_secs(0);
        let mut count = 0;
        for i in 0..total_inserts {
            let key = if i > 0 && rng.gen_bool(reinsert_rate) {
                *doubles.choose(&mut rng).unwrap()
            } else {
                rng.gen()
            };
            if cache.insert(key) {
                count += 1;
            }
            if i % double_interval == 0 {
                doubles.push(key);
            }
            if i > 0 && i % inserts_per_sec == 0 {
                let target = last_sleep + Duration::from_secs(1);
                let now = Instant::now();
                if target > now {
                    sleep(target - now);
                    slept += target - now;
                }
                last_sleep = Instant::now();
            }
        }
        (slept, count)
    }

    pub fn find_max_inserts_per_sec<T: DuplicateCacheTrait<u64>>(
        create: fn(Duration) -> T,
        reinsert_rate: f64,
        ttl: Duration,
        target_time: Duration,
        start_inserts_per_sec: u64,
    ) {
        let mut rng = SmallRng::from_entropy();
        let mut slept = target_time;
        let mut inserts_per_sec = start_inserts_per_sec;
        let factor = target_time.as_secs_f64() / ttl.as_secs_f64();
        while slept > ttl {
            let now = Instant::now();
            let total_inserts = (inserts_per_sec as f64 * factor) as u64;
            let (s, real_insertions) = benchmark_duplicate_cache(
                create(ttl),
                inserts_per_sec,
                reinsert_rate,
                total_inserts,
                rng.gen(),
            );
            let duration = Instant::now() - now;
            slept = s;
            println!(
                "Sleep time with {} inserts per sec: {:?} / {:?}, in total that are {} \
            insertions per second. Real insertions: {}",
                inserts_per_sec,
                slept,
                duration,
                total_inserts as f64 / duration.as_secs_f64(),
                real_insertions
            );

            inserts_per_sec *= 2;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn cache_added_entries_exist() {
        let mut cache = DuplicateCache::new(Duration::from_secs(10));

        cache.insert("t");
        cache.insert("e");

        // Should report that 't' and 't' already exists
        assert!(!cache.insert("t"));
        assert!(!cache.insert("e"));
    }

    #[test]
    fn cache_entries_expire() {
        let mut cache = DuplicateCache::new(Duration::from_millis(100));

        cache.insert("t");
        assert!(!cache.insert("t"));
        cache.insert("e");
        //assert!(!cache.insert("t"));
        assert!(!cache.insert("e"));
        // sleep until cache expiry
        std::thread::sleep(Duration::from_millis(101));
        // add another element to clear previous cache
        cache.insert("s");

        // should be removed from the cache
        assert!(cache.insert("t"));
    }
}

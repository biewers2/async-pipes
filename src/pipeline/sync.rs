use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire};

/// A generic data structure to synchronize tasks being done among multiple threads.
///
/// The IDs of the tasks must be registered in order to be tracked. Tasks can be marked as "started"
/// or "ended" which increments and decrements (respectively) the respective task ID counter. A
/// synchronizer is considered "complete" if all task ID counters are zero.
#[derive(Default, Debug)]
pub struct Synchronizer {
    /// Maintain a total count to easily check if all task ID counters are zero
    total_count: AtomicUsize,
    counts: HashMap<String, AtomicUsize>,
}

impl Synchronizer {
    pub fn register<S: Into<String>>(&mut self, id: S) {
        self.counts.insert(id.into(), AtomicUsize::new(0));
    }

    pub fn started<S: AsRef<str>>(&self, id: S) {
        self.started_many(id, 1);
    }

    pub fn started_many<S: AsRef<str>>(&self, id: S, n: usize) {
        if let Some(count) = self.counts.get(id.as_ref()) {
            count.fetch_add(n, AcqRel);
            self.total_count.fetch_add(n, AcqRel);
        }
    }

    pub fn ended<S: AsRef<str>>(&self, id: S) {
        self.ended_many(id, 1);
    }

    pub fn ended_many<S: AsRef<str>>(&self, id: S, n: usize) {
        if let Some(count) = self.counts.get(id.as_ref()) {
            count.fetch_sub(n, AcqRel);
            self.total_count.fetch_sub(n, AcqRel);
        }
    }

    pub fn completed(&self) -> bool {
        self.total_count.load(Acquire) == 0
    }

    #[cfg(test)]
    pub fn get<S: AsRef<str>>(&self, id: S) -> usize {
        self.counts[id.as_ref()].load(Acquire)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering::Acquire;

    use tokio::join;

    use super::*;

    #[test]
    fn test_add_creates_entry() {
        let mut synch = Synchronizer::default();
        let id = "my-id";

        synch.register(id);

        assert_eq!(synch.counts.len(), 1);
        assert!(synch.counts.contains_key(id));
        assert_eq!(synch.counts[id].load(Acquire), 0);
        assert_eq!(synch.total_count.load(Acquire), 0);
    }

    #[test]
    fn test_add_same_id_does_nothing() {
        let mut synch = Synchronizer::default();
        let id = "my-id";

        synch.register(id);
        synch.register(id);

        assert_eq!(synch.counts.len(), 1);
        assert!(synch.counts.contains_key(id));
        assert_eq!(synch.counts[id].load(Acquire), 0);
        assert_eq!(synch.total_count.load(Acquire), 0);
    }

    #[test]
    fn test_started_many_works() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        synch.started_many("id0", 3);
        synch.started_many("id1", 1);
        synch.started_many("id2", 7);

        assert_eq!(synch.counts["id0"].load(Acquire), 3);
        assert_eq!(synch.counts["id1"].load(Acquire), 1);
        assert_eq!(synch.counts["id2"].load(Acquire), 7);
        assert_eq!(synch.total_count.load(Acquire), 11);
    }

    #[test]
    fn test_started_works() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        synch.started("id0");
        synch.started("id1");
        synch.started("id2");

        assert_eq!(synch.counts["id0"].load(Acquire), 1);
        assert_eq!(synch.counts["id1"].load(Acquire), 1);
        assert_eq!(synch.counts["id2"].load(Acquire), 1);
        assert_eq!(synch.total_count.load(Acquire), 3);
    }

    #[test]
    fn test_ended_many_works() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        synch.started_many("id0", 10);
        synch.started_many("id1", 10);
        synch.started_many("id2", 10);

        synch.ended_many("id0", 3);
        synch.ended_many("id1", 1);
        synch.ended_many("id2", 7);

        assert_eq!(synch.counts["id0"].load(Acquire), 7);
        assert_eq!(synch.counts["id1"].load(Acquire), 9);
        assert_eq!(synch.counts["id2"].load(Acquire), 3);
        assert_eq!(synch.total_count.load(Acquire), 19);
    }

    #[test]
    fn test_ended_works() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        synch.started_many("id0", 10);
        synch.started_many("id1", 10);
        synch.started_many("id2", 10);

        synch.ended("id0");
        synch.ended("id1");
        synch.ended("id2");

        assert_eq!(synch.counts["id0"].load(Acquire), 9);
        assert_eq!(synch.counts["id1"].load(Acquire), 9);
        assert_eq!(synch.counts["id2"].load(Acquire), 9);
        assert_eq!(synch.total_count.load(Acquire), 27);
    }

    #[test]
    fn test_completed_works() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        assert!(synch.completed());

        for id in ids {
            synch.started(id);
            assert!(!synch.completed());
            synch.ended(id);
        }
    }

    #[tokio::test]
    async fn test_completed_works_under_load() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        join!(
            async {
                for _ in [0; 100] {
                    synch.started("id0");
                }
                for _ in [0; 100] {
                    synch.started("id1");
                }
                for _ in [0; 100] {
                    synch.started("id2");
                }
            },
            async {
                for _ in [0; 100] {
                    synch.ended("id0");
                }
                for _ in [0; 100] {
                    synch.ended("id1");
                }
                for _ in [0; 100] {
                    synch.ended("id2");
                }
            },
        );

        assert!(synch.completed());
    }
}

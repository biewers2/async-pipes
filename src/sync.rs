use std::collections::HashMap;

use tokio::sync::Mutex;

/// A generic data structure to synchronize tasks being done among multiple threads.
///
/// The IDs of the tasks must be registered in order to be tracked. Tasks can be marked as "started" or
/// "ended" which increments and decrements (respectively) the respective task ID counter. A synchronizer
/// is considered "complete" if all task ID counters are zero.
#[derive(Default, Debug)]
pub(crate) struct Synchronizer {
    /// Maintain a total count to easily check if all task ID counters are zero
    total_count: Mutex<usize>,
    counts: HashMap<String, Mutex<usize>>,
}

impl Synchronizer {
    pub(crate) fn register<S: Into<String>>(&mut self, id: S) {
        self.counts.insert(id.into(), Mutex::new(0));
    }

    pub(crate) async fn started<S: AsRef<str>>(&self, id: S) {
        self.started_many(id, 1).await;
    }

    pub(crate) async fn started_many<S: AsRef<str>>(&self, id: S, n: usize) {
        if let Some(count) = self.counts.get(id.as_ref()) {
            *count.lock().await += n;
            *self.total_count.lock().await += n;
        }
    }

    pub(crate) async fn ended<S: AsRef<str>>(&self, id: S) {
        self.ended_many(id, 1).await;
    }

    pub(crate) async fn ended_many<S: AsRef<str>>(&self, id: S, n: usize) {
        if let Some(count) = self.counts.get(id.as_ref()) {
            *count.lock().await -= n;
            *self.total_count.lock().await -= n;
        }
    }

    pub(crate) async fn completed(&self) -> bool {
        *self.total_count.lock().await == 0
    }

    #[cfg(test)]
    pub async fn get<S: AsRef<str>>(&self, id: S) -> usize {
        *self.counts[id.as_ref()].lock().await
    }
}

#[cfg(test)]
mod tests {
    use tokio::join;

    use crate::sync::Synchronizer;

    #[tokio::test]
    async fn test_add_creates_entry() {
        let mut synch = Synchronizer::default();
        let id = "my-id";

        synch.register(id);

        assert_eq!(synch.counts.len(), 1);
        assert!(synch.counts.contains_key(id));
        assert_eq!(*synch.counts[id].lock().await, 0);
        assert_eq!(*synch.total_count.lock().await, 0);
    }

    #[tokio::test]
    async fn test_add_same_id_does_nothing() {
        let mut synch = Synchronizer::default();
        let id = "my-id";

        synch.register(id);
        synch.register(id);

        assert_eq!(synch.counts.len(), 1);
        assert!(synch.counts.contains_key(id));
        assert_eq!(*synch.counts[id].lock().await, 0);
        assert_eq!(*synch.total_count.lock().await, 0);
    }

    #[tokio::test]
    async fn test_started_many_works() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        synch.started_many("id0", 3).await;
        synch.started_many("id1", 1).await;
        synch.started_many("id2", 7).await;

        assert_eq!(*synch.counts["id0"].lock().await, 3);
        assert_eq!(*synch.counts["id1"].lock().await, 1);
        assert_eq!(*synch.counts["id2"].lock().await, 7);
        assert_eq!(*synch.total_count.lock().await, 11);
    }

    #[tokio::test]
    async fn test_started_works() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        synch.started("id0").await;
        synch.started("id1").await;
        synch.started("id2").await;

        assert_eq!(*synch.counts["id0"].lock().await, 1);
        assert_eq!(*synch.counts["id1"].lock().await, 1);
        assert_eq!(*synch.counts["id2"].lock().await, 1);
        assert_eq!(*synch.total_count.lock().await, 3);
    }

    #[tokio::test]
    async fn test_ended_many_works() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        synch.started_many("id0", 10).await;
        synch.started_many("id1", 10).await;
        synch.started_many("id2", 10).await;

        synch.ended_many("id0", 3).await;
        synch.ended_many("id1", 1).await;
        synch.ended_many("id2", 7).await;

        assert_eq!(*synch.counts["id0"].lock().await, 7);
        assert_eq!(*synch.counts["id1"].lock().await, 9);
        assert_eq!(*synch.counts["id2"].lock().await, 3);
        assert_eq!(*synch.total_count.lock().await, 19);
    }

    #[tokio::test]
    async fn test_ended_works() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        synch.started_many("id0", 10).await;
        synch.started_many("id1", 10).await;
        synch.started_many("id2", 10).await;

        synch.ended("id0").await;
        synch.ended("id1").await;
        synch.ended("id2").await;

        assert_eq!(*synch.counts["id0"].lock().await, 9);
        assert_eq!(*synch.counts["id1"].lock().await, 9);
        assert_eq!(*synch.counts["id2"].lock().await, 9);
        assert_eq!(*synch.total_count.lock().await, 27);
    }

    #[tokio::test]
    async fn test_completed_works() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        assert!(synch.completed().await);

        for id in ids {
            synch.started(id).await;
            assert!(!synch.completed().await);
            synch.ended(id).await;
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
                    synch.started("id0").await;
                }
                for _ in [0; 100] {
                    synch.started("id1").await;
                }
                for _ in [0; 100] {
                    synch.started("id2").await;
                }
            },
            async {
                for _ in [0; 100] {
                    synch.ended("id0").await;
                }
                for _ in [0; 100] {
                    synch.ended("id1").await;
                }
                for _ in [0; 100] {
                    synch.ended("id2").await;
                }
            },
        );

        assert!(synch.completed().await);
    }
}

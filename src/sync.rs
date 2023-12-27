use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default, Debug)]
pub(crate) struct Synchronizer {
    total_count: Mutex<usize>,
    counts: HashMap<String, Mutex<usize>>,
}

impl Synchronizer {
    pub(crate) fn register<S: Into<String>>(&mut self, id: S) {
        self.counts.insert(id.into(), Mutex::new(0));
    }

    pub(crate) fn started<S: AsRef<str>>(&self, id: S) {
        self.started_many(id, 1);
    }

    pub(crate) fn started_many<S: AsRef<str>>(&self, id: S, n: usize) {
        if let Some(count) = self.counts.get(id.as_ref()) {
            let mut count = count.lock().expect("failed to acquire counts lock");
            let mut net_count = self
                .total_count
                .lock()
                .expect("failed to acquire total count lock");

            *count += n;
            *net_count += n;
        }
    }

    pub(crate) fn ended<S: AsRef<str>>(&self, id: S) {
        self.ended_many(id, 1);
    }

    pub(crate) fn ended_many<S: AsRef<str>>(&self, id: S, n: usize) {
        if let Some(count) = self.counts.get(id.as_ref()) {
            let mut count = count.lock().expect("failed to acquire counts lock");
            let mut net_count = self
                .total_count
                .lock()
                .expect("failed to acquire total count lock");

            *count -= n;
            *net_count -= n;
        }
    }

    pub(crate) fn completed(&self) -> bool {
        let net_count = self
            .total_count
            .lock()
            .expect("failed to acquire counts lock");

        *net_count == 0
    }

    #[cfg(test)]
    pub fn get<S: AsRef<str>>(&self, id: S) -> usize {
        *self.counts[id.as_ref()].lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::sync::Synchronizer;

    #[test]
    fn test_add_creates_entry() {
        let mut synch = Synchronizer::default();
        let id = "my-id";

        synch.register(id);

        assert_eq!(synch.counts.len(), 1);
        assert!(synch.counts.contains_key(id));
        assert_eq!(*synch.counts[id].lock().unwrap(), 0);
        assert_eq!(*synch.total_count.lock().unwrap(), 0);
    }

    #[test]
    fn test_add_same_id_does_nothing() {
        let mut synch = Synchronizer::default();
        let id = "my-id";

        synch.register(id);
        synch.register(id);

        assert_eq!(synch.counts.len(), 1);
        assert!(synch.counts.contains_key(id));
        assert_eq!(*synch.counts[id].lock().unwrap(), 0);
        assert_eq!(*synch.total_count.lock().unwrap(), 0);
    }

    #[test]
    fn test_started_many_works() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        synch.started_many("id0", 3);
        synch.started_many("id1", 1);
        synch.started_many("id2", 7);

        assert_eq!(*synch.counts["id0"].lock().unwrap(), 3);
        assert_eq!(*synch.counts["id1"].lock().unwrap(), 1);
        assert_eq!(*synch.counts["id2"].lock().unwrap(), 7);
        assert_eq!(*synch.total_count.lock().unwrap(), 11);
    }

    #[test]
    fn test_started_works() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        synch.started("id0");
        synch.started("id1");
        synch.started("id2");

        assert_eq!(*synch.counts["id0"].lock().unwrap(), 1);
        assert_eq!(*synch.counts["id1"].lock().unwrap(), 1);
        assert_eq!(*synch.counts["id2"].lock().unwrap(), 1);
        assert_eq!(*synch.total_count.lock().unwrap(), 3);
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

        assert_eq!(*synch.counts["id0"].lock().unwrap(), 7);
        assert_eq!(*synch.counts["id1"].lock().unwrap(), 9);
        assert_eq!(*synch.counts["id2"].lock().unwrap(), 3);
        assert_eq!(*synch.total_count.lock().unwrap(), 19);
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

        assert_eq!(*synch.counts["id0"].lock().unwrap(), 9);
        assert_eq!(*synch.counts["id1"].lock().unwrap(), 9);
        assert_eq!(*synch.counts["id2"].lock().unwrap(), 9);
        assert_eq!(*synch.total_count.lock().unwrap(), 27);
    }

    #[test]
    fn test_is_done_works() {
        let mut synch = Synchronizer::default();
        let ids = ["id0", "id1", "id2"];
        ids.iter().for_each(|id| synch.register(*id));

        assert!(synch.completed());

        ids.iter().for_each(|id| {
            synch.started(id);
            assert!(!synch.completed());
            synch.ended(id);
        })
    }
}

//! Keeps sealed redb handles cached and moves expensive close work off the writer thread.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        mpsc::{self, SyncSender},
    },
    thread::{self, JoinHandle},
};

use redb::Database;

use super::{StorageError, mutex};

const REAPER_QUEUE_CAPACITY: usize = 64;

pub(super) type SegmentDatabase = Arc<DeferredDatabase>;

/// Defers redb's close-time allocator commit and flush to the reaper thread.
pub(super) struct DeferredDatabase {
    database: Option<Database>,
    path: PathBuf,
    reaper: SyncSender<RetiredDatabase>,
    pending_closes: Arc<PendingCloses>,
}

pub(super) struct SegmentCache {
    capacity: usize,
    order: VecDeque<u64>,
    databases: HashMap<u64, SegmentDatabase>,
}

pub(super) struct DatabaseReaper {
    sender: Option<SyncSender<RetiredDatabase>>,
    pending_closes: Arc<PendingCloses>,
    thread: Option<JoinHandle<()>>,
}

struct RetiredDatabase {
    path: PathBuf,
    database: Database,
}

#[derive(Default)]
struct PendingCloses {
    paths: Mutex<HashSet<PathBuf>>,
    changed: Condvar,
}

impl SegmentCache {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::new(),
            databases: HashMap::new(),
        }
    }

    pub(super) fn get(&mut self, id: u64) -> Option<SegmentDatabase> {
        let database = self.databases.get(&id).cloned()?;
        self.order.retain(|candidate| *candidate != id);
        self.order.push_back(id);
        Some(database)
    }

    pub(super) fn insert(&mut self, id: u64, database: SegmentDatabase) {
        self.remove(id);
        self.order.push_back(id);
        self.databases.insert(id, database);
        while self.databases.len() > self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.databases.remove(&expired);
            }
        }
    }

    pub(super) fn remove(&mut self, id: u64) {
        self.order.retain(|candidate| *candidate != id);
        self.databases.remove(&id);
    }
}

impl Deref for DeferredDatabase {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        self.database
            .as_ref()
            .expect("segment database remains available until its final handle drops")
    }
}

impl Drop for DeferredDatabase {
    fn drop(&mut self) {
        let Some(database) = self.database.take() else {
            return;
        };
        mutex(&self.pending_closes.paths).insert(self.path.clone());
        if let Err(error) = self.reaper.try_send(RetiredDatabase {
            path: self.path.clone(),
            database,
        }) {
            let retired = match error {
                mpsc::TrySendError::Full(retired) | mpsc::TrySendError::Disconnected(retired) => {
                    retired
                }
            };
            drop(retired.database);
            self.pending_closes.finish(&self.path);
        }
    }
}

impl DatabaseReaper {
    pub(super) fn start() -> Result<Self, StorageError> {
        let (sender, receiver) = mpsc::sync_channel::<RetiredDatabase>(REAPER_QUEUE_CAPACITY);
        let pending_closes = Arc::new(PendingCloses::default());
        let thread_pending_closes = Arc::clone(&pending_closes);
        let thread = thread::Builder::new()
            .name("lightcdc-segment-reaper".to_owned())
            .spawn(move || {
                while let Ok(retired) = receiver.recv() {
                    drop(retired.database);
                    thread_pending_closes.finish(&retired.path);
                }
            })?;
        Ok(Self {
            sender: Some(sender),
            pending_closes,
            thread: Some(thread),
        })
    }

    pub(super) fn wrap(&self, database: Database, path: PathBuf) -> SegmentDatabase {
        Arc::new(DeferredDatabase {
            database: Some(database),
            path,
            reaper: self
                .sender
                .as_ref()
                .expect("segment reaper remains available while the store is open")
                .clone(),
            pending_closes: Arc::clone(&self.pending_closes),
        })
    }

    pub(super) fn wait_until_closed(&self, path: &Path) {
        let mut pending = mutex(&self.pending_closes.paths);
        while pending.contains(path) {
            pending = self
                .pending_closes
                .changed
                .wait(pending)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

impl Drop for DatabaseReaper {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl PendingCloses {
    fn finish(&self, path: &Path) {
        mutex(&self.paths).remove(path);
        self.changed.notify_all();
    }
}

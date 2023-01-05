use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use cognite::files::FileMetadata;
use fuser::{FileAttr, FileType};
use tokio::sync::RwLock;

use crate::types::{CachedDirectory, BLOCK_SIZE};

pub struct CacheFileAccess {
    cache_path: PathBuf,
    local_mod: bool,
    known_size: Option<u64>,
    loaded_at: Option<Instant>,
}

impl CacheFileAccess {
    pub fn new(inode: u64, cache_path: &str) -> Self {
        CacheFileAccess {
            cache_path: PathBuf::from(format!("{}/{}", cache_path, inode)),
            local_mod: false,
            known_size: None,
            loaded_at: None,
        }
    }
}

pub struct SyncDirectory {
    pub path: String,
    pub parent: Option<u64>,
    pub children: Vec<u64>,
    pub loaded_at: Option<Instant>,
    pub inode: u64,
    pub name: String,
    pub is_new: bool,
}

impl SyncDirectory {
    pub fn should_reload(&self) -> bool {
        match self.loaded_at {
            Some(i) => i.elapsed().as_millis() > 600_000,
            None => true,
        }
    }

    pub fn get_file_attr(&self) -> FileAttr {
        FileAttr {
            ino: self.inode,
            size: 0,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o666,
            nlink: self.children.len() as u32,
            uid: 501,
            gid: 20,
            rdev: 0,
            blksize: 0,
            flags: 512,
        }
    }

    pub fn is_below_path(&self, path: &str) -> bool {
        self.path.starts_with(path)
    }
}

pub struct SyncFile {
    pub cache_file: Arc<RwLock<CacheFileAccess>>,
    pub meta: FileMetadata,
    pub inode: u64,
    pub is_new: bool,
}

impl SyncFile {
    pub fn get_file_attr(&self) -> FileAttr {
        let data = self.cache_file.blocking_read();
        self.get_file_attr_int(&data)
    }

    fn get_file_attr_int(&self, f: &CacheFileAccess) -> FileAttr {
        let size = f.known_size.unwrap_or(0);
        FileAttr {
            ino: self.inode,
            size, // not usually known at this stage
            blocks: (size + BLOCK_SIZE - 1) / BLOCK_SIZE,
            atime: SystemTime::now(),
            mtime: SystemTime::UNIX_EPOCH
                + Duration::from_millis(self.meta.last_updated_time as u64),
            ctime: SystemTime::UNIX_EPOCH
                + Duration::from_millis(self.meta.last_updated_time as u64),
            crtime: SystemTime::UNIX_EPOCH + Duration::from_millis(self.meta.created_time as u64),
            kind: FileType::RegularFile,
            perm: 0o666,
            nlink: 1,
            uid: 501,
            gid: 20,
            rdev: 0,
            blksize: BLOCK_SIZE as u32,
            flags: 512,
        }
    }

    pub fn is_below_path(&self, path: &str) -> bool {
        let sp = self
            .meta
            .directory
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("/");
        sp.starts_with(path)
    }
}

pub enum Node {
    Dir(SyncDirectory),
    File(SyncFile),
}

impl Node {
    pub fn get_file_attr(&self) -> FileAttr {
        match self {
            Self::Dir(d) => d.get_file_attr(),
            Self::File(d) => d.get_file_attr(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Dir(d) => &d.name,
            Self::File(f) => &f.meta.name,
        }
    }

    pub fn is_new(&self) -> bool {
        match self {
            Self::Dir(d) => d.is_new,
            Self::File(f) => f.is_new,
        }
    }

    pub fn directory(&self) -> Option<&SyncDirectory> {
        match self {
            Self::Dir(d) => Some(d),
            _ => None,
        }
    }

    pub fn directory_mut(&mut self) -> Option<&mut SyncDirectory> {
        match self {
            Self::Dir(d) => Some(d),
            _ => None,
        }
    }

    pub fn file(&self) -> Option<&SyncFile> {
        match self {
            Self::File(f) => Some(f),
            _ => None,
        }
    }

    pub fn file_mut(&mut self) -> Option<&mut SyncFile> {
        match self {
            Self::File(f) => Some(f),
            _ => None,
        }
    }
}
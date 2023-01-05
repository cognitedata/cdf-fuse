use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};

use cognite::files::FileMetadata;
use fuser::{FileAttr, FileType};
use tokio::fs::{File, OpenOptions};

pub struct CachedDirectory {
    pub path: Option<String>,
    pub parent: Option<String>,
    pub children: Vec<Inode>,
    pub loaded_at: Option<Instant>,
    pub inode: u64,
    pub name: String,
}

pub struct CachedFile {
    pub meta: FileMetadata,
    pub loaded_at: Option<Instant>,
    pub is_new: bool,
    pub read_at: Option<Instant>,
    pub inode: u64,
    pub known_size: Option<u64>,
    pub local_mod: bool,
}

pub const BLOCK_SIZE: u64 = 512;

impl CachedFile {
    pub fn get_file_attr(&self) -> FileAttr {
        let size = self.known_size.unwrap_or(0);
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

    pub fn get_cache_file_path(&self, cache_dir: &str) -> PathBuf {
        Path::new(cache_dir).join(self.meta.id.to_string())
    }

    pub async fn get_cache_file(
        &self,
        cache_dir: &str,
        write: bool,
        read: bool,
        wipe: bool,
    ) -> Result<File, std::io::Error> {
        let path = self.get_cache_file_path(cache_dir);
        if !write {
            if !path.exists() {
                File::create(path).await
            } else {
                File::open(path).await
            }
        } else {
            OpenOptions::new()
                .write(true)
                .truncate(wipe)
                .read(read)
                .create(true)
                .open(path)
                .await
        }
    }
}

impl CachedDirectory {
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
}

pub enum NodeRef<'a> {
    Dir(&'a CachedDirectory),
    File(&'a CachedFile),
}

impl<'a> NodeRef<'a> {
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
}

#[derive(Debug, Clone)]
pub enum Inode {
    File { ino: u64, id: i64 },
    Directory { ino: u64, dir: String },
}

impl Inode {
    pub fn is_file(&self) -> bool {
        matches!(self, Self::File { .. })
    }

    pub fn new_directory(ino: u64, node: String) -> Self {
        Self::Directory { ino, dir: node }
    }

    pub fn new_file(ino: u64, node: i64) -> Self {
        Self::File { ino, id: node }
    }

    pub fn ino(&self) -> u64 {
        match self {
            Self::Directory { ino, .. } => *ino,
            Self::File { ino, .. } => *ino,
        }
    }

    pub fn directory(&self) -> Option<&String> {
        match self {
            Self::Directory { dir: x, .. } => Some(x),
            _ => None,
        }
    }

    pub fn file(&self) -> Option<i64> {
        match self {
            Self::File { id: x, .. } => Some(*x),
            _ => None,
        }
    }
}

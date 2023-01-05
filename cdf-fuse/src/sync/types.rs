use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use cognite::files::FileMetadata;
use fuser::{FileAttr, FileType};
use tokio::{
    fs::{File, OpenOptions},
    sync::RwLock,
};

use crate::{
    err::FsError,
    types::{CachedDirectory, BLOCK_SIZE},
};

pub struct CacheFileAccess {
    pub cache_path: PathBuf,
    pub local_mod: bool,
    pub known_size: Option<u64>,
    pub loaded_at: Option<Instant>,
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

    pub fn exists(&self) -> bool {
        Path::exists(&self.cache_path)
    }

    pub async fn get_handle_write(
        &mut self,
        wipe: bool,
        read: bool,
    ) -> Result<File, std::io::Error> {
        OpenOptions::new()
            .write(true)
            .truncate(wipe)
            .read(read)
            .create(true)
            .open(&self.cache_path)
            .await
    }

    pub async fn get_handle_read(&self) -> Result<File, std::io::Error> {
        File::open(&self.cache_path).await
    }

    pub async fn size(&self) -> Result<u64, std::io::Error> {
        Ok(tokio::fs::metadata(&self.cache_path).await?.len())
    }

    pub async fn set_size(&mut self, size: u64) -> Result<(), std::io::Error> {
        self.known_size = Some(size);
        let fh = self.get_handle_write(false, false).await?;
        fh.set_len(size).await?;
        Ok(())
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

    pub fn get_node_info(&self) -> NodeInfo {
        NodeInfo {
            name: self.name.clone(),
            ino: self.inode,
            typ: NodeType::File,
            parent: self.parent,
        }
    }
}

pub struct SyncFile {
    pub cache_file: Arc<RwLock<CacheFileAccess>>,
    pub meta: FileMetadata,
    pub inode: u64,
    pub is_new: bool,
    pub parent: u64,
}

impl SyncFile {
    pub async fn get_file_attr(&self) -> FileAttr {
        let data = self.cache_file.read().await;
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

    pub fn get_node_info(&self) -> NodeInfo {
        NodeInfo {
            name: self.meta.name.clone(),
            ino: self.inode,
            typ: NodeType::File,
            parent: Some(self.parent),
        }
    }
}

pub enum NodeType {
    Dir,
    File,
}

pub struct NodeInfo {
    pub name: String,
    pub ino: u64,
    pub typ: NodeType,
    pub parent: Option<u64>,
}

pub enum Node {
    Dir(SyncDirectory),
    File(SyncFile),
}

impl Node {
    pub async fn get_file_attr(&self) -> FileAttr {
        match self {
            Self::Dir(d) => d.get_file_attr(),
            Self::File(d) => d.get_file_attr().await,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Dir(d) => &d.name,
            Self::File(f) => &f.meta.name,
        }
    }

    pub fn get_node_info(&self) -> NodeInfo {
        match self {
            Self::Dir(d) => NodeInfo {
                name: d.name.clone(),
                ino: d.inode,
                typ: NodeType::Dir,
                parent: d.parent,
            },
            Self::File(f) => f.get_node_info(),
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

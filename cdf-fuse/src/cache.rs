use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};

use bytes::Bytes;
use cognite::{
    files::{FileFilter, FileMetadata},
    CogniteClient, FilterWithRequest, Identity, PartitionedFilter,
};
use fuser::{FileAttr, FileType, FUSE_ROOT_ID};
use futures_util::{SinkExt, TryStreamExt};
use log::{debug, info, trace};
use tokio::{
    fs::{remove_file, File, OpenOptions},
    io::{BufWriter, Sink},
};
use tokio_util::codec::{BytesCodec, FramedWrite};

use crate::{err::FsError, fs::CdfFS};

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
}

const BLOCK_SIZE: u64 = 512;

impl CachedFile {
    pub fn get_file_attr(&self) -> FileAttr {
        let size = self.known_size.unwrap_or(0);
        FileAttr {
            ino: self.inode,
            size: size, // not usually known at this stage
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
        append: bool,
        read: bool,
        wipe: bool,
    ) -> Result<File, std::io::Error> {
        let path = self.get_cache_file_path(cache_dir);
        OpenOptions::new()
            .write(write)
            .create_new(wipe)
            .append(true)
            .read(read)
            .create(true)
            .open(path)
            .await
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

pub enum Inode {
    File(i64),
    Directory(String),
}

impl Inode {
    pub fn is_file(&self) -> bool {
        matches!(self, Self::File(_))
    }

    pub fn directory(&self) -> Option<&String> {
        match self {
            Self::Directory(x) => Some(x),
            _ => None,
        }
    }

    pub fn file(&self) -> Option<i64> {
        match self {
            Self::File(x) => Some(*x),
            _ => None,
        }
    }
}

pub struct Cache {
    pub directories: HashMap<String, CachedDirectory>,
    pub files: HashMap<i64, CachedFile>,
    pub inode_map: HashMap<u64, Inode>,
    pub cache_dir: String,
}

impl Cache {
    pub fn new(cache_dir: String) -> Self {
        Cache {
            directories: HashMap::new(),
            files: HashMap::new(),
            inode_map: HashMap::new(),
            cache_dir,
        }
    }

    pub fn init(&mut self, loaded: bool) {
        if !self.directories.contains_key("") {
            self.directories.insert(
                "".to_string(),
                CachedDirectory {
                    path: None,
                    parent: None,
                    children: vec![],
                    loaded_at: if loaded { Some(Instant::now()) } else { None },
                    inode: FUSE_ROOT_ID,
                    name: "".to_string(),
                },
            );
            self.inode_map
                .insert(FUSE_ROOT_ID, Inode::Directory("".to_string()));
        }
    }

    pub fn get_dir(&self, node: u64) -> Option<&CachedDirectory> {
        self.inode_map
            .get(&node)
            .and_then(|n| self.get_dir_inode(n))
    }

    pub fn get_dir_inode(&self, node: &Inode) -> Option<&CachedDirectory> {
        node.directory().and_then(|d| self.directories.get(d))
    }

    pub fn get_file(&self, node: u64) -> Option<&CachedFile> {
        self.inode_map
            .get(&node)
            .and_then(|n| self.get_file_inode(n))
    }

    pub fn get_file_inode(&self, node: &Inode) -> Option<&CachedFile> {
        node.file().and_then(|f| self.files.get(&f))
    }

    pub fn get_node<'a>(&'a self, node: &Inode) -> Option<NodeRef<'a>> {
        self.get_file_inode(node)
            .map(NodeRef::File)
            .or_else(|| self.get_dir_inode(node).map(NodeRef::Dir))
    }

    pub async fn reload_directory(
        &mut self,
        client: &CogniteClient,
        raw_dir: &str,
    ) -> Result<(), FsError> {
        let dir = self.directories.get(raw_dir);
        let root = if raw_dir != "" {
            Some(raw_dir.to_string())
        } else {
            None
        };
        let files = Self::load_cached_directory(
            client,
            dir.map(|d| {
                if d.loaded_at.is_some() {
                    d.children.iter().filter(|c| c.is_file()).count()
                } else {
                    100_000
                }
            }),
            root.clone(),
        )
        .await?;
        self.build_directories_from_files(&files, root);
        for file in files {
            if !self.files.contains_key(&file.meta.id) {
                self.files.insert(file.meta.id, file);
            }
        }
        Ok(())
    }

    pub async fn open_file(
        &mut self,
        client: &CogniteClient,
        file: u64,
        write: bool,
        read: bool,
        append: bool,
    ) -> Result<File, FsError> {
        let file = self
            .inode_map
            .get(&file)
            .and_then(|n| n.file())
            .and_then(|f| self.files.get_mut(&f))
            .ok_or_else(|| FsError::FileNotFound)?;

        let should_read = !file.is_new
            && (read || append)
            && match file.read_at {
                Some(x) => x.elapsed().as_millis() > 600_000,
                None => true,
            };
        // Always need to open file in write mode...
        let handle = file
            .get_cache_file(&self.cache_dir, should_read, append, read, should_read)
            .await?;

        if should_read {
            info!(
                "Downloading file with id {} and name {} from CDF",
                file.meta.id, file.meta.name
            );
            let mut stream = client
                .files
                .download_file(Identity::Id { id: file.meta.id })
                .await?
                .map_err(|e| FsError::from(cognite::Error::from(e)));
            let fwrite = FramedWrite::new(handle, BytesCodec::new());
            <FramedWrite<tokio::fs::File, BytesCodec> as SinkExt<Bytes>>::sink_map_err(
                fwrite,
                FsError::from,
            )
            .send_all(&mut stream)
            .await?;
            file.read_at = Some(Instant::now());
            let handle = file
                .get_cache_file(&self.cache_dir, write, append, read, false)
                .await?;
            file.known_size = Some(handle.metadata().await?.len());
            Ok(handle)
        } else {
            Ok(handle)
        }
    }

    pub async fn close_file(&mut self, file: u64) -> Result<(), FsError> {
        let file = self
            .inode_map
            .get(&file)
            .and_then(|n| n.file())
            .and_then(|f| self.files.get_mut(&f))
            .ok_or_else(|| FsError::FileNotFound)?;
        let path = file.get_cache_file_path(&self.cache_dir);
        if path.exists() {
            remove_file(path).await?;
        }
        file.read_at = None;
        Ok(())
    }

    pub async fn open_directory<'a>(
        &'a mut self,
        client: &CogniteClient,
        raw_dir: &str,
    ) -> Result<(Vec<&'a CachedFile>, Vec<&'a CachedDirectory>), FsError> {
        let mut dir = self.directories.get(raw_dir);
        let should_reload = match dir {
            Some(x) => match x.loaded_at {
                Some(i) => i.elapsed().as_millis() > 600_000,
                None => true,
            },
            None => true,
        };

        if should_reload {
            self.reload_directory(client, raw_dir).await?;
            dir = self.directories.get(raw_dir);
        }

        let mut final_files = vec![];
        let mut final_dirs = vec![];

        let dir = match dir {
            Some(x) => x,
            None => return Err(FsError::DirectoryNotFound),
        };
        for child in &dir.children {
            match child {
                Inode::File(f) => final_files.push(self.files.get(f).unwrap()),
                Inode::Directory(d) => final_dirs.push(self.directories.get(d).unwrap()),
            }
        }

        Ok((final_files, final_dirs))
    }

    async fn load_cached_directory(
        client: &CogniteClient,
        expected_size: Option<usize>,
        path: Option<String>,
    ) -> Result<Vec<CachedFile>, FsError> {
        let num_parallel = match expected_size {
            Some(x) => (x / 1000).clamp(1, 10),
            None => 5,
        };

        info!(
            "Loading files from CDF, doing {} parallel queries",
            num_parallel
        );
        let res = client
            .files
            .filter_all_partitioned(
                PartitionedFilter::new(
                    FileFilter {
                        directory_prefix: path.clone().map(|p| format!("/{}", p)),
                        ..Default::default()
                    },
                    None,
                    Some(1000),
                    None,
                ),
                num_parallel as u32,
            )
            .await?;
        info!("Found a total of {} files in CDF", res.len());

        Ok(res
            .into_iter()
            .map(|f| CachedFile {
                inode: f.id as u64,
                meta: f,
                loaded_at: None,
                is_new: false,
                read_at: None,
                known_size: None,
            })
            .collect())
    }

    fn get_max_inode(&self) -> u64 {
        self.inode_map
            .iter()
            .map(|(i, _)| *i)
            .filter(|i| i > &(1 << 63))
            .max()
            .unwrap_or(1 << 63)
    }

    fn build_directories_from_files(&mut self, files: &[CachedFile], root: Option<String>) {
        // We refresh the directory tree based on the returned data
        // Anything at or below "parent" overwrites the existing data, and needs to be removed here.
        match &root {
            Some(x) => {
                let mut to_remove = vec![];
                for dir in self.directories.keys() {
                    if dir.starts_with(x) {
                        to_remove.push(dir.clone());
                    }
                }
                for dir in to_remove {
                    let old = self.directories.remove(&dir);
                    if let Some(old) = old {
                        self.inode_map.remove(&old.inode);
                    }
                }
            }
            None => {
                self.directories.clear();
                self.inode_map.clear();
            }
        }

        let mut inode = self.get_max_inode();

        // The root directory needs to exist
        self.init(root.is_none());

        let mut visited = HashSet::new();
        for file in files {
            let mut current_path = "".to_string();
            let mut current_parent = self.directories.get_mut("").unwrap();
            if let Some(dir) = &file.meta.directory {
                let path = Path::new(dir.trim_start_matches("/"));

                for comp in path.components() {
                    let parent = current_path.clone();
                    if current_path != "" {
                        current_path.push('/');
                    }
                    current_path.push_str(comp.as_os_str().to_str().unwrap());

                    if visited.insert(current_path.clone()) {
                        // If we haven't visited this directory yet, try to add to it.
                        let loaded_at = if let Some(p) = &root {
                            if current_path.starts_with(p) {
                                Some(Instant::now())
                            } else {
                                None
                            }
                        } else {
                            Some(Instant::now())
                        };

                        current_parent
                            .children
                            .push(Inode::Directory(current_path.clone()));

                        if !self.directories.contains_key(&current_path) {
                            trace!("Inserting directory with path {}", current_path);
                            self.directories.insert(
                                current_path.clone(),
                                CachedDirectory {
                                    path: Some(current_path.clone()),
                                    parent: Some(parent),
                                    children: vec![],
                                    loaded_at,
                                    inode: inode + 1,
                                    name: CdfFS::get_dir_name(&current_path).to_string(),
                                },
                            );
                            inode += 1;
                            self.inode_map
                                .insert(inode, Inode::Directory(current_path.clone()));
                        } else {
                            let dir = self.directories.get_mut(&current_path).unwrap();
                            dir.loaded_at = loaded_at;
                        }
                        current_parent = self.directories.get_mut(&current_path).unwrap();
                    }
                }
            }
            trace!(
                "Loaded file with name {} and id {}",
                file.meta.name,
                file.meta.id
            );
            current_parent.children.push(Inode::File(file.meta.id));
            self.inode_map
                .insert(file.meta.id as u64, Inode::File(file.meta.id));
        }
    }
}

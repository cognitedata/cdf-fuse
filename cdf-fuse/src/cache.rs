use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use cognite::{
    files::{AddFile, FileFilter, FileMetadata},
    CogniteClient, Delete, FilterWithRequest, Identity, PartitionedFilter,
};
use fuser::{FileAttr, FileType, FUSE_ROOT_ID};
use futures_util::{SinkExt, TryStreamExt};
use log::{debug, info, warn};
use tokio::fs::{remove_file, File, OpenOptions};
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

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
    pub local_mod: bool,
}

const BLOCK_SIZE: u64 = 512;

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

    pub fn ino(&self) -> u64 {
        match self {
            Self::Dir(d) => d.inode,
            Self::File(f) => f.inode,
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
            self.inode_map.insert(
                FUSE_ROOT_ID,
                Inode::new_directory(FUSE_ROOT_ID, "".to_string()),
            );
        }
    }

    pub fn get_dir(&self, node: u64) -> Option<&CachedDirectory> {
        self.inode_map
            .get(&node)
            .and_then(|n| self.get_dir_inode(n))
    }

    pub fn get_dir_mut(&mut self, node: u64) -> Option<&mut CachedDirectory> {
        self.inode_map
            .get(&node)
            .and_then(|n| n.directory().and_then(|d| self.directories.get_mut(d)))
    }

    pub fn get_dir_inode(&self, node: &Inode) -> Option<&CachedDirectory> {
        node.directory().and_then(|d| self.directories.get(d))
    }

    #[allow(dead_code)]
    pub fn get_file(&self, node: u64) -> Option<&CachedFile> {
        self.inode_map
            .get(&node)
            .and_then(|n| self.get_file_inode(n))
    }

    pub fn get_file_mut(&mut self, node: u64) -> Option<&mut CachedFile> {
        self.inode_map
            .get(&node)
            .and_then(|n| n.file())
            .and_then(|f| self.files.get_mut(&f))
    }

    pub fn get_file_inode(&self, node: &Inode) -> Option<&CachedFile> {
        node.file().and_then(|f| self.files.get(&f))
    }

    pub fn get_node<'a>(&'a self, node: u64) -> Option<NodeRef<'a>> {
        self.inode_map
            .get(&node)
            .and_then(|k| self.get_node_inode(k))
    }

    pub fn get_node_inode<'a>(&'a self, node: &Inode) -> Option<NodeRef<'a>> {
        self.get_file_inode(node)
            .map(NodeRef::File)
            .or_else(|| self.get_dir_inode(node).map(NodeRef::Dir))
    }

    pub fn get_inode_of_dir(&self, dir: &String) -> Option<u64> {
        self.directories.get(dir).map(|d| d.inode)
    }

    pub async fn reload_directory(
        &mut self,
        client: &CogniteClient,
        raw_dir: &str,
    ) -> Result<(), FsError> {
        let dir = self.directories.get(raw_dir);
        let root = if !raw_dir.is_empty() {
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
            self.files.entry(file.meta.id).or_insert(file);
        }
        Ok(())
    }

    pub async fn open_file(
        &mut self,
        client: &CogniteClient,
        file: u64,
        write: bool,
        read: bool,
    ) -> Result<File, FsError> {
        let file = self
            .inode_map
            .get(&file)
            .and_then(|n| n.file())
            .and_then(|f| self.files.get_mut(&f))
            .ok_or(FsError::FileNotFound)?;

        let should_read = !file.is_new && file.read_at.is_none() && file.meta.uploaded;
        // Always need to open file in write mode...
        let handle = file
            .get_cache_file(&self.cache_dir, should_read || write, read, should_read)
            .await?;

        debug!("File successfully opened!");

        if should_read {
            info!(
                "Downloading file with id {} and name {} from CDF",
                file.meta.id, file.meta.name
            );
            if file.local_mod {
                warn!("Overwriting file with local modifications");
            }
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
                .get_cache_file(&self.cache_dir, write, read, false)
                .await?;
            file.known_size = Some(handle.metadata().await?.len());
            Ok(handle)
        } else {
            Ok(handle)
        }
    }

    pub fn update_stored_size(&mut self, file: u64, new_size: u64) -> Result<(), FsError> {
        let file = self
            .inode_map
            .get(&file)
            .and_then(|n| n.file())
            .and_then(|f| self.files.get_mut(&f))
            .ok_or(FsError::FileNotFound)?;
        info!("Updating file in cache, new size: {}", new_size);
        file.local_mod = true;
        if new_size > file.known_size.unwrap_or(0) {
            file.known_size = Some(new_size);
        }
        Ok(())
    }

    pub fn forget_inode(&mut self, node: u64) {
        match self.inode_map.get(&node) {
            Some(Inode::File { id: f, .. }) => {
                let file = self.files.get_mut(f);
                if let Some(file) = file {
                    file.loaded_at = None;
                    file.known_size = None;
                }
            }
            Some(Inode::Directory { dir: d, .. }) => {
                let dir = self.directories.get_mut(d);
                if let Some(dir) = dir {
                    dir.loaded_at = None;
                }
            }
            None => (),
        }
    }

    async fn upload_changed_file(
        &mut self,
        file: u64,
        client: &CogniteClient,
    ) -> Result<(Option<CachedFile>, Option<i64>, PathBuf), FsError> {
        let old = self
            .inode_map
            .get(&file)
            .and_then(|n| n.file())
            .and_then(|f| self.files.get_mut(&f))
            .ok_or(FsError::FileNotFound)?;
        old.read_at = None;
        let mut path = old.get_cache_file_path(&self.cache_dir);
        if !old.local_mod {
            debug!("File not modified, will not be flushed");
            return Ok((None, None, path));
        }

        let size = tokio::fs::metadata(&path).await?.len();

        let new = client.files.upload(true, &AddFile::from(&old.meta)).await?;
        let mut new = CachedFile {
            inode: new.id as u64,
            meta: new,
            loaded_at: Some(Instant::now()),
            is_new: false,
            read_at: Some(Instant::now()),
            known_size: Some(size),
            local_mod: false,
        };
        let mut ret = None;
        let mut rem_id = None;
        let url = new.meta.upload_url.clone();
        let mime = new
            .meta
            .mime_type
            .clone()
            .unwrap_or_else(|| "application/bytes".to_string());
        let id = new.meta.id;
        new.meta.uploaded = true;
        old.meta.uploaded = true;
        old.local_mod = false;
        // We need to delete the old one...
        if new.meta.id != old.meta.id {
            info!(
                "File {} has changed id, removing old file from CDF",
                new.meta.name
            );
            rem_id = Some(old.meta.id);
            // First, remove it from its parent's array
            let parent = self.directories.get_mut(
                &old.meta
                    .directory
                    .clone()
                    .map(|d| d.trim_start_matches('/').to_string())
                    .unwrap_or_else(|| "".to_string()),
            );
            if let Some(parent) = parent {
                info!("Removing from parent {}...", parent.name);
                let idx = parent
                    .children
                    .iter()
                    .position(|i| i.file() == Some(old.meta.id));
                if let Some(idx) = idx {
                    parent.children.remove(idx);
                }
                info!("Push to parent with id {}", new.meta.id);
                parent
                    .children
                    .push(Inode::new_file(new.meta.id as u64, new.meta.id));
            }

            // Next, remove it from the main files map and the inodes map
            // Next, add a translation from the old ino to a new ino, we do this so that existing references will keep working...
            self.inode_map
                .insert(old.meta.id as u64, Inode::new_file(old.meta.id as u64, id));

            let res = client
                .files
                .delete(&[Identity::Id { id: old.meta.id }])
                .await;
            match res {
                Ok(_) => (),
                Err(e) => warn!("Failed to delete old file from CDF: {:?}", e),
            }

            // Move the buffer file from old to new
            let new_path = Path::new(&self.cache_dir).join(new.meta.id.to_string());
            tokio::fs::rename(&path, &new_path).await?;
            path = new_path;
            ret = Some(new);
        }
        self.inode_map
            .insert(id as u64, Inode::new_file(id as u64, id));
        let rfile = File::open(&path).await?;
        let stream = FramedRead::new(rfile, BytesCodec::new());

        info!("Uploading file {:?} to CDF with new size {}", path, size);
        client
            .files
            .upload_stream_known_size(&mime, &url.unwrap(), stream, size)
            .await?;
        Ok((ret, rem_id, path))
    }

    pub async fn flush_file(
        &mut self,
        client: &CogniteClient,
        file: u64,
    ) -> Result<PathBuf, FsError> {
        let (new, rem_id, path) = self.upload_changed_file(file, client).await?;

        if let Some(new) = new {
            info!("Create new file in cache {} {}", new.meta.name, new.meta.id);
            self.files.insert(new.meta.id, new);
        }
        if let Some(rem_id) = rem_id {
            self.files.remove(&rem_id);
        }

        Ok(path)
    }

    pub async fn close_file(&mut self, client: &CogniteClient, file: u64) -> Result<(), FsError> {
        let path = self.flush_file(client, file).await?;
        if path.exists() {
            remove_file(path).await?;
        }

        Ok(())
    }

    pub async fn open_directory<'a>(
        &'a mut self,
        client: &CogniteClient,
        raw_dir: &str,
    ) -> Result<(Vec<&'a CachedFile>, Vec<&'a CachedDirectory>, Option<u64>), FsError> {
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
            debug!(
                "Load child {:?} {}",
                child,
                self.get_node_inode(child).unwrap().name()
            );
            match child {
                Inode::File { id: f, .. } => {
                    final_files.push(self.files.get(f).ok_or_else(|| FsError::FileNotFound)?)
                }
                Inode::Directory { dir: d, .. } => final_dirs.push(
                    self.directories
                        .get(d)
                        .ok_or_else(|| FsError::DirectoryNotFound)?,
                ),
            }
        }

        Ok((
            final_files,
            final_dirs,
            dir.parent.as_ref().and_then(|p| self.get_inode_of_dir(p)),
        ))
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
                local_mod: false,
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
        let mut old_map = HashMap::new();
        info!("Refresh all directories for root {:?}", root);
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
                        old_map.insert(old.path.clone().unwrap_or_else(|| "".to_string()), old);
                    }
                }
            }
            None => {
                for (k, dir) in self.directories.drain() {
                    old_map.insert(k, dir);
                }
                self.directories.clear();
                self.inode_map.clear();
            }
        }

        let mut inode = self.get_max_inode();

        // The root directory needs to exist
        self.init(root.is_none());

        let now_unix_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let mut visited = HashSet::new();
        for file in files {
            let mut current_path = "".to_string();

            if let Some(dir) = &file.meta.directory {
                let path = Path::new(dir.trim_start_matches('/'));

                for comp in path.components() {
                    let parent = current_path.clone();
                    if !current_path.is_empty() {
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

                        if !self.directories.contains_key(&current_path) {
                            debug!("Inserting directory with path {}", current_path);
                            let old = old_map.remove(&current_path);
                            let keep_children = old
                                .map(|c| {
                                    c.children
                                        .into_iter()
                                        .filter(|c| {
                                            self.get_file_inode(c)
                                                .map(|f| {
                                                    f.is_new
                                                        || now_unix_ts as i64 - f.meta.created_time
                                                            < 60_000
                                                })
                                                .unwrap_or_default()
                                        })
                                        .collect()
                                })
                                .unwrap_or_else(|| vec![]);
                            self.directories.insert(
                                current_path.clone(),
                                CachedDirectory {
                                    path: Some(current_path.clone()),
                                    parent: Some(parent.clone()),
                                    children: keep_children,
                                    loaded_at,
                                    inode: inode + 1,
                                    name: CdfFS::get_dir_name(&current_path).to_string(),
                                },
                            );
                            inode += 1;
                            self.inode_map
                                .insert(inode, Inode::new_directory(inode, current_path.clone()));
                            let current_parent = self.directories.get_mut(&parent).unwrap();
                            current_parent
                                .children
                                .push(Inode::new_directory(inode, current_path.clone()));
                        } else {
                            let dir = self.directories.get_mut(&current_path).unwrap();
                            dir.loaded_at = loaded_at;
                            let inode = dir.inode;
                            let current_parent = self.directories.get_mut(&parent).unwrap();
                            current_parent
                                .children
                                .push(Inode::new_directory(inode, current_path.clone()));
                        }
                    }
                }
            }
            debug!(
                "Loaded file with name {} and id {} into parent {}, directory: {:?}",
                file.meta.name, file.meta.id, current_path, file.meta.directory
            );
            let current_parent = self.directories.get_mut(&current_path).unwrap();
            current_parent
                .children
                .push(Inode::new_file(file.meta.id as u64, file.meta.id));
            self.inode_map.insert(
                file.meta.id as u64,
                Inode::new_file(file.meta.id as u64, file.meta.id),
            );
        }
    }

    pub fn create_dir(&mut self, name: String, parent: u64) -> Result<Inode, FsError> {
        let parent_path = &self
            .get_dir(parent)
            .ok_or_else(|| FsError::DirectoryNotFound)?
            .path;

        let inode = self.get_max_inode() + 1;

        let parent_path_c = parent_path.clone();
        let path = if let Some(p) = parent_path {
            Path::new(&p).join(&name)
        } else {
            Path::new(&name).to_path_buf()
        };
        let path_str = path
            .to_str()
            .ok_or_else(|| FsError::InvalidPath)?
            .to_string();

        let dir = CachedDirectory {
            path: Some(path_str.clone()),
            parent: parent_path_c,
            children: vec![],
            loaded_at: Some(Instant::now()),
            inode,
            name,
        };

        let ind = Inode::new_directory(inode, path_str.clone());

        {
            let parent = self.get_dir_mut(parent).unwrap();
            parent.children.push(ind.clone());
        }

        self.directories.insert(path_str.clone(), dir);
        self.inode_map.insert(inode, ind.clone());

        Ok(ind)
    }

    pub async fn create_file(
        &mut self,
        client: &CogniteClient,
        name: String,
        parent: u64,
    ) -> Result<Inode, FsError> {
        info!("Create file with name {} and parent {}", name, parent);
        let parent_path = &self
            .get_dir(parent)
            .ok_or_else(|| FsError::DirectoryNotFound)?
            .path;

        let mime_type = mime_guess::from_path(&name)
            .first()
            .map(|m| m.to_string())
            .unwrap_or_else(|| "application/binary".to_string());

        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        info!(
            "File created with directory {}",
            parent_path
                .clone()
                .map(|p| format!("/{}", p))
                .unwrap_or("".to_string())
        );

        let meta = AddFile {
            name,
            directory: parent_path.clone().map(|p| format!("/{}", p)),
            mime_type: Some(mime_type),
            source_created_time: Some(time),
            source_modified_time: Some(time),
            ..Default::default()
        };

        let created = client.files.upload(false, &meta).await?;
        let inode = created.id;
        let file = CachedFile {
            inode: inode as u64,
            meta: created,
            loaded_at: Some(Instant::now()),
            is_new: true,
            read_at: None,
            known_size: Some(0),
            local_mod: false,
        };

        let ind = Inode::new_file(inode as u64, inode);

        {
            let parent = self.get_dir_mut(parent).unwrap();
            parent.children.push(ind.clone());
        }

        self.files.insert(inode, file);
        self.inode_map.insert(inode as u64, ind.clone());

        Ok(ind)
    }

    pub async fn delete_node_from_parent(
        &mut self,
        client: &CogniteClient,
        name: &String,
        parent: u64,
    ) -> Result<(), FsError> {
        info!("Delete node {} from parent {}", name, parent);
        let to_remove = {
            let parent = self
                .get_dir(parent)
                .ok_or_else(|| FsError::DirectoryNotFound)?;
            let mut to_remove = None;
            for (idx, child) in parent.children.iter().enumerate() {
                match child {
                    Inode::Directory { dir: d, .. } => {
                        let dir = self.directories.get(d).unwrap();
                        if &dir.name == name {
                            to_remove = Some((idx, child.clone()));
                            break;
                        }
                    }
                    Inode::File { id: f, .. } => {
                        let file = self.files.get(f).unwrap();
                        if &file.meta.name == name {
                            to_remove = Some((idx, child.clone()));
                            break;
                        }
                    }
                }
            }
            to_remove
        };

        let (idx, to_remove) = to_remove.ok_or_else(|| FsError::FileNotFound)?;
        info!("Deleting node with index {}, node {:?}", idx, to_remove);
        let ind = match to_remove {
            Inode::File { id: f, .. } => {
                let file = self.files.remove(&f).unwrap();
                let e = client
                    .files
                    .delete(&[Identity::Id { id: file.meta.id }])
                    .await;
                match e {
                    Ok(()) => (),
                    Err(e) => {
                        self.files.insert(f, file);
                        return Err(FsError::Cognite(e));
                    }
                }
                file.inode
            }
            Inode::Directory { dir: d, .. } => {
                let dir = self.directories.remove(&d).unwrap();
                if !dir.children.is_empty() {
                    self.directories.insert(d, dir);
                    return Err(FsError::NotEmpty);
                }
                dir.inode
            }
        };

        {
            let parent = self
                .get_dir_mut(parent)
                .ok_or_else(|| FsError::DirectoryNotFound)?;
            parent.children.remove(idx);
        }

        self.inode_map.remove(&ind);

        Ok(())
    }
}

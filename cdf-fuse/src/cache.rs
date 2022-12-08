use std::{
    collections::{HashMap, HashSet},
    path::Path,
    time::{Duration, Instant, SystemTime},
};

use cognite::{
    files::{FileFilter, FileMetadata},
    CogniteClient, FilterWithRequest, Identity, PartitionedFilter,
};
use fuser::{FileAttr, FileType, FUSE_ROOT_ID};

use crate::{err::FsError, fs::CdfFS};

pub struct CachedDirectory {
    pub path: Option<String>,
    pub parent: Option<String>,
    pub files: Vec<i64>,
    pub loaded_at: Option<Instant>,
    pub inode: u64,
    pub name: String,
}

pub struct CachedFile {
    pub meta: FileMetadata,
    pub loaded_at: Option<Instant>,
    pub inode: u64,
}

impl CachedFile {
    pub fn get_file_attr(&self) -> FileAttr {
        FileAttr {
            ino: self.inode,
            size: 0, // not usually known at this stage
            blocks: 0,
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
            blksize: 0,
            flags: 512,
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
            nlink: self.files.len() as u32,
            uid: 501,
            gid: 20,
            rdev: 0,
            blksize: 0,
            flags: 512,
        }
    }
}

pub enum Inode {
    File(i64),
    Directory(String),
}

pub struct Cache {
    pub data: HashMap<String, CachedDirectory>,
    pub parents: HashMap<String, Vec<String>>,
    pub files: HashMap<i64, CachedFile>,
    pub inode_map: HashMap<u64, Inode>,
    pub cache_dir: String,
}

impl Cache {
    pub fn new(cache_dir: String) -> Self {
        Cache {
            data: HashMap::new(),
            parents: HashMap::new(),
            files: HashMap::new(),
            inode_map: HashMap::new(),
            cache_dir,
        }
    }

    pub async fn open_directory<'a>(
        &'a mut self,
        client: &CogniteClient,
        raw_dir: &str,
    ) -> Result<(Vec<&'a CachedFile>, Vec<&'a CachedDirectory>), FsError> {
        let dir = self.data.get(raw_dir);
        let should_reload = match dir {
            Some(x) => match x.loaded_at {
                Some(i) => i.elapsed().as_millis() > 600_000,
                None => true,
            },
            None => true,
        };

        if should_reload {
            let root = if raw_dir != "" {
                Some(raw_dir.to_string())
            } else {
                None
            };
            let files = Self::load_cached_directory(
                client,
                dir.map(|d| {
                    if d.loaded_at.is_some() {
                        d.files.len()
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
        }

        let mut final_files = vec![];
        let mut final_dirs = vec![];

        let dir = self.data.get(raw_dir);
        let dir = match dir {
            Some(x) => x,
            None => return Err(FsError::DirectoryNotFound),
        };
        if let Some(children) = self.parents.get(raw_dir) {
            for child in children {
                println!("Push directory {} to final dirs list", child);
                final_dirs.push(self.data.get(child).unwrap());
            }
        }
        for file in &dir.files {
            final_files.push(self.files.get(file).unwrap());
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
        println!("Found {} raw files", res.len());

        Ok(res
            .into_iter()
            .map(|f| CachedFile {
                inode: f.id as u64,
                meta: f,
                loaded_at: None,
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
                for dir in self.data.keys() {
                    if dir.starts_with(x) {
                        to_remove.push(dir.clone());
                    }
                }
                for dir in to_remove {
                    let old = self.data.remove(&dir);
                    if let Some(old) = old {
                        self.inode_map.remove(&old.inode);
                    }
                    self.parents.remove(&dir);
                }
            }
            None => {
                self.data.clear();
                self.parents.clear();
                self.inode_map.clear();
            }
        }

        let mut inode = self.get_max_inode();

        // The root directory needs to exist
        if !self.data.contains_key("") {
            self.data.insert(
                "".to_string(),
                CachedDirectory {
                    path: None,
                    parent: None,
                    files: vec![],
                    loaded_at: if root.is_none() {
                        Some(Instant::now())
                    } else {
                        None
                    },
                    inode: FUSE_ROOT_ID,
                    name: "".to_string(),
                },
            );
            self.inode_map
                .insert(FUSE_ROOT_ID, Inode::Directory("".to_string()));
        }

        let mut visited = HashSet::new();
        for file in files {
            let mut current_path = "".to_string();
            if let Some(dir) = &file.meta.directory {
                let path = Path::new(dir.trim_start_matches("/"));
                let is_visited = visited.contains(dir);
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

                        if !self.parents.contains_key(&parent) {
                            self.parents
                                .insert(parent.clone(), vec![current_path.clone()]);
                        } else {
                            let parents = self.parents.get_mut(&parent).unwrap();
                            parents.push(current_path.clone());
                        }

                        if !self.data.contains_key(&current_path) {
                            println!("Building directory {} with parent {}", current_path, parent);
                            self.data.insert(
                                current_path.clone(),
                                CachedDirectory {
                                    path: Some(current_path.clone()),
                                    parent: Some(parent),
                                    files: vec![],
                                    loaded_at,
                                    inode: inode + 1,
                                    name: CdfFS::get_dir_name(&current_path).to_string(),
                                },
                            );
                            inode += 1;
                            self.inode_map
                                .insert(inode, Inode::Directory(current_path.clone()));
                        } else {
                            let dir = self.data.get_mut(&current_path).unwrap();
                            dir.loaded_at = loaded_at;
                        }
                    }
                }
                println!("Get dir {}", dir);
                let dir = self.data.get_mut(dir.trim_start_matches("/")).unwrap();
                if !is_visited {
                    dir.files.clear();
                }
                println!("Loading file {}", file.meta.name);
                dir.files.push(file.meta.id);
                self.inode_map
                    .insert(file.meta.id as u64, Inode::File(file.meta.id));
            } else {
                println!("Loading file to root at {}", file.meta.name);
                let dir = self.data.get_mut("").unwrap();
                dir.files.push(file.meta.id);
                self.inode_map
                    .insert(file.meta.id as u64, Inode::File(file.meta.id));
            }
        }
    }
}

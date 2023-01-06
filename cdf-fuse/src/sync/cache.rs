use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::Instant,
};

use cognite::files::FileMetadata;
use fuser::FUSE_ROOT_ID;
use tokio::sync::RwLock;

use super::{
    cdf_helper::get_subpaths,
    types::{CacheFileAccess, Node, SyncDirectory, SyncFile},
};

pub struct SyncCache {
    nodes: HashMap<u64, Node>,
    file_map: HashMap<i64, u64>,
    dir_map: HashMap<String, u64>,
    cache_dir: String,
    inode_counter: u64,
}

impl SyncCache {
    pub fn new(cache_dir: String) -> Self {
        Self {
            nodes: HashMap::new(),
            file_map: HashMap::new(),
            dir_map: HashMap::new(),
            cache_dir,
            inode_counter: FUSE_ROOT_ID,
        }
    }

    pub fn init(&mut self) {
        if !self.dir_map.contains_key("/") {
            self.nodes.insert(
                FUSE_ROOT_ID,
                Node::Dir(SyncDirectory {
                    path: "/".to_string(),
                    parent: None,
                    children: vec![],
                    loaded_at: None,
                    inode: FUSE_ROOT_ID,
                    name: "".to_string(),
                    is_new: false,
                }),
            );
            self.dir_map.insert("/".to_string(), FUSE_ROOT_ID);
        }
    }

    pub fn add_file_map(&mut self, id: i64, inode: u64) {
        self.file_map.insert(id, inode);
    }

    pub fn get_node(&self, node: u64) -> Option<&Node> {
        self.nodes.get(&node)
    }

    pub fn get_node_mut(&mut self, node: u64) -> Option<&mut Node> {
        self.nodes.get_mut(&node)
    }

    pub fn get_directory(&self, node: u64) -> Option<&SyncDirectory> {
        self.get_node(node).and_then(|n| n.directory())
    }

    pub fn get_directory_mut(&mut self, node: u64) -> Option<&mut SyncDirectory> {
        self.get_node_mut(node).and_then(|n| n.directory_mut())
    }

    pub fn get_file(&self, node: u64) -> Option<&SyncFile> {
        self.get_node(node).and_then(|n| n.file())
    }

    pub fn get_file_mut(&mut self, node: u64) -> Option<&mut SyncFile> {
        self.get_node_mut(node).and_then(|n| n.file_mut())
    }

    pub fn get_nodes<'a>(&'a self, nodes: &'a [u64]) -> impl Iterator<Item = &Node> + 'a {
        nodes.iter().filter_map(|f| self.get_node(*f))
    }

    pub fn remove_node(&mut self, node: u64) -> Option<Node> {
        let n = self.nodes.remove(&node);
        match &n {
            Some(Node::File(f)) => {
                self.file_map.remove(&f.meta.id);
            }
            Some(Node::Dir(d)) => {
                self.dir_map.remove(&d.path);
            }
            None => (),
        }
        n
    }

    pub fn add_file(&mut self, mut file: FileMetadata, parent: u64) -> u64 {
        if file.id == 0 {
            file.id = -(self.inode_counter as i64 + 1);
        }
        let inode = match self.file_map.get(&file.id) {
            Some(x) => *x,
            None => {
                self.inode_counter += 1;
                self.inode_counter
            }
        };

        if let Some(n) = self.nodes.get_mut(&inode) {
            match n {
                Node::File(f) => {
                    f.meta = file;
                    return inode;
                }
                _ => (),
            }
        }

        self.file_map.insert(file.id, inode);

        self.nodes.insert(
            inode,
            Node::File(SyncFile {
                cache_file: Arc::new(RwLock::new(CacheFileAccess::new(inode, &self.cache_dir))),
                meta: file,
                inode,
                is_new: false,
                parent,
            }),
        );

        inode
    }

    pub fn get_or_add_dir(&mut self, dir: String, parent: Option<u64>) -> u64 {
        let inode = match self.dir_map.get(&dir) {
            Some(x) => *x,
            None => {
                self.inode_counter += 1;
                self.inode_counter
            }
        };

        if let Some(n) = self.nodes.get_mut(&inode) {
            match n {
                Node::Dir(_) => {
                    return inode;
                }
                _ => (),
            }
        }

        self.dir_map.insert(dir.clone(), inode);

        self.nodes.insert(
            inode,
            Node::Dir(SyncDirectory {
                path: dir.clone(),
                parent,
                children: vec![],
                loaded_at: None,
                inode,
                is_new: false,
                name: Path::new(&dir)
                    .components()
                    .last()
                    .unwrap()
                    .as_os_str()
                    .to_str()
                    .unwrap()
                    .to_string(),
            }),
        );

        inode
    }

    pub fn update_directories_from_files(&mut self, files: Vec<FileMetadata>, root: String) {
        let mut built_dirs: HashMap<u64, HashSet<u64>> = HashMap::new();

        for file in files {
            // We need some data from the file before we give up ownership...
            let dir = file.directory.clone();
            let ps = get_subpaths(dir);
            // Iterate over path to build directories
            // let mut last_dir = "/".to_string();
            let mut last_dir = FUSE_ROOT_ID;
            for p in ps.into_iter() {
                let parent = if p == "/" { None } else { Some(last_dir) };
                let dir = self.get_or_add_dir(p.clone(), parent);
                if !built_dirs.contains_key(&dir) {
                    built_dirs.insert(dir, HashSet::new());
                }
                built_dirs.get_mut(&last_dir).unwrap().insert(dir);
                last_dir = dir;
            }
            let inode = self.add_file(file, last_dir);
            let parent = built_dirs.get_mut(&last_dir).unwrap();
            parent.insert(inode);
        }

        let mut dead_nodes = HashSet::new();
        let mut known_nodes = HashSet::new();
        for (dir, entries) in built_dirs {
            // The dir must exist here
            let cdir = self.get_directory(dir).unwrap();
            if !cdir.is_below_path(&root) {
                continue;
            }
            let node_set: HashSet<_> = cdir.children.iter().cloned().collect();

            let mut new_children = vec![];
            for entry in entries.iter() {
                if !node_set.contains(&entry) {
                    new_children.push(*entry);
                }
                known_nodes.insert(*entry);
                dead_nodes.remove(entry);
            }

            for old in node_set {
                if !entries.contains(&old) {
                    let node = self.get_node(old).unwrap();
                    let is_local_new = node.is_new();

                    if is_local_new {
                        new_children.push(old);
                    } else if !known_nodes.contains(&old) {
                        dead_nodes.insert(old);
                    }
                }
            }

            let cdir = self.get_directory_mut(dir).unwrap();
            cdir.children = new_children;
            cdir.loaded_at = Some(Instant::now());
        }

        for node in dead_nodes {
            // TODO: Figure out more cleanup here, not sure how likely this really is
            self.nodes.remove(&node);
        }
    }
}

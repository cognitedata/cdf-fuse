use std::{
    ffi::c_int,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use cognite::{AuthenticatorConfig, CogniteClient};
use fuser::{FileType, Filesystem, FUSE_ROOT_ID};
use log::{debug, trace};
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};

use crate::cache::{Cache, CachedDirectory, CachedFile, Inode};

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Config {
    pub name: String,
    pub project: String,
    pub host: Option<String>,
    pub api_key: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub token_url: Option<String>,
    pub resource: Option<String>,
    pub audience: Option<String>,
    pub scopes: Option<String>,
}

pub struct CdfFS {
    rt: Runtime,
    client: CogniteClient,
    config: Config,
    temp_dir: String,
    cache: Cache,
    fh_counter: AtomicU64,
}

macro_rules! fail {
    ($code:expr, $repl:ident) => {{
        $repl.error($code);
        return;
    }};
}

impl Filesystem for CdfFS {
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), c_int> {
        // Create a temporary directory for us, wiping it first if it already exists.
        let temp_dir_path = Path::new(&self.temp_dir);
        if temp_dir_path.exists() {
            std::fs::remove_dir_all(&temp_dir_path)
                .map_err(|e| e.raw_os_error().unwrap_or(libc::ENOENT))?;
        }
        std::fs::create_dir_all(&temp_dir_path)
            .map_err(|e| e.raw_os_error().unwrap_or(libc::ENOENT))?;

        // Load the root node into the cache
        self.cache.init(false);

        Ok(())
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        let attr = match self.cache.inode_map.get(&ino) {
            Some(x) => match x {
                Inode::File(f) => self.cache.files.get(f).map(|f| f.get_file_attr()),
                Inode::Directory(d) => self.cache.directories.get(d).map(|d| d.get_file_attr()),
            },
            None => {
                if ino == FUSE_ROOT_ID {
                    match self
                        .rt
                        .block_on(self.cache.open_directory(&self.client, ""))
                    {
                        Ok(_) => {}
                        Err(e) => fail!(e.as_code(), reply),
                    }
                    self.cache.directories.get("").map(|d| d.get_file_attr())
                } else {
                    fail!(libc::ENOENT, reply);
                }
            }
        };

        if let Some(a) = attr {
            reply.attr(&Duration::new(0, 0), &a)
        } else {
            fail!(libc::ENOENT, reply);
        }
    }

    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        debug!("Lookup {:?} for parent {}", name.to_str(), parent);
        let name_str = name.to_str().unwrap();
        let (is_loaded, path) = {
            let parent = match self.cache.get_dir(parent) {
                Some(d) => d,
                _ => fail!(libc::ENOENT, reply),
            };
            (
                parent.loaded_at.is_none(),
                parent.path.to_owned().unwrap_or_else(|| "".to_string()),
            )
        };

        if is_loaded {
            match self
                .rt
                .block_on(self.cache.open_directory(&self.client, &path))
            {
                Err(x) => fail!(x.as_code(), reply),
                _ => (),
            }
        }

        let parent = match self.cache.get_dir(parent) {
            Some(d) => d,
            _ => fail!(libc::ENOENT, reply),
        };

        let attr = parent
            .children
            .iter()
            .filter_map(|n| self.cache.get_node(n))
            .find(|n| n.name() == name_str);

        match attr {
            Some(x) => {
                reply.entry(&Duration::new(0, 0), &x.get_file_attr(), 0);
            }
            None => fail!(libc::ENOENT, reply),
        }
    }

    fn opendir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _flags: i32,
        reply: fuser::ReplyOpen,
    ) {
        debug!("Open directory with ino {}", ino);
        let node = match self.cache.inode_map.get(&ino) {
            Some(x) => match x {
                Inode::File(_) => fail!(libc::ENOENT, reply),
                Inode::Directory(d) => d,
            },
            None => fail!(libc::ENOENT, reply),
        }
        .clone();

        match self
            .rt
            .block_on(self.cache.open_directory(&self.client, &node))
        {
            Ok(_) => {}
            Err(e) => fail!(e.as_code(), reply),
        }
        reply.opened(self.get_next_fh(), 0);
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        debug!("Readdir called with offset {} for ino {}", offset, ino);
        let node = match self.cache.inode_map.get(&ino) {
            Some(x) => match x {
                Inode::File(_) => fail!(libc::ENOENT, reply),
                Inode::Directory(d) => d,
            },
            None => fail!(libc::ENOENT, reply),
        }
        .clone();

        let (files, dirs) = match self
            .rt
            .block_on(self.cache.open_directory(&self.client, &node))
        {
            Ok(x) => x,
            Err(e) => fail!(e.as_code(), reply),
        };

        debug!("Found {} files and {} directories", files.len(), dirs.len());

        let iter = Self::to_dir_desc(files, dirs);
        let iter = iter.skip(offset as usize);

        for entry in iter {
            trace!(
                "Add entry {} to buffer with offset {}",
                entry.name,
                entry.offset
            );
            let buffer_full: bool = reply.add(entry.inode, entry.offset, entry.typ, entry.name);

            if buffer_full {
                break;
            }
        }

        reply.ok();
    }
}

struct EntryDesc {
    pub inode: u64,
    pub offset: i64,
    pub typ: FileType,
    pub name: String,
}

impl CdfFS {
    pub fn new(config_path: &str) -> Self {
        let config = std::fs::read(config_path).expect("Failed to read config file");
        let config: Config =
            serde_json::from_slice(&config).expect("Failed to deserialize config file");
        let client_config = config.clone();
        let client = if let Some(key) = client_config.api_key {
            CogniteClient::new_from(
                &key,
                &client_config
                    .host
                    .unwrap_or_else(|| "https://api.cognitedata.com".to_string()),
                &client_config.project,
                "cdf-fuse",
                None,
            )
        } else {
            CogniteClient::new_from_oidc(
                &client_config
                    .host
                    .unwrap_or_else(|| "https://api.cognitedata.com".to_string()),
                AuthenticatorConfig {
                    client_id: client_config
                        .client_id
                        .expect("Client id is required for OIDC"),
                    token_url: client_config
                        .token_url
                        .expect("Token URL is required for OIDC"),
                    secret: client_config
                        .client_secret
                        .expect("Secret is required for OIDC"),
                    resource: client_config.resource,
                    audience: client_config.audience,
                    scopes: client_config.scopes,
                },
                &client_config.project,
                "cdf-fuse",
                None,
            )
        }
        .unwrap();

        let rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let temp_dir = std::env::temp_dir()
            .as_path()
            .join("cognite/fuse")
            .join(&config.name)
            .to_str()
            .unwrap()
            .to_string();
        CdfFS {
            rt,
            client,
            config,
            cache: Cache::new(temp_dir.clone()),
            temp_dir,
            fh_counter: AtomicU64::new(0),
        }
    }

    fn to_dir_desc<'a>(
        files: Vec<&'a CachedFile>,
        dirs: Vec<&'a CachedDirectory>,
    ) -> impl Iterator<Item = EntryDesc> + 'a {
        let len = files.len();
        files
            .into_iter()
            .enumerate()
            .map(|(idx, f)| EntryDesc {
                inode: f.inode,
                offset: idx as i64 + 1,
                typ: FileType::RegularFile,
                name: f.meta.name.clone(),
            })
            .chain(dirs.into_iter().enumerate().map(move |(idx, f)| EntryDesc {
                inode: f.inode,
                offset: idx as i64 + (len as i64) + 1,
                typ: FileType::Directory,
                name: f.name.clone(),
            }))
    }

    fn get_next_fh(&mut self) -> u64 {
        self.fh_counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn get_dir_name(dir: &str) -> &str {
        let path = Path::new(dir);
        path.components()
            .last()
            .unwrap()
            .as_os_str()
            .to_str()
            .unwrap()
    }
}

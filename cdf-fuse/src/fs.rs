use std::{
    ffi::c_int,
    io::SeekFrom,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use cognite::{AuthenticatorConfig, CogniteClient};
use fuser::{FileType, Filesystem, FUSE_ROOT_ID};
use log::{debug, trace};
use serde::Deserialize;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    runtime::{Builder, Runtime},
};

use crate::{
    cache::{Cache, CachedDirectory, CachedFile, Inode},
    err::FsError,
};

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
    #[allow(dead_code)]
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

macro_rules! run {
    ($slf:ident, $repl:ident, $fut:expr) => {
        match $slf.rt.block_on($fut) {
            Ok(x) => x,
            Err(e) => fail!(e.as_code(), $repl),
        }
    };
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
            std::fs::remove_dir_all(temp_dir_path)
                .map_err(|e| e.raw_os_error().unwrap_or(libc::ENOENT))?;
        }
        std::fs::create_dir_all(temp_dir_path)
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
                parent.path.to_owned().unwrap_or_default(),
            )
        };

        if is_loaded {
            run!(self, reply, self.cache.open_directory(&self.client, &path));
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

        let (files, dirs) = run!(self, reply, self.cache.open_directory(&self.client, &node));
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

    fn open(&mut self, _req: &fuser::Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        debug!("open() called for inode {}", ino);
        let (is_read, is_write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => (true, false),
            libc::O_RDWR => (true, true),
            libc::O_WRONLY => (false, true),
            _ => fail!(libc::EINVAL, reply),
        };
        run!(
            self,
            reply,
            self.cache.open_file(&self.client, ino, is_write, is_read)
        );
        reply.opened(self.get_next_fh(), 0);
    }

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        run!(
            self,
            reply,
            Self::write_from_buf(&mut self.cache, &self.client, ino, offset, data)
        );
        reply.written(data.len() as u32);
    }

    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        // Closing the file means releasing the cache
        // This way when you reopen a file it is reloaded, but not before.
        // No real way to do much better than this.
        debug!("Closing file with ino {} and wiping cached data", ino);
        run!(self, reply, self.cache.close_file(&self.client, ino));
        reply.ok()
    }

    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        // Not alot we can do here...
        self.getattr(_req, ino, reply);
    }

    fn flush(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok()
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let data = run!(
            self,
            reply,
            Self::read_to_buf(&mut self.cache, &self.client, ino, offset, size)
        );
        reply.data(&data);
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

    async fn read_to_buf(
        cache: &mut Cache,
        client: &CogniteClient,
        ino: u64,
        offset: i64,
        size: u32,
    ) -> Result<Vec<u8>, FsError> {
        debug!("Open file with ino {} for read", ino);
        let mut file = cache.open_file(client, ino, false, true).await?;
        let file_size = file.metadata().await?.len();
        let read_size = size.min(file_size.saturating_sub(offset as u64) as u32);
        debug!(
            "Read data from file with size {}, {} bytes",
            file_size, read_size
        );

        let mut buffer = vec![0u8; read_size as usize];
        file.seek(SeekFrom::Start(offset as u64)).await?;
        debug!("Begin read");
        file.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    async fn write_from_buf(
        cache: &mut Cache,
        client: &CogniteClient,
        ino: u64,
        offset: i64,
        data: &[u8],
    ) -> Result<(), FsError> {
        debug!("Open file with ino {} for write", ino);
        let mut file = cache.open_file(client, ino, true, false).await?;
        file.seek(SeekFrom::Start(offset as u64)).await?;
        file.write_all(data).await?;
        cache.update_stored_size(ino, (data.len() + offset as usize) as u64)?;
        file.flush().await?;
        debug!("Finished writing from buffer");
        Ok(())
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

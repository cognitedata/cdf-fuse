use std::{
    io::SeekFrom,
    path::PathBuf,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use cognite::{
    files::{AddFile, FileMetadata},
    CogniteClient, Delete, Identity,
};
use fuser::FileAttr;
use futures_util::{SinkExt, TryStreamExt};
use log::{info, warn};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{RwLock, RwLockWriteGuard},
};
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

use crate::err::FsError;

use super::{
    cache::SyncCache,
    cdf_helper::load_cached_directory,
    types::{CacheFileAccess, Node, NodeInfo},
};

pub struct State {
    pub cache: Arc<RwLock<SyncCache>>,
    pub client: Arc<CogniteClient>,
}

impl State {
    pub fn new(client: CogniteClient, cache_dir: String) -> Self {
        Self {
            cache: Arc::new(RwLock::new(SyncCache::new(cache_dir))),
            client: Arc::new(client),
        }
    }

    pub async fn init(&self) {
        self.cache.write().await.init();
    }

    pub async fn open_directory(&self, node: u64) -> Result<(Vec<u64>, NodeInfo), FsError> {
        let info = {
            let cache = self.cache.read().await;
            let dir = cache
                .get_directory(node)
                .ok_or(FsError::DirectoryNotFound)?;

            if !dir.should_reload() {
                return Self::get_directory_children(node, &cache)
                    .map(|r| (r, dir.get_node_info()));
            }
            dir.get_node_info()
        };

        let lock = self.reload_directory(node).await?;
        Self::get_directory_children(node, &lock).map(|r| (r, info))
    }

    fn get_directory_children(node: u64, cache: &SyncCache) -> Result<Vec<u64>, FsError> {
        let dir = cache
            .get_directory(node)
            .ok_or(FsError::DirectoryNotFound)?;
        Ok(dir.children.clone())
    }

    pub async fn get_node_infos(&self, nodes: &[u64]) -> Vec<NodeInfo> {
        let cache = self.cache.read().await;
        cache.get_nodes(nodes).map(|n| n.get_node_info()).collect()
    }

    #[allow(dead_code)]
    pub async fn get_node_info(&self, node: u64) -> Result<NodeInfo, FsError> {
        let cache = self.cache.read().await;
        Ok(cache
            .get_node(node)
            .ok_or(FsError::FileNotFound)?
            .get_node_info())
    }

    #[allow(dead_code)]
    pub async fn get_file_attrs(&self, nodes: &[u64]) -> Vec<FileAttr> {
        let mut res = vec![];
        let cache = self.cache.read().await;
        for node in cache.get_nodes(nodes) {
            res.push(node.get_file_attr().await);
        }
        res
    }

    async fn reload_directory(&self, node: u64) -> Result<RwLockWriteGuard<SyncCache>, FsError> {
        let mut cache = self.cache.write().await;
        let dir = cache
            .get_directory_mut(node)
            .ok_or(FsError::DirectoryNotFound)?;
        if !dir.should_reload() {
            return Ok(cache);
        }

        let expected_size = if dir.loaded_at.is_some() {
            dir.children.len()
        } else {
            100_000
        };
        // Set loaded at here, even if there are no results from CDF, it is still going to
        // be loaded
        dir.loaded_at = Some(Instant::now());

        let root = dir.path.clone();
        let files = load_cached_directory(&self.client, expected_size, root.clone()).await?;

        cache.update_directories_from_files(files, root);

        Ok(cache)
    }

    pub async fn get_file_attr(&self, node: u64) -> Result<FileAttr, FsError> {
        let node = self
            .cache
            .read()
            .await
            .get_node(node)
            .ok_or(FsError::FileNotFound)?
            .get_file_attr()
            .await;
        Ok(node)
    }

    pub async fn open_file(&self, file: u64) -> Result<(), FsError> {
        let cache = self.cache.read().await;
        let file = cache.get_file(file).ok_or(FsError::FileNotFound)?;
        let should_read = {
            let fcache = file.cache_file.read().await;
            !file.is_new
                && (fcache.loaded_at.is_none() || !fcache.exists())
                && file.meta.uploaded
                && !fcache.local_mod
        };

        if !should_read {
            return Ok(());
        }

        let mut fcache = file.cache_file.write().await;
        // Need to check again after locking the write lock...
        if file.is_new
            || fcache.loaded_at.is_some() && fcache.exists()
            || !file.meta.uploaded
            || fcache.local_mod
        {
            return Ok(());
        }

        let handle = fcache.get_handle_write(false, false).await?;

        let mut stream = self
            .client
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
        fcache.loaded_at = Some(Instant::now());
        let handle = fcache.get_handle_write(false, false).await?;
        fcache.known_size = Some(handle.metadata().await?.len());

        Ok(())
    }

    pub async fn read_to_buf(&self, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>, FsError> {
        let cache = self.cache.read().await;
        let file = cache.get_file(ino).ok_or(FsError::FileNotFound)?;
        let fcache = file.cache_file.read().await;
        let mut handle = fcache.get_handle_read().await?;

        let file_size = handle.metadata().await?.len();
        let read_size = size.min(file_size.saturating_sub(offset as u64) as u32);

        let mut buffer = vec![0u8; read_size as usize];
        handle.seek(SeekFrom::Start(offset as u64)).await?;
        handle.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    pub async fn write_from_buf(&self, ino: u64, offset: i64, data: &[u8]) -> Result<(), FsError> {
        let cache = self.cache.read().await;
        let file = cache.get_file(ino).ok_or(FsError::FileNotFound)?;
        let mut fcache = file.cache_file.write().await;
        let mut handle = fcache.get_handle_write(false, false).await?;
        handle.seek(SeekFrom::Start(offset as u64)).await?;
        handle.write_all(data).await?;
        fcache.local_mod = true;
        let new_size = (data.len() + offset as usize) as u64;
        if new_size > fcache.known_size.unwrap_or(0) {
            fcache.known_size = Some(new_size);
        }
        Ok(())
    }

    async fn upload_changed_file(
        node: u64,
        client: Arc<CogniteClient>,
        gcache: Arc<RwLock<SyncCache>>,
        fcache: Arc<RwLock<CacheFileAccess>>,
    ) -> Result<(), FsError> {
        // First, we need to upload meta to CDF, which requires a write lock on the cache
        let (url, mime) = {
            let mut cache = gcache.write().await;
            let old = cache.get_file_mut(node).ok_or(FsError::FileNotFound)?;
            let oldid = old.meta.id;
            let new = client.files.upload(true, &AddFile::from(&old.meta)).await?;
            let newid = new.id;
            let url = new.upload_url.clone();
            let mime = new
                .mime_type
                .clone()
                .unwrap_or_else(|| "application/bytes".to_string());
            old.meta = new;
            let inode = old.inode;

            if oldid != newid {
                // Id has changed, need to update the node map
                cache.add_file_map(newid, inode);
            }
            (url.unwrap(), mime)
        };

        // Release the lock on the cache, or we risk a deadlock later, we also don't want the sync to lock the cache if possible.
        {
            let mut fcache = fcache.write().await;
            fcache.loaded_at = Some(Instant::now());
            let size = fcache.size().await?;
            let rfile = fcache.get_handle_read().await?;
            let stream = FramedRead::new(rfile, BytesCodec::new());

            client
                .files
                .upload_stream_known_size(&mime, &url, stream, size)
                .await?;
        }
        info!("Uploaded file with ino {} to CDF", node);

        Ok(())
    }

    pub async fn synchronize(&self, ino: u64, block: bool) -> Result<(), FsError> {
        let cache = self.cache.read().await;
        let file = cache.get_file(ino).ok_or(FsError::FileNotFound)?;
        // First, try obtaining write access to the cache file, this will wait for ongoing synchronization...
        let mut fcache = file.cache_file.write().await;
        if !fcache.local_mod {
            return Ok(());
        }
        // Need to set local mod to false here, otherwise we risk multiple syncs being spawned
        fcache.local_mod = false;

        let send_cache = self.cache.clone();
        let send_fcache = file.cache_file.clone();
        let send_client = self.client.clone();
        let fut = tokio::spawn(Self::upload_changed_file(
            ino,
            send_client,
            send_cache,
            send_fcache,
        ));

        if block {
            fut.await.unwrap()?;
        }

        Ok(())
    }

    pub async fn close(&self, ino: u64) -> Result<(), FsError> {
        info!("Close file with ino {}", ino);
        self.synchronize(ino, false).await
    }

    pub async fn set_size(&self, ino: u64, size: u64) -> Result<(), FsError> {
        info!("Truncate file with ino {} to {} bytes", ino, size);
        let cache = self.cache.read().await;
        let file = cache.get_file(ino).ok_or(FsError::FileNotFound)?;
        let mut fcache = file.cache_file.write().await;
        fcache.set_size(size).await?;
        Ok(())
    }

    pub async fn add_file(&self, name: String, parent: u64) -> Result<FileAttr, FsError> {
        let mut cache = self.cache.write().await;
        let parent_dir = cache
            .get_directory(parent)
            .ok_or(FsError::DirectoryNotFound)?;
        let existing = parent_dir
            .children
            .iter()
            .filter_map(|n| cache.get_node(*n))
            .find(|n| n.name() == name);

        if existing.is_some() {
            return Err(FsError::Conflict);
        }

        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let meta = FileMetadata {
            name: name.clone(),
            directory: parent_dir.get_cdf_directory(),
            mime_type: Some(
                mime_guess::from_path(name)
                    .first()
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| "application/binary".to_string()),
            ),
            source_created_time: Some(time),
            source_modified_time: Some(time),
            id: 0,
            uploaded: false,
            ..Default::default()
        };
        let file = cache.add_file(meta, parent);

        let parent = cache
            .get_directory_mut(parent)
            .ok_or(FsError::DirectoryNotFound)?;
        parent.children.push(file);

        Ok(cache.get_file(file).unwrap().get_file_attr().await)
    }

    pub async fn add_dir(&self, name: String, parent: u64) -> Result<FileAttr, FsError> {
        let mut cache = self.cache.write().await;
        let parent_dir = cache
            .get_directory(parent)
            .ok_or(FsError::DirectoryNotFound)?;

        let existing = parent_dir
            .children
            .iter()
            .filter_map(|n| cache.get_node(*n))
            .find(|n| n.name() == name);

        if existing.is_some() {
            return Err(FsError::Conflict);
        }

        let path = PathBuf::from(parent_dir.path.clone()).join(name);

        let inode = cache.get_or_add_dir(
            path.to_str().ok_or(FsError::InvalidPath)?.to_string(),
            Some(parent),
        );

        let node = cache.get_directory_mut(inode).unwrap();
        node.loaded_at = Some(Instant::now());

        let parent = cache
            .get_directory_mut(parent)
            .ok_or(FsError::DirectoryNotFound)?;
        parent.children.push(inode);

        Ok(cache.get_directory(inode).unwrap().get_file_attr())
    }

    pub async fn delete_node_from_parent(&self, name: String, parent: u64) -> Result<(), FsError> {
        let mut cache = self.cache.write().await;
        let parent_dir = cache
            .get_directory(parent)
            .ok_or(FsError::DirectoryNotFound)?;

        let existing = parent_dir
            .children
            .iter()
            .filter_map(|n| cache.get_node(*n))
            .find(|n| n.name() == name)
            .ok_or(FsError::FileNotFound)?;

        let inode = existing.ino();

        // Do pre-checks...
        if let Node::Dir(d) = existing {
            if !d.children.is_empty() {
                return Err(FsError::NotEmpty);
            }
        }

        let removed = cache.remove_node(inode).unwrap();
        let inode = removed.ino();

        // Actually remove the file
        if let Node::File(f) = removed {
            let mut fcache = f.cache_file.write().await;
            if f.meta.id > 0 {
                self.client
                    .files
                    .delete(&[Identity::Id { id: f.meta.id }])
                    .await?;
            }

            if let Err(e) = fcache.delete_cache().await {
                warn!("Failed to delete cache file {:?}", e)
            }
        }

        let parent_dir = cache.get_directory_mut(parent).unwrap();
        parent_dir.children.retain(|i| i != &inode);

        Ok(())
    }
}

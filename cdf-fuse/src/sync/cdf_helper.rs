use std::path::Path;

use cognite::{
    files::{FileFilter, FileMetadata},
    CogniteClient, FilterWithRequest, PartitionedFilter,
};
use log::info;

use crate::err::FsError;

pub async fn load_cached_directory(
    client: &CogniteClient,
    expected_size: usize,
    path: String,
) -> Result<Vec<FileMetadata>, FsError> {
    let num_parallel = (expected_size / 1000).clamp(1, 10);

    let path = if path == "/" { None } else { Some(path) };

    info!(
        "Loading files from CDF, doing {} parallel queries",
        num_parallel
    );
    let res = client
        .files
        .filter_all_partitioned(
            PartitionedFilter::new(
                FileFilter {
                    directory_prefix: path,
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
    Ok(res)
}

pub fn get_subpaths(path: Option<String>) -> Vec<String> {
    let path = match path {
        Some(x) => x,
        None => return vec!["/".to_string()],
    };
    let mut path: Vec<_> = Path::new(&path)
        .ancestors()
        .into_iter()
        .map(|p| p.to_str().unwrap().to_string())
        .collect();
    path.reverse();
    path
}

use std::path::PathBuf;
use std::sync::Arc;

use cas_client::Client;
use cas_types::FileRange;
use file_reconstruction::{DataOutput, FileReconstructor};
use merklehash::MerkleHash;
use progress_tracking::item_tracking::ItemProgressUpdater;
use tracing::instrument;

use crate::errors::*;
use crate::prometheus_metrics;

/// Manages the download of files based on a hash or pointer file.
pub struct FileDownloader {
    client: Arc<dyn Client>,
}

impl FileDownloader {
    pub fn new(client: Arc<dyn Client>) -> Self {
        Self { client }
    }

    #[instrument(skip_all, name = "FileDownloader::smudge_file_from_hash", fields(hash=file_id.hex()))]
    pub async fn smudge_file_from_hash(
        &self,
        file_id: &MerkleHash,
        file_name: Arc<str>,
        output_path: PathBuf,
        range: Option<FileRange>,
        progress_updater: Option<Arc<ItemProgressUpdater>>,
    ) -> Result<u64> {
        let file_progress_tracker = progress_updater.map(|p| ItemProgressUpdater::item_tracker(&p, file_name, None));

        let output = DataOutput::write_in_file(output_path);

        let mut reconstructor = FileReconstructor::new(&self.client, *file_id, output);

        if let Some(range) = range {
            reconstructor = reconstructor.with_byte_range(range);
        }

        if let Some(tracker) = file_progress_tracker {
            reconstructor = reconstructor.with_progress_updater(tracker);
        }

        reconstructor.run().await?;

        // TODO: Return actual bytes written from FileReconstructor
        let n_bytes = 0u64;
        prometheus_metrics::FILTER_BYTES_SMUDGED.inc_by(n_bytes);

        Ok(n_bytes)
    }
}

use bytes::Bytes;
use std::{collections::HashMap, io::SeekFrom, usize};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tracing::{info, instrument};

#[derive(Debug)]
pub(crate) struct Record {
    record_file: File,
    key_map: HashMap<String, u64>,
}

impl Record {
    pub async fn new(record_name: &str) -> Self {
        let record_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(record_name)
            //TODO: Handle error here
            .await
            .unwrap();

        Record {
            record_file: record_file,
            key_map: HashMap::new(),
        }
    }

    #[instrument]
    pub async fn write_record(
        &mut self,
        key: String,
        data: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let cursor = self.record_file.seek(SeekFrom::End(0)).await?;
        self.key_map.insert(key, cursor);

        self.record_file.write_all(&data[..]).await?;

        info!("Wrote data");
        Ok(())
    }
}

use bytes::Bytes;
use std::{collections::HashMap, usize};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

pub(crate) struct Record {
    record_file: File,
    map: HashMap<String, usize>,
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
            map: HashMap::new(),
        }
    }

    pub async fn write_record(
        &mut self,
        key: String,
        data: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.record_file.write_all(&data[..]).await?;

        Ok(())
    }
}

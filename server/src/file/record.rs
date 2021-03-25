use bytes::{Bytes, BytesMut};
use std::{
    collections::HashMap,
    io::{Cursor, SeekFrom},
};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
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

    #[instrument]
    pub async fn read_record(
        &mut self,
        key: String,
    ) -> Result<Option<Bytes>, Box<dyn std::error::Error + Send + Sync>> {
        match self.key_map.get(&key) {
            Some(offset) => {
                let pos = self.record_file.seek(SeekFrom::Start(*offset)).await?;
                let mut buf = vec![0, 0, 0, 0];

                info!("Reading data..");

                self.record_file.read_exact(&mut buf[..]).await?;
                let mut cursor = Cursor::new(buf);
                let size = cursor.read_i32().await?;

                let _ = self.record_file.seek(SeekFrom::Start(pos)).await?;

                let mut data = BytesMut::with_capacity(size as usize);
                self.record_file.read_buf(&mut data).await?;

                Ok(Some(data.freeze()))
            }
            None => Ok(None),
        }
    }
}

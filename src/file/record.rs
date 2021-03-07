use bytes::BytesMut;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

pub(crate) struct Record {
    record_file: File,
}

impl Record {
    pub async fn new(record_name: &str) -> Self {
        let mut record_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(record_name)
            //TODO: Handle error here
            .await
            .unwrap();

        Record {
            record_file: record_file,
        }
    }

    pub async fn write_record(
        &mut self,
        data: BytesMut,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.record_file.write_all(&data[..]).await?;

        Ok(())
    }
}

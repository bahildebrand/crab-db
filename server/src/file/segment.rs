use bytes::Bytes;
use std::collections::BTreeMap;
use tokio::fs::{File, OpenOptions};
use tracing::{info, instrument};

pub(crate) struct SegmentManager {
    segment_map_file: File,
    segment_map: Vec<String>,
    current_segment: Segment,
}

impl SegmentManager {
    pub async fn new(segment_map_filename: &str) -> Self {
        let segment_map_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(segment_map_filename)
            .await
            .unwrap();

        Self {
            segment_map_file,
            segment_map: Vec::new(),
            current_segment: Segment::new().await,
        }
    }

    pub async fn put(&mut self, key: String, value: Bytes) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.current_segment.write_segment(key, value).await
    }

    pub async fn get(&mut self, key: String) -> Result<Option<Bytes>, Box<dyn std::error::Error + Send + Sync>> {
        self.current_segment.read_segment(key).await
    }
}

#[derive(Debug)]
pub(crate) struct Segment {
    key_map: BTreeMap<String, Bytes>,
}

impl Segment {
    pub async fn new() -> Self {
        Segment {
            key_map: BTreeMap::new(),
        }
    }

    #[instrument]
    pub async fn write_segment(
        &mut self,
        key: String,
        data: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.key_map.insert(key, data);

        info!("Wrote data");
        Ok(())
    }

    #[instrument]
    pub async fn read_segment(
        &mut self,
        key: String,
    ) -> Result<Option<Bytes>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.key_map.get(&key).cloned())
    }

    pub fn memtable_size(&self) -> usize {
        self.key_map.len()
    }
}

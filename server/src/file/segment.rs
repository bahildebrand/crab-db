use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::{info, instrument};

pub(crate) struct SegmentManager {
    segment_map: Arc<RwLock<SegmentMap>>,
    current_segment: Segment,
}

impl SegmentManager {
    const MAX_SEGMENT_SIZE: usize = 2048; // Max segment size in bytes

    pub async fn new(segment_map_filename: &str) -> Self {
        let segment_map = Arc::new(RwLock::new(SegmentMap::new(segment_map_filename).await));

        let mut segment_merger = SegmentMerger::new(segment_map.clone());
        tokio::spawn(async move {
            segment_merger.run().await;
        });

        Self {
            segment_map,
            current_segment: Segment::new().await,
        }
    }

    pub async fn put(
        &mut self,
        key: String,
        value: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.current_segment.write_segment(key, value).await?;

        if self.current_segment.size() > Self::MAX_SEGMENT_SIZE {
            let old_map = self.current_segment.split_segment();

            self.segment_map
                .write()
                .await
                .write_new_segment_file(old_map)
                .await;
        }

        Ok(())
    }

    pub async fn get(
        &mut self,
        key: String,
    ) -> Result<Option<Bytes>, Box<dyn std::error::Error + Send + Sync>> {
        self.current_segment.read_segment(key).await
    }
}

struct SegmentMerger {
    segment_map: Arc<RwLock<SegmentMap>>,
}

impl SegmentMerger {
    const MERGE_DURATION_TIME_MS: u64 = 200;

    fn new(segment_map: Arc<RwLock<SegmentMap>>) -> Self {
        Self { segment_map }
    }

    async fn run(&mut self) {
        let mut interval =
            tokio::time::interval(Duration::from_millis(Self::MERGE_DURATION_TIME_MS));

        loop {
            interval.tick().await;

            // TODO Actually merge segment maps
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct SegmentMapData {
    data: Vec<String>,
}

struct SegmentMap {
    segment_map_file: File,
    segment_data: SegmentMapData,
}

impl SegmentMap {
    async fn new(segment_map_filename: &str) -> Self {
        let mut segment_map_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(segment_map_filename)
            .await
            .unwrap();

        let mut segment_data_bytes = Vec::new();
        segment_map_file
            .read_to_end(&mut segment_data_bytes)
            .await
            .unwrap();

        let segment_data: SegmentMapData = bson::from_slice(segment_data_bytes.as_slice()).unwrap();

        Self {
            segment_map_file,
            segment_data,
        }
    }

    async fn write_new_segment_file(&mut self, segment: SegmentData) {
        let start = SystemTime::now();
        let timestamp_ms = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let segment_filename = format!("segment-{}", timestamp_ms);
        self.segment_data.data.push(segment_filename.clone());
        let mut segment_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(segment_filename)
            .await
            .unwrap();

        let data = bson::to_vec(&segment).unwrap();
        segment_file.write_all(&data).await.unwrap();

        // This is really dumb. I need a way to append in place instead of rewriting files
        self.segment_map_file.rewind().await.unwrap();
        self.write_segment_map().await;
    }

    async fn write_segment_map(&mut self) {
        let data = bson::to_vec(&self.segment_data).unwrap();
        self.segment_map_file.write_all(&data).await.unwrap();
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct SegmentData {
    data: BTreeMap<String, Bytes>,
}

#[derive(Debug)]
pub(crate) struct Segment {
    key_map: SegmentData,
    segment_size: usize,
}

impl Segment {
    pub async fn new() -> Self {
        Segment {
            key_map: SegmentData::default(),
            segment_size: 0,
        }
    }

    #[instrument]
    pub async fn write_segment(
        &mut self,
        key: String,
        data: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let new_data_size = data.len();
        self.segment_size = if let Some(old_data) = self.key_map.data.insert(key, data) {
            self.segment_size - old_data.len() + new_data_size
        } else {
            self.segment_size + new_data_size
        };

        info!("Wrote data");
        Ok(())
    }

    #[instrument]
    pub async fn read_segment(
        &mut self,
        key: String,
    ) -> Result<Option<Bytes>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.key_map.data.get(&key).cloned())
    }

    fn split_segment(&mut self) -> SegmentData {
        self.segment_size = 0;
        std::mem::take(&mut self.key_map)
    }

    fn size(&self) -> usize {
        self.segment_size
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_segment_map_serde() {
        let mut segment_map_data = SegmentMapData::default();

        segment_map_data.data.push("segment-1".into());
        segment_map_data.data.push("segment-2".into());

        let bson_document = bson::to_document(&segment_map_data).unwrap();

        assert_eq!(
            segment_map_data,
            bson::from_document(bson_document).unwrap()
        );
    }
}

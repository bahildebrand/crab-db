use bson::Document;
use crab_db::crab_db_client::CrabDbClient;
use crab_db::{ReadRequest, WriteRequest};
use std::io::Cursor;
use tonic::{transport::Channel, Request};

pub mod crab_db {
    tonic::include_proto!("crab_db"); // The string specified here must match the proto package name
}

pub struct CrabClient {
    client: CrabDbClient<Channel>,
}

impl CrabClient {
    pub async fn new(addr: String) -> Self {
        CrabClient {
            client: CrabDbClient::connect(addr).await.unwrap(),
        }
    }

    pub async fn write(
        &mut self,
        key: String,
        data: Document,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut buffer = Vec::<u8>::new();
        data.to_writer(&mut buffer)?;

        let request = Request::new(WriteRequest {
            key: key,
            data: buffer,
        });

        let response = self.client.write(request).await?;

        println!("Response: {:?}", response);
        Ok(())
    }

    pub async fn read(&mut self, key: String) -> Result<Document, Box<dyn std::error::Error>> {
        let request = Request::new(ReadRequest { key: key });

        let response = self.client.read(request).await?;

        println!("Response: {:?}", response);
        let data = response.into_inner().data;
        let mut cursor = Cursor::new(&data);
        let doc = Document::from_reader(&mut cursor)?;
        Ok(doc)
    }
}

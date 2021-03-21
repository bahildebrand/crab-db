use crab_db::crab_db_client::CrabDbClient;
use crab_db::{ReadRequest, WriteRequest};
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
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let request = Request::new(WriteRequest {
            key: key,
            data: data,
        });

        let response = self.client.write(request).await?;

        println!("Response: {:?}", response);
        Ok(())
    }

    pub async fn read(&mut self, key: String) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let request = Request::new(ReadRequest { key: key });

        let response = self.client.read(request).await?;

        println!("Response: {:?}", response);
        Ok(response.into_inner().data)
    }
}

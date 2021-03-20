use crab_db::crab_db_client::CrabDbClient;
use crab_db::{WriteRequest, WriteResponse};
use tonic::{transport::Channel, Request, Response, Status};

pub mod crab_db {
    tonic::include_proto!("crab_db"); // The string specified here must match the proto package name
}

pub struct CrabClient {
    client: CrabDbClient<Channel>,
}

impl CrabClient {
    pub async fn new(addr: String) -> Self {
        CrabClient {
            client: CrabDbClient::connect(addr).await.unwrap()
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
}

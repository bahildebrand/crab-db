use crab_db::crab_db_client::CrabDbClient;
use crab_db::{WriteRequest, WriteResponse};
use tonic::{transport::Channel, Request, Response, Status};

pub mod crab_db {
    tonic::include_proto!("crab_db"); // The string specified here must match the proto package name
}

struct CrabClient {
    client: CrabDbClient<Channel>,
}

impl CrabClient {
    pub fn new(client: CrabDbClient<Channel>) -> Self {
        CrabClient { client: client }
    }

    pub async fn write(&mut self, key: String, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        let request = Request::new(WriteRequest {
            key: key,
            data: data,
        });

        let response = self.client.write(request).await?;

        println!("Response: {:?}", response);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CrabClient::new(CrabDbClient::connect("http://[::1]:50051").await?);

    client.write("Hello".into(), "Stuff".into()).await?;

    Ok(())
}

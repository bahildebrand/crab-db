use crab_db::crab_db_server::CrabDb;
use crab_db::{WriteRequest, WriteResponse};
use tonic::{Request, Response, Status};

pub mod crab_db {
    tonic::include_proto!("crab_db"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct ProtoHandler {}

#[tonic::async_trait]
impl CrabDb for ProtoHandler {
    async fn write(
        &self,
        request: Request<WriteRequest>
    ) -> Result<Response<WriteResponse>, Status> {
        let response = WriteResponse {
            message: format!("Wrote {}", request.into_inner().key).into(), // We must use .into_inner() as the fields of gRPC requests and responses are private
        };

        Ok(Response::new(response))
    }
}
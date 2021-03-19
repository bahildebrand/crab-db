use crate::file::actor::FileActorHandle;
use bytes::Bytes;
use crab_db::crab_db_server::CrabDb;
use crab_db::{WriteRequest, WriteResponse};
use tonic::{Request, Response, Status};

pub mod crab_db {
    tonic::include_proto!("crab_db"); // The string specified here must match the proto package name
}
pub(crate) struct ProtoHandler {
    actor_handle: FileActorHandle,
}

impl ProtoHandler {
    pub fn new(handle: FileActorHandle) -> Self {
        ProtoHandler {
            actor_handle: handle,
        }
    }
}

#[tonic::async_trait]
impl CrabDb for ProtoHandler {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let write_request = request.into_inner();

        self.actor_handle
            .write_data(write_request.key.clone(), Bytes::from(write_request.data))
            .await;

        let response = WriteResponse {
            message: format!("Wrote {}", write_request.key.clone()).into(), // We must use .into_inner() as the fields of gRPC requests and responses are private
        };

        Ok(Response::new(response))
    }
}

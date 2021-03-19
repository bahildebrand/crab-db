mod file;
mod protocol;

use file::actor::FileActorHandle;
use protocol::crab_db::crab_db_server::CrabDbServer;
use protocol::ProtoHandler;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root_handle = FileActorHandle::new();
    let proto = ProtoHandler::new(root_handle);

    let addr = "[::1]:50051".parse()?;
    Server::builder()
        .add_service(CrabDbServer::new(proto))
        .serve(addr)
        .await?;

    Ok(())
}

mod file;
mod protocol;

use file::actor::FileActorHandle;
use protocol::crab_db::crab_db_server::CrabDbServer;
use protocol::ProtoHandler;
use tonic::transport::Server;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("Starting server...");

    let root_handle = FileActorHandle::new().await;
    let proto = ProtoHandler::new(root_handle);

    let addr = "[::1]:50051".parse()?;
    Server::builder()
        .add_service(CrabDbServer::new(proto))
        .serve(addr)
        .await?;

    Ok(())
}

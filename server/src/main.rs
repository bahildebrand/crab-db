mod file;
mod protocol;

use bytes::BytesMut;
use file::actor::FileActorHandle;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tonic::transport::Server;
use protocol::ProtoHandler;
use protocol::crab_db::crab_db_server::CrabDbServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let proto = ProtoHandler::default();
    Server::builder()
        .add_service(CrabDbServer::new(proto))
        .serve(addr)
        .await?;

    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    let root_handle = FileActorHandle::new();

    loop {
        let (mut socket, _) = listener.accept().await?;

        let handle = root_handle.clone();
        tokio::spawn(async move {
            // TODO: Parse these values from input JSON
            let mut buf = BytesMut::with_capacity(1024);
            let key = String::from("key");

            loop {
                let n = match socket.read_buf(&mut buf).await {
                    Ok(n) if n == 0 => {
                        println!("Empty message");
                        return;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };

                handle.write_data(key.clone(), buf.clone()).await;
            }
        });
    }
}

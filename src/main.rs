mod file;

use bytes::BytesMut;
use file::actor::FileActorHandle;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    let root_handle = FileActorHandle::new();

    loop {
        let (mut socket, _) = listener.accept().await?;

        let handle = root_handle.clone();
        tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(1024);

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

                handle.write_data(buf.clone()).await;
            }
        });
    }
}

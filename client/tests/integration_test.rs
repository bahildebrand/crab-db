use client::CrabClient;
use rand::prelude::*;

#[tokio::test]
async fn test_write() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CrabClient::new("http://[::1]:50051".into()).await;

    let mut rng = thread_rng();
    let key = format!("key-{}", rng.gen_range(0..1024));
    let data = format!("data-{}", rng.gen_range(0..1024));


    client.write(key, data.into()).await?;

    Ok(())
}

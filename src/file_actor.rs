use bytes::BytesMut;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};

struct FileActor {
    receiver: mpsc::Receiver<FileActorMessage>,
}
enum FileActorMessage {
    WriteData {
        respond_to: oneshot::Sender<u32>,
        data: BytesMut,
    },
}

impl FileActor {
    fn new(receiver: mpsc::Receiver<FileActorMessage>) -> Self {
        FileActor { receiver }
    }
    async fn handle_message(
        &mut self,
        msg: FileActorMessage,
        mut db_file: File,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match msg {
            FileActorMessage::WriteData { respond_to, data } => {
                db_file.write_all(&data[..]).await?;
                // TODO: Turn this into an error response
                respond_to.send(0);
            }
        }

        Ok(())
    }
}

async fn run_file_actor(
    mut actor: FileActor,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    while let Some(msg) = actor.receiver.recv().await {
        let mut file = File::create("foo.txt").await?;

        actor.handle_message(msg, file).await?;
    }

    Ok(())
}

#[derive(Clone)]
pub(crate) struct FileActorHandle {
    sender: mpsc::Sender<FileActorMessage>,
}

impl FileActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let actor = FileActor::new(receiver);
        tokio::spawn(run_file_actor(actor));

        Self { sender }
    }

    pub async fn write_data(&self, data: BytesMut) -> u32 {
        let (send, recv) = oneshot::channel();
        let msg = FileActorMessage::WriteData {
            respond_to: send,
            data: data,
        };

        println!("Writing data...");
        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

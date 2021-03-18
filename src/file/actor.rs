use crate::file::record::Record;
use bytes::BytesMut;
use tokio::fs::OpenOptions;
use tokio::sync::{mpsc, oneshot};

struct FileActor {
    receiver: mpsc::Receiver<FileActorMessage>,
}
enum FileActorMessage {
    WriteData {
        respond_to: oneshot::Sender<u32>,
        key: String,
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
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match msg {
            FileActorMessage::WriteData { respond_to, key, data } => {
                let mut record = Record::new("record.csv").await;

                record.write_record(key, data).await?;
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
    let shard_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open("shard.csv")
        .await?;

    while let Some(msg) = actor.receiver.recv().await {
        actor.handle_message(msg).await?;
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

    pub async fn write_data(&self, key: String, data: BytesMut) -> u32 {
        let (send, recv) = oneshot::channel();
        let msg = FileActorMessage::WriteData {
            respond_to: send,
            key,
            data: data,
        };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

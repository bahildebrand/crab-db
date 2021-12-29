use crate::file::segment::SegmentManager;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
struct FileActor {
    receiver: mpsc::Receiver<FileActorMessage>,
    segment_manager: SegmentManager,
}

#[derive(Debug)]
pub enum ReadError {
    KeyNotFound,
}

pub type ReadResult = Result<Bytes, ReadError>;

#[derive(Debug)]
enum FileActorMessage {
    WriteData {
        respond_to: oneshot::Sender<u32>,
        key: String,
        data: Bytes,
    },
    ReadData {
        respond_to: oneshot::Sender<ReadResult>,
        key: String,
    },
}

impl FileActor {
    async fn new(receiver: mpsc::Receiver<FileActorMessage>) -> Self {
        FileActor {
            receiver,
            segment_manager: SegmentManager::new("segment_map").await,
        }
    }

    async fn handle_message(
        &mut self,
        msg: FileActorMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match msg {
            FileActorMessage::WriteData {
                respond_to,
                key,
                data,
            } => {
                self.segment_manager.put(key, data).await?;

                // TODO: Turn this into an error response
                respond_to.send(0).unwrap();
            }
            FileActorMessage::ReadData { respond_to, key } => {
                let read = self.segment_manager.get(key).await?;

                match read {
                    Some(data) => {
                        respond_to.send(Ok(data)).unwrap();
                    }
                    None => {
                        respond_to.send(Err(ReadError::KeyNotFound)).unwrap();
                    }
                }
            }
        }

        Ok(())
    }
}

async fn run_file_actor(
    mut actor: FileActor,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
    pub async fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let actor = FileActor::new(receiver).await;
        tokio::spawn(run_file_actor(actor));

        Self { sender }
    }

    pub async fn write_data(&self, key: String, data: Bytes) -> u32 {
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

    pub async fn read_data(&self, key: String) -> ReadResult {
        let (send, recv) = oneshot::channel();
        let msg = FileActorMessage::ReadData {
            respond_to: send,
            key,
        };

        let _ = self.sender.send(msg).await;
        let response = recv.await.expect("Actor task has been killed");

        // TODO: This whole section is braindead, please fix it.
        match response {
            Ok(data) => Ok(data),
            Err(e) => Err(e),
        }
    }
}

use crate::file::Writer;
use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

type Record = (DateTime<Utc>, String, String);
enum WriteCommand {
    Record(Record),
    Finish,
}

#[derive(Clone)]
pub struct WriterSender(mpsc::Sender<WriteCommand>);
impl WriterSender {
    pub async fn send(&self, record: Record) -> Result<(), anyhow::Error> {
        self.0
            .send(WriteCommand::Record(record))
            .await
            .map_err(|_| anyhow::anyhow!("collector writer stopped"))
    }
    pub async fn finish(&self) -> Result<(), anyhow::Error> {
        self.0
            .send(WriteCommand::Finish)
            .await
            .map_err(|_| anyhow::anyhow!("collector writer stopped"))
    }
}

pub fn start(
    path: String,
    capacity: usize,
) -> (
    WriterSender,
    tokio::task::JoinHandle<Result<(), anyhow::Error>>,
) {
    let (tx, mut rx) = mpsc::channel(capacity);
    let worker = tokio::task::spawn_blocking(move || {
        let mut writer = Writer::new(&path);
        let mut result = Ok(());
        while let Some(command) = rx.blocking_recv() {
            match command {
                WriteCommand::Record((time, symbol, data)) => {
                    if let Err(error) = writer.write(time, symbol, data) {
                        result = Err(error);
                        rx.close();
                        break;
                    }
                }
                WriteCommand::Finish => rx.close(),
            }
        }
        let finished = writer.finish().map_err(anyhow::Error::from);
        result.and(finished)
    });
    (WriterSender(tx), worker)
}

pub struct AbortOnDrop<T>(pub tokio::task::JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    #[tokio::test]
    async fn bounded_writer_drains_rotates_and_appends_gzip_members() {
        let root = std::env::temp_dir().join(format!(
            "titan-collector-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let now = Utc::now();
        for batch in 0..2 {
            let (sender, worker) = start(root.to_string_lossy().into(), 1);
            for id in 0..20 {
                sender
                    .send((now, "BTC".into(), format!("{batch}:{id}")))
                    .await
                    .unwrap();
            }
            if batch == 1 {
                sender
                    .send((
                        now + chrono::Duration::days(1),
                        "BTC".into(),
                        "next-day".into(),
                    ))
                    .await
                    .unwrap();
            }
            sender.finish().await.unwrap();
            worker.await.unwrap().unwrap();
            assert!(
                sender
                    .send((now, "BTC".into(), "late".into()))
                    .await
                    .is_err()
            );
        }
        let path = root.join(format!("btc_{}.gz", now.format("%Y%m%d")));
        let mut decoder = flate2::read::MultiGzDecoder::new(std::fs::File::open(path).unwrap());
        let mut text = String::new();
        decoder.read_to_string(&mut text).unwrap();
        assert_eq!(text.lines().count(), 40);
        assert!(text.contains("0:0") && text.contains("1:19"));
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn writer_io_failure_is_returned_to_owner() {
        let missing = std::env::temp_dir()
            .join(format!(
                "titan-missing-{}",
                Utc::now().timestamp_nanos_opt().unwrap()
            ))
            .join("no-directory");
        let (sender, worker) = start(missing.to_string_lossy().into(), 1);
        sender
            .send((Utc::now(), "BTC".into(), "data".into()))
            .await
            .unwrap();
        assert!(worker.await.unwrap().is_err());
    }
}

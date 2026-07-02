//! Write-ahead log: length-prefixed, crc32-checked msgpack frames.
//!
//! Format per frame: `[u32 len][u32 crc32(body)][body: rmp(Record)]`.
//! Replay stops at the first torn/corrupt frame (crash tail), which is the
//! correct recovery semantic for an append-only log.
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::Path;

use crate::error::{GirderError, Result};
use crate::record::Record;

/// When to fsync the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// fsync after every append batch (max durability).
    Always,
    /// fsync after every N appended records.
    EveryN(u32),
    /// Never fsync explicitly (OS decides; fastest, weakest).
    Os,
}

pub struct Wal {
    file: File,
    policy: FsyncPolicy,
    since_sync: u32,
    pub appended: u64,
}

impl Wal {
    pub fn open(path: &Path, policy: FsyncPolicy) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Wal {
            file,
            policy,
            since_sync: 0,
            appended: 0,
        })
    }

    pub fn append_batch(&mut self, records: &[Record]) -> Result<()> {
        let mut buf = Vec::with_capacity(records.len() * 256);
        for record in records {
            let body =
                rmp_serde::to_vec(record).map_err(|e| GirderError::Encode(e.to_string()))?;
            let crc = crc32fast::hash(&body);
            buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
            buf.extend_from_slice(&crc.to_le_bytes());
            buf.extend_from_slice(&body);
        }
        self.file.write_all(&buf)?;
        self.appended += records.len() as u64;
        self.since_sync += records.len() as u32;
        match self.policy {
            FsyncPolicy::Always => {
                self.file.sync_data()?;
                self.since_sync = 0;
            }
            FsyncPolicy::EveryN(n) if self.since_sync >= n => {
                self.file.sync_data()?;
                self.since_sync = 0;
            }
            _ => {}
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        self.since_sync = 0;
        Ok(())
    }

    /// Replay every intact frame; a torn/corrupt tail ends replay cleanly.
    pub fn replay(path: &Path) -> Result<Vec<Record>> {
        let Ok(file) = File::open(path) else {
            return Ok(Vec::new());
        };
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        loop {
            let mut header = [0u8; 8];
            match reader.read_exact(&mut header) {
                Ok(()) => {}
                Err(_) => break, // clean EOF or torn header
            }
            let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
            if len > 64 * 1024 * 1024 {
                tracing::warn!(len, "wal frame length implausible; stopping replay");
                break;
            }
            let mut body = vec![0u8; len];
            if reader.read_exact(&mut body).is_err() {
                break; // torn body
            }
            if crc32fast::hash(&body) != crc {
                tracing::warn!("wal crc mismatch; stopping replay at corrupt frame");
                break;
            }
            match rmp_serde::from_slice::<Record>(&body) {
                Ok(record) => records.push(record),
                Err(err) => {
                    tracing::warn!(%err, "wal frame undecodable; stopping replay");
                    break;
                }
            }
        }
        Ok(records)
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(i: usize) -> Record {
        Record {
            key: format!("k{i}"),
            timestamp: i as i64,
            labels: Default::default(),
            numerics: Default::default(),
            payload: vec![1, 2, 3],
        }
    }

    #[test]
    fn roundtrips_and_survives_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let mut wal = Wal::open(&path, FsyncPolicy::Always).unwrap();
            wal.append_batch(&[record(0), record(1)]).unwrap();
            wal.append_batch(&[record(2)]).unwrap();
        }
        // Simulate a crash mid-append: garbage tail.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[42, 0, 0, 0, 9, 9]).unwrap(); // torn header
        }
        let replayed = Wal::replay(&path).unwrap();
        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[2].key, "k2");
    }

    #[test]
    fn corrupt_frame_stops_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let mut wal = Wal::open(&path, FsyncPolicy::Always).unwrap();
            wal.append_batch(&[record(0)]).unwrap();
            wal.append_batch(&[record(1)]).unwrap();
        }
        // Flip a byte inside the second frame's body.
        {
            let mut bytes = std::fs::read(&path).unwrap();
            let n = bytes.len();
            bytes[n - 2] ^= 0xFF;
            std::fs::write(&path, bytes).unwrap();
        }
        let replayed = Wal::replay(&path).unwrap();
        assert_eq!(replayed.len(), 1);
    }
}

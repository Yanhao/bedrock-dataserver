use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::slice;

use anyhow::{anyhow, bail, Result};
use log::error;
use prost::Message;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::error::WalError;

use dataserver::replog_pb::Entry;

const MAX_META_COUNT: u64 = 1024;
const WAL_MAGIC: u64 = 0x12345;

#[repr(C, packed)]
pub struct WalEntryMeta {
    pub entry_offset: u64,
    pub version: u64,
} // 16 byte fixed

#[repr(C, packed)]
pub struct WalEntryHeader {
    pub offset: u64,
    pub prev_entry_offset: u64,
    pub version: u64,

    pub entry_length: u64,

    pub padding_: [u8; 128 - 48],

    pub checksum: u64,
} // 128 byte fixed

#[repr(C, packed)]
pub struct WalHeader {
    pub magic: u64,
    pub format: u8,
    pub first_entry_offset: u64, // always 4096

    pub padding_: [u8; 4096 - 17],
} // 4k fixed

#[repr(C, packed)]
pub struct WalFooter {
    pub entry_index_offset: u64,
    pub entry_count: u32,

    pub start_version: u64,
    pub end_version: u64,

    pub padding_: [u8; 4096 - 28],
} // 4k fixed

pub struct WalFile {
    pub header: Box<WalHeader>,
    pub wal_entry_index: Vec<WalEntryMeta>,
    pub footer: Option<Box<WalFooter>>,

    pub file: File,
    pub path: PathBuf,

    pub next_entry_offset: u64,

    pub sealed: bool,

    pub start_version: u64,
    pub end_version: u64,
}

impl WalFile {
    pub async fn load_wal_file(path: impl AsRef<Path>) -> Result<WalFile> {
        let mut file = match OpenOptions::new()
            .create_new(false)
            .write(true)
            .read(true)
            .open(path.as_ref())
            .await
        {
            Err(e) => {
                error!("failed to open wal file: {:?}, err: {:?}", path.as_ref(), e);
                bail!(WalError::FailedToOpen);
            }
            Ok(v) => v,
        };

        let mut header: WalHeader = unsafe { std::mem::MaybeUninit::uninit().assume_init() };
        let data = unsafe {
            slice::from_raw_parts_mut(
                &mut header as *mut _ as *mut u8,
                std::mem::size_of::<WalHeader>(),
            )
        };
        if let Err(e) = file.read(data).await {
            error!(
                "failed to read wal header, wal file: {:?}, err: {:?}",
                path.as_ref(),
                e
            );
            bail!(WalError::FailedToRead);
        }

        let mut metas = vec![];
        let mut wal_entry_offset = header.first_entry_offset;

        let mut sealed = false;

        loop {
            file.seek(SeekFrom::Start(wal_entry_offset))
                .await
                .map_err(|_| anyhow!(WalError::FailedToSeek))?;

            let mut entry_header: WalEntryHeader =
                unsafe { std::mem::MaybeUninit::uninit().assume_init() };
            let data = unsafe {
                slice::from_raw_parts_mut(
                    &mut entry_header as *mut _ as *mut u8,
                    std::mem::size_of::<WalEntryHeader>(),
                )
            };

            match file.read(data).await {
                Err(e) => {
                    error!(
                        "failed to read wal entry header at {:?}, err: {:?}",
                        wal_entry_offset, e
                    );
                    bail!(WalError::FailedToRead);
                }
                Ok(v) => {
                    if v == 0 {
                        sealed = false;
                        break;
                    }
                }
            }

            metas.push(WalEntryMeta {
                version: entry_header.version,
                entry_offset: entry_header.offset,
            });

            wal_entry_offset += std::mem::size_of::<WalEntryHeader>() as u64;
        }

        if !sealed {
            return Ok(Self {
                header: Box::new(header),
                footer: None,

                sealed,
                file,
                path: path.as_ref().to_path_buf(),

                next_entry_offset: wal_entry_offset,

                start_version: metas.first().unwrap().version,
                end_version: metas.last().unwrap().version,

                wal_entry_index: metas,
            });
        }

        let file_size = file.metadata().await.unwrap().len();
        let footer_offset = file_size - 4096;

        let footer: WalFooter = unsafe { std::mem::MaybeUninit::uninit().assume_init() };
        let data = unsafe {
            slice::from_raw_parts_mut(
                &mut header as *mut _ as *mut u8,
                std::mem::size_of::<WalFooter>(),
            )
        };

        file.seek(SeekFrom::Start(footer_offset))
            .await
            .map_err(|_| anyhow!(WalError::FailedToSeek))?;

        if let Err(e) = file.read(data).await {
            error!(
                "failed to read wal header, wal file: {:?}, err: {:?}",
                path.as_ref(),
                e
            );
            bail!(WalError::FailedToRead);
        }

        Ok(Self {
            header: Box::new(header),

            file,
            path: path.as_ref().to_path_buf(),

            next_entry_offset: wal_entry_offset,
            sealed,
            start_version: footer.start_version,
            end_version: footer.end_version,

            footer: Some(Box::new(footer)),
            wal_entry_index: metas,
        })
    }

    pub async fn append_entry(&mut self, ent: Entry) -> Result<()> {
        let meta = WalEntryMeta {
            entry_offset: self.next_entry_offset,
            version: ent.index,
        };

        let entry_header = WalEntryHeader {
            offset: meta.entry_offset,
            entry_length: ent.encoded_len() as u64,

            version: ent.index,
            checksum: 0,
            prev_entry_offset: 1,
            padding_: [0; 80],
        };

        let mut entry_buf = vec![];

        let entry_header_data: &[u8] = unsafe {
            slice::from_raw_parts(
                &entry_header as *const _ as *const u8,
                std::mem::size_of::<WalEntryHeader>(),
            )
        };

        // TODO: use writev to eliminate copy
        entry_buf.append(&mut entry_header_data.to_owned());

        let mut buf = Vec::new();
        ent.encode(&mut buf).unwrap();
        entry_buf.append(&mut buf);

        self.next_entry_offset += entry_buf.len() as u64;
        self.file
            .seek(SeekFrom::Start(meta.entry_offset))
            .await
            .map_err(|_| anyhow!(WalError::FailedToSeek))?;

        if let Err(e) = self.file.write(&entry_buf).await {
            error!("");
            bail!(WalError::FailedToWrite);
        }
        self.wal_entry_index.push(meta);

        Ok(())
    }

    pub async fn create_new_wal_file(path: impl AsRef<Path>) -> Result<WalFile> {
        if path.as_ref().exists() {
            error!("the wal file already exists!");
            bail!(WalError::FileExists);
        }

        let mut file = match OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path.as_ref().to_owned())
            .await
        {
            Err(e) => {
                bail!(WalError::FailedToCreateFile)
            }
            Ok(v) => v,
        };

        let wal_header = WalHeader {
            magic: WAL_MAGIC,
            format: 0x1,
            first_entry_offset: 4096,
            padding_: [0; 4079],
        };

        let wal_header_as_data = unsafe {
            slice::from_raw_parts(
                &wal_header as *const _ as *const u8,
                std::mem::size_of::<WalHeader>(),
            )
        };

        if let Err(e) = file.write(wal_header_as_data).await {
            error!("failed to write wal file header");
            bail!(WalError::FailedToWrite);
        }

        Ok(WalFile {
            header: Box::new(wal_header),
            footer: None,
            file,
            path: path.as_ref().into(),
            wal_entry_index: vec![],
            next_entry_offset: 4096,
            sealed: false,
            start_version: 0,
            end_version: 0,
        })
    }

    pub async fn seal(&mut self) -> Result<()> {
        let l = self.wal_entry_index.len();

        let metas_data = unsafe {
            slice::from_raw_parts(
                self.wal_entry_index.as_slice() as *const _ as *const u8,
                std::mem::size_of::<WalEntryMeta>() * l,
            )
        };

        self.file
            .seek(SeekFrom::Start(self.next_entry_offset))
            .await
            .map_err(|_| anyhow!(WalError::FailedToSeek))?;

        if let Err(e) = self.file.write(metas_data).await {
            error!("failed to write wal entry index");
            bail!(WalError::FailedToWrite);
        }

        let footer_offset =
            self.next_entry_offset + std::mem::size_of::<WalEntryMeta>() as u64 * (l as u64);

        let footer = WalFooter {
            entry_index_offset: self.next_entry_offset,
            entry_count: self.wal_entry_index.len() as u32,

            start_version: self.wal_entry_index[0].version,
            end_version: self.wal_entry_index[l - 1].version,

            padding_: [0; 4068],
        };

        let footer_data = unsafe {
            slice::from_raw_parts(
                &footer as *const _ as *const u8,
                std::mem::size_of::<WalFooter>(),
            )
        };

        self.file
            .seek(SeekFrom::Start(footer_offset))
            .await
            .map_err(|_| anyhow!(WalError::FailedToSeek))?;

        if let Err(e) = self.file.write(footer_data).await {
            error!("failed to write wal footer, err: {:?}", e);
            bail!(WalError::FailedToWrite);
        }

        Ok(())
    }

    pub fn first_version(&self) -> u64 {
        self.start_version
    }

    pub fn last_version(&self) -> u64 {
        self.end_version
    }

    pub fn suffix(&self) -> u64 {
        let path = self.path.as_os_str().to_str().unwrap();
        let suffix_str = path.split(".").last().unwrap();
        let suffix = suffix_str.parse().unwrap();
        suffix
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    pub fn entry_len(&self) -> u64 {
        self.wal_entry_index.len() as u64
    }

    pub async fn entries(&mut self, start_version: u64, end_version: u64) -> Result<Vec<Entry>> {
        if start_version < self.start_version || end_version > self.end_version {
            bail!(WalError::InvalidParameter);
        }

        let start_index = start_version - self.start_version;
        let end_index = self.end_version - end_version;

        let target_meta = &(self.wal_entry_index)[start_index as usize..(end_index + 1) as usize];

        let mut ret = vec![];
        for m in target_meta.iter() {
            self.file
                .seek(SeekFrom::Start(m.entry_offset))
                .await
                .unwrap(); // TODO error

            let mut buf = vec![];
            buf.resize(std::mem::size_of::<WalEntryMeta>(), 0);
            self.file.read(&mut buf).await.unwrap();

            let header_len = std::mem::size_of::<WalEntryHeader>();
            let entry_header: WalEntryHeader =
                unsafe { std::ptr::read(buf[0..header_len].as_ptr() as *const _) };

            let ent =
                Entry::decode(&buf[header_len..header_len + entry_header.entry_length as usize])
                    .unwrap();

            ret.push(ent);
        }

        Ok(ret)
    }
}

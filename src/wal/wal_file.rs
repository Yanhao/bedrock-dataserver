use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::slice;

use anyhow::{anyhow, bail, Result};
use derivative::Derivative;
use prost::Message;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, error, info};

use idl_gen::replog_pb::Entry;

use crate::wal::WalError;

const MAX_META_COUNT: u64 = 10;
const WAL_MAGIC: u64 = 0x12345;

#[derive(Copy, Clone)]
#[repr(C, packed)]
pub struct WalEntryMeta {
    pub entry_offset: u64,
    pub entry_length: u64,
    pub index: u64,
} // 16 byte fixed

#[derive(Derivative)]
#[derivative(Debug)]
#[repr(C, packed)]
pub struct WalEntryHeader {
    pub offset: u64,
    pub prev_entry_offset: u64,
    pub index: u64,

    pub entry_length: u64,

    #[derivative(Debug = "ignore")]
    pub padding_: [u8; 128 - 48],

    pub checksum: u64,
} // 128 byte fixed

#[derive(Derivative)]
#[derivative(Debug)]
#[repr(C, packed)]
pub struct WalHeader {
    pub magic: u64,
    pub format: u8,
    pub first_entry_offset: u64, // always 4096

    #[derivative(Debug = "ignore")]
    pub padding_: [u8; 4096 - 17],
} // 4k fixed

#[derive(Derivative)]
#[derivative(Debug)]
#[repr(C, packed)]
pub struct WalFooter {
    pub entry_index_offset: u64,
    pub entry_count: u32,

    pub start_index: u64,
    pub end_index: u64,

    #[derivative(Debug = "ignore")]
    pub padding_: [u8; 4096 - 28],
} // 4k fixed

pub struct WalFile {
    pub header: Box<WalHeader>,
    pub wal_entry_metas: Vec<WalEntryMeta>,
    pub footer: Option<Box<WalFooter>>,

    pub file: File,
    pub path: PathBuf,

    pub next_entry_offset: u64,

    pub sealed: bool,

    pub start_index: u64,
    pub next_index: u64,
}

impl WalFile {
    pub async fn load_wal_file_header(file: &mut File) -> Result<WalHeader> {
        let mut header: WalHeader = unsafe { std::mem::zeroed() };
        let data = unsafe {
            slice::from_raw_parts_mut(
                &mut header as *mut _ as *mut u8,
                std::mem::size_of::<WalHeader>(),
            )
        };
        if let Err(e) = file.read(data).await {
            error!("failed to read wal header, err: {:?}", e);
            bail!(WalError::FailedToRead);
        }

        Ok(header)
    }

    pub async fn load_sealed_wal_file(mut file: File, path: impl AsRef<Path>) -> Result<WalFile> {
        info!("load_sealed_wal_file at {:?}", path.as_ref());
        let header = Self::load_wal_file_header(&mut file).await?;

        let mut footer: WalFooter = unsafe { std::mem::zeroed() };
        let data = unsafe {
            slice::from_raw_parts_mut(
                &mut footer as *mut _ as *mut u8,
                std::mem::size_of::<WalFooter>(),
            )
        };
        debug!("footer length: {}", data.len());

        let footer_offset = file.metadata().await?.len() - 4096;

        file.seek(SeekFrom::Start(footer_offset)).await?;
        file.read(data).await?;

        info!("footer: {:?}", footer);

        let mut metas = {
            let meta = WalEntryMeta {
                entry_offset: 0,
                entry_length: 0,
                index: 0,
            };
            vec![meta; footer.entry_count as usize]
        };

        let metas_data = unsafe {
            slice::from_raw_parts_mut(
                metas.as_mut_ptr() as *mut u8,
                metas.len() * std::mem::size_of::<WalEntryMeta>(),
            )
        };
        file.seek(SeekFrom::Start(footer.entry_index_offset))
            .await
            .map_err(|_| anyhow!(WalError::FailedToSeek))?;

        if let Err(e) = file.read(metas_data).await {
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

            next_entry_offset: footer_offset,
            sealed: true,
            start_index: footer.start_index,
            next_index: footer.end_index + 1,

            footer: Some(Box::new(footer)),
            wal_entry_metas: metas,
        })
    }

    pub async fn load_unsealed_wal_file(mut file: File, path: impl AsRef<Path>) -> Result<WalFile> {
        info!("load_unsealed_wal_file at {:?}", path.as_ref());
        let header = Self::load_wal_file_header(&mut file).await?;
        info!("wal file header: {header:?}");
        let mut wal_entry_offset = header.first_entry_offset;

        let mut metas = vec![];
        let (mut current_entry_offset, mut current_entry_index) = (0, 0);
        loop {
            info!("wal_entry_offset: {}", wal_entry_offset);
            file.seek(SeekFrom::Start(wal_entry_offset)).await?;

            let mut entry_header: WalEntryHeader = unsafe { std::mem::zeroed() };
            let data = unsafe {
                slice::from_raw_parts_mut(
                    &mut entry_header as *mut _ as *mut u8,
                    std::mem::size_of::<WalEntryHeader>(),
                )
            };

            let sz = file.read(data).await.inspect_err(|e| {
                error!("failed to read wal entry header at {wal_entry_offset:?}, err: {e}")
            })?;
            info!("sz: {sz}");

            if sz == 0 {
                break;
            }
            if entry_header.offset <= current_entry_offset
                || entry_header.index <= current_entry_index
            {
                break;
            } else {
                (current_entry_offset, current_entry_index) =
                    (entry_header.offset, entry_header.index);
            }
            info!(
                "read entry header: offset: {}, index: {}",
                current_entry_offset, current_entry_index
            );

            metas.push(WalEntryMeta {
                index: entry_header.index,
                entry_offset: entry_header.offset,
                entry_length: entry_header.entry_length,
            });

            wal_entry_offset += std::mem::size_of::<WalEntryHeader>() as u64;
            wal_entry_offset += entry_header.entry_length;
        }

        Ok(Self {
            header: Box::new(header),
            footer: None,

            sealed: false,
            file,
            path: path.as_ref().to_path_buf(),

            next_entry_offset: wal_entry_offset,

            start_index: metas.first().unwrap().index,
            next_index: metas.last().unwrap().index + 1,

            wal_entry_metas: metas,
        })
    }

    pub async fn load_wal_file(path: impl AsRef<Path>, is_sealed: bool) -> Result<WalFile> {
        info!(
            "start load wal file, path: {}, is_sealed: {}",
            path.as_ref().display(),
            is_sealed
        );

        let file = match OpenOptions::new()
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

        if is_sealed {
            Self::load_sealed_wal_file(file, path).await
        } else {
            Self::load_unsealed_wal_file(file, path).await
        }
    }

    pub async fn append_entry(&mut self, ent: Entry) -> Result<()> {
        if self.entry_len() >= MAX_META_COUNT {
            info!("wal file is full, length: {}", self.entry_len());
            bail!(WalError::WalFileFull);
        }

        info!("wal file next_version before: {}", self.next_index);
        let meta = WalEntryMeta {
            entry_offset: self.next_entry_offset,
            entry_length: ent.encoded_len() as u64,
            index: self.next_index,
        };

        let entry_header = WalEntryHeader {
            offset: meta.entry_offset,
            entry_length: ent.encoded_len() as u64,

            index: self.next_index,
            checksum: 0,
            prev_entry_offset: 1,

            padding_: [0; 80],
        };

        {
            let a = entry_header.offset;
            let b = entry_header.index;
            info!("append meta header: offset: {}, index: {}", a, b);
        }

        let entry_header_data: &[u8] = unsafe {
            slice::from_raw_parts(
                &entry_header as *const _ as *const u8,
                std::mem::size_of::<WalEntryHeader>(),
            )
        };

        let entry_buf = {
            let mut buf = vec![];
            buf.append(&mut entry_header_data.to_owned());

            let mut buf2 = vec![];
            ent.encode(&mut buf2).unwrap();
            buf.append(&mut buf2);

            buf
        };

        // TODO: use pwritev to eliminate copy and seek
        self.next_entry_offset += entry_buf.len() as u64;
        self.file
            .seek(SeekFrom::Start(meta.entry_offset))
            .await
            .inspect_err(|e| error!("file seek to failed, err: {e}"))?;

        self.file
            .write(&entry_buf)
            .await
            .inspect_err(|e| error!("file write failed, err: {e}"))?;

        self.wal_entry_metas.push(meta);
        self.next_index += 1;
        info!("wal file next_version after: {}", self.next_index);

        Ok(())
    }

    pub async fn create_new_wal_file(
        path: impl AsRef<Path>,
        start_version: u64,
    ) -> Result<WalFile> {
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
                error!("create wal file failed, error: {e}");
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

        debug!("wal file header length: {}", wal_header_as_data.len());

        if let Err(e) = file.write(wal_header_as_data).await {
            error!("failed to write wal file header, err: {e}");
            bail!(WalError::FailedToWrite);
        }

        Ok(WalFile {
            header: Box::new(wal_header),
            footer: None,
            file,
            path: path.as_ref().into(),
            wal_entry_metas: vec![],
            next_entry_offset: 4096,
            sealed: false,
            start_index: start_version,
            next_index: start_version,
        })
    }

    pub async fn seal(&mut self) -> Result<()> {
        let l = self.wal_entry_metas.len();

        let metas_data = unsafe {
            slice::from_raw_parts(
                self.wal_entry_metas.as_slice() as *const _ as *const u8,
                std::mem::size_of::<WalEntryMeta>() * l,
            )
        };

        let entry_index_offset = self.next_entry_offset;
        self.file
            .seek(SeekFrom::Start(entry_index_offset))
            .await
            .map_err(|_| anyhow!(WalError::FailedToSeek))?;

        if let Err(e) = self.file.write(metas_data).await {
            error!("failed to write wal entry index, error: {e}");
            bail!(WalError::FailedToWrite);
        }

        let footer_offset =
            self.next_entry_offset + std::mem::size_of::<WalEntryMeta>() as u64 * (l as u64);

        let footer = WalFooter {
            entry_index_offset,
            entry_count: self.wal_entry_metas.len() as u32,

            start_index: self.wal_entry_metas[0].index,
            end_index: self.wal_entry_metas[l - 1].index,

            padding_: [0; 4068],
        };

        let footer_data = unsafe {
            slice::from_raw_parts(
                &footer as *const _ as *const u8,
                std::mem::size_of::<WalFooter>(),
            )
        };

        debug!("footer data length: {}", footer_data.len());

        self.file
            .seek(SeekFrom::Start(footer_offset))
            .await
            .map_err(|_| anyhow!(WalError::FailedToSeek))?;

        if let Err(e) = self.file.write(footer_data).await {
            error!("failed to write wal footer, err: {:?}", e);
            bail!(WalError::FailedToWrite);
        }

        self.sealed = true;

        Ok(())
    }

    pub fn first_version(&self) -> u64 {
        self.start_index
    }

    pub fn next_version(&self) -> u64 {
        self.next_index
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
        self.wal_entry_metas.len() as u64
    }

    pub async fn entries(
        &mut self,
        start_version: u64,
        end_version: u64, /* start_version..=end_version */
    ) -> Result<Vec<Entry>> {
        if start_version < self.start_index || end_version >= self.next_index {
            bail!(WalError::InvalidParameter);
        }

        let start_index = start_version - self.start_index;
        let end_index = start_index + (end_version - start_version);

        info!("start_index: {start_index}, end_index: {end_index}");
        let target_meta = &(self.wal_entry_metas)[start_index as usize..={ end_index as usize }];

        let mut ret = vec![];
        for m in target_meta.iter() {
            self.file.seek(SeekFrom::Start(m.entry_offset)).await?;

            let mut buf = vec![0; std::mem::size_of::<WalEntryHeader>() + m.entry_length as usize];
            self.file.read(&mut buf).await?;

            info!("buf len: {}", buf.len());

            let header_len = std::mem::size_of::<WalEntryHeader>();
            info!("header len: {}", header_len);
            let entry_header: WalEntryHeader =
                unsafe { std::ptr::read(buf[0..header_len].as_ptr() as *const _) };

            let ent =
                Entry::decode(&buf[header_len..header_len + entry_header.entry_length as usize])
                    .unwrap();

            ret.push(ent);
        }

        Ok(ret)
    }

    pub async fn discard(&mut self, next_version: u64) -> Result<()> {
        self.sealed = false;
        self.next_index = next_version;

        let entry_offset = if next_version == self.first_version() {
            4096
        } else {
            let index = next_version - self.first_version() - 1;
            self.wal_entry_metas[index as usize].entry_offset
        };

        self.next_entry_offset = entry_offset;
        self.footer = None;

        self.file.set_len(entry_offset).await?;

        Ok(())
    }
}

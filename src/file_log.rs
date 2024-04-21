use std::{
    collections::HashMap,
    io::{ErrorKind, Read, Seek, Write},
    path::Path,
};

pub type CacheCollection = HashMap<Vec<u8>, u64>;
const USIZE_BYTE_LEN: usize = (usize::BITS / 8) as usize;

pub enum LogOperation {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

pub struct Log {
    file: std::fs::File,
}

impl Log {
    pub fn new<S: AsRef<Path>>(directory: S) -> std::io::Result<Self> {
        if !directory.as_ref().is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Must enter directory",
            ));
        }
        std::fs::DirBuilder::new()
            .recursive(true)
            .create(directory.as_ref())?;

        let mut file_path = directory.as_ref().to_owned();

        file_path.push("operation.log");

        let file = std::fs::OpenOptions::new()
            .append(true)
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;

        Ok(Self { file })
    }

    pub fn construct_in_memory_cache(&mut self) -> std::io::Result<CacheCollection> {
        let mut action_buffer = [0u8; 1];
        let mut map = HashMap::new();

        let mut cursor = self.file.seek(std::io::SeekFrom::Start(0))?;

        loop {
            println!("Cursor: {cursor}");
            match self.file.read_exact(&mut action_buffer) {
                Ok(_) => (),
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };

            cursor += action_buffer.len() as u64;
            let action = action_buffer[0] as char;

            match action {
                'p' => {
                    let (key, value_cursor) = self.get_key(cursor)?;
                    let (_, new_cursor) = self.get_value(value_cursor)?;
                    cursor = new_cursor;
                    map.insert(key, value_cursor);
                }
                'd' => {
                    let (key, new_cursor) = self.get_key(cursor)?;
                    cursor = new_cursor;
                    map.remove(&key);
                }
                _ => {
                    return Err(std::io::Error::new(
                        ErrorKind::InvalidData,
                        "Invalid Action",
                    ))
                }
            }
        }

        Ok(map)
    }

    pub fn get_key(&mut self, key_start_position: u64) -> std::io::Result<(Vec<u8>, u64)> {
        let mut key_length_buffer = [0u8; USIZE_BYTE_LEN];

        let current_position = self.file.stream_position()?;

        if current_position != key_start_position {
            self.file.seek(std::io::SeekFrom::Current(
                key_start_position as i64 - current_position as i64,
            ))?;
        }

        let mut cursor = key_start_position;
        self.file.read_exact(&mut key_length_buffer)?;
        let key_length = usize::from_be_bytes(
            key_length_buffer[..key_length_buffer.len()]
                .try_into()
                .unwrap(),
        );
        cursor += key_length_buffer.len() as u64;

        let mut key = vec![0u8; key_length];

        self.file.read_exact(&mut key[..key_length])?;
        cursor += key_length as u64;

        Ok((key, cursor))
    }

    pub fn get_value(&mut self, value_start_position: u64) -> std::io::Result<(Vec<u8>, u64)> {
        let mut value_length_buffer = [0u8; USIZE_BYTE_LEN];
        let current_position = self.file.stream_position()?;

        if current_position != value_start_position {
            self.file.seek(std::io::SeekFrom::Current(
                value_start_position as i64 - current_position as i64,
            ))?;
        }

        let mut cursor = value_start_position;

        self.file.read_exact(&mut value_length_buffer)?;
        cursor += value_length_buffer.len() as u64;

        let value_length = usize::from_be_bytes(
            value_length_buffer[..value_length_buffer.len()]
                .try_into()
                .unwrap(),
        );

        let mut value = vec![0u8; value_length];
        self.file.read_exact(&mut value[..value_length])?;
        cursor += value_length as u64;

        Ok((value, cursor))
    }
}

impl Log {
    pub fn append(&mut self, mut op: LogOperation) -> std::io::Result<u64> {
        let capacity_needed = match &op {
            LogOperation::Put(key, value) => {
                1 + USIZE_BYTE_LEN + key.len() + USIZE_BYTE_LEN + value.len()
            }
            LogOperation::Delete(key) => 1 + 4 + key.len(),
        };

        let position = self.file.seek(std::io::SeekFrom::End(0))?;
        let new_position = position
            + match &op {
                LogOperation::Put(key, _) => 1 + USIZE_BYTE_LEN + key.len(),
                LogOperation::Delete(key) => 1 + USIZE_BYTE_LEN + key.len(),
            } as u64;

        let mut bytes = Vec::with_capacity(capacity_needed);

        match op {
            LogOperation::Put(ref mut key, ref mut value) => {
                bytes.push('p' as u8);
                bytes.extend_from_slice(&(key.len()).to_be_bytes());
                bytes.append(key);
                bytes.extend_from_slice(&(value.len()).to_be_bytes());
                bytes.append(value);
            }
            LogOperation::Delete(ref mut key) => {
                bytes.push('d' as u8);
                bytes.extend_from_slice(&(key.len()).to_be_bytes());
                bytes.append(key);
            }
        }

        self.file
            .write_all(&bytes)
            .and_then(|_| self.file.sync_data())?;

        Ok(new_position)
    }
}

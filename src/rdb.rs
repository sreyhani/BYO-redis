use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, Read},
};
// use byteorder::{LittleEndian, ReadBytesExt};

use anyhow::{anyhow, Ok, Result};

const RDB_MAGIC: &str = "REDIS";
const RDB_VERSIOIN: u32 = 9;
const STRING_VALUE: u8 = 0;

#[derive(Debug, PartialEq)]
pub struct RdbFile {
    pub key_vals: HashMap<String, String>,
}

pub fn read_rdb_file(path: String) -> Result<RdbFile> {
    let file = File::open(path)?;
    let reader = io::BufReader::new(file);
    parse(reader)
}
fn parse(mut reader: impl BufRead) -> Result<RdbFile> {
    read_header(&mut reader)?;
    let mut buf = vec![];
    reader.read_until(0xfb, &mut buf)?;
    let mut hash_size = [0];
    let mut expire_hash_size = [0];

    reader.read_exact(&mut hash_size)?;
    reader.read_exact(&mut expire_hash_size)?;

    let mut key_vals = HashMap::<String, String>::new();
    let hash_size = hash_size[0];
    for _ in 0..hash_size {
        let mut value_type = [0];
        reader.read_exact(&mut value_type)?;
        assert!(value_type[0] == STRING_VALUE);
        let mut key_string_size = [0];
        reader.read_exact(&mut key_string_size)?;
        let mut key = vec![0; key_string_size[0].into()];
        reader.read_exact(&mut key)?;
        let mut value_string_size = [0];
        reader.read_exact(&mut value_string_size)?;
        let mut value = vec![0; value_string_size[0].into()];
        reader.read_exact(&mut value)?;

        key_vals.insert(String::from_utf8(key)?, String::from_utf8(value)?);
    }

    Ok(RdbFile { key_vals })
}

fn read_header(mut reader: impl Read) -> Result<()> {
    check_magic(&mut reader)?;
    check_version(reader)?;
    Ok(())
}

fn check_magic(reader: &mut impl Read) -> Result<()> {
    let mut magic = [0; 5];
    reader.read_exact(&mut magic)?;
    Ok(if magic != RDB_MAGIC.as_bytes() {
        return Err(anyhow!("wrong magic"));
    })
}

fn check_version(mut reader: impl Read) -> Result<()> {
    let mut version_bytes = [0; 4];
    reader.read_exact(&mut version_bytes)?;
    let adjusted_bytes = [
        version_bytes[0] - 48,
        version_bytes[1] - 48,
        version_bytes[2] - 48,
        version_bytes[3] - 48,
    ];
    let version = u32::from_be_bytes(adjusted_bytes);
    Ok(if version != RDB_VERSIOIN {
        return Err(anyhow!("wrong version"));
    })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::rdb::read_header;

    #[test]
    fn should_fail_with_wrong_magic() {
        let data: &[u8] = b"REDICK0006";
        let cursor = Cursor::new(data);
        assert!(read_header(cursor).is_err());
    }

    #[test]
    fn should_fail_with_wrong_version() {
        let data: &[u8] = b"REDIS0005";
        let cursor = Cursor::new(data);
        assert!(read_header(cursor).is_err());
    }

    #[test]
    fn should_read_header() {
        let data: &[u8] = b"REDIS0009";
        let cursor = Cursor::new(data);
        assert!(read_header(cursor).is_ok());
    }
}

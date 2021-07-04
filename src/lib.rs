use std::{
    fmt,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use byteorder::{BigEndian, ReadBytesExt};
use fxhash::{FxHashMap, FxHashSet};
use thiserror::Error;

/// String interner used for efficiently reading atoms
pub trait Interner {
    type Atom;

    /// Intern an atom
    fn intern(&self, name: &str) -> Self::Atom;

    /// Efficiently intern many atoms at once.
    ///
    /// Can be useful to avoid repeated locking-unlocking when interning
    /// in a tight loop
    fn intern_many<'a>(&self, iter: impl Iterator<Item = &'a str>) -> Vec<Self::Atom> {
        iter.map(|name| self.intern(name)).collect()
    }
}

/// The identifier which indicates the type of a chunk.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Id([u8; 4]);

impl From<[u8; 4]> for Id {
    fn from(data: [u8; 4]) -> Self {
        Self(data)
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(str) => f.write_fmt(format_args!("b{:?}", str)),
            Err(_) => f.write_fmt(format_args!("{:?}", self.0)),
        }
    }
}

#[derive(Error, Debug)]
pub enum BeamFileError {
    #[error("Unexpected magic number {0:?}, expected b\"FOR1\"")]
    UnexpectedMagicNumber(Id),

    #[error("Unexpected form type {0:?}, expected b\"BEAM\"")]
    UnexpectedFormType(Id),

    #[error("Chunk {0:?} not found")]
    MissingChunk(Id),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, BeamFileError>;

#[derive(Clone, Debug)]
struct IndexEntry {
    position: u64,
    len: u64,
}

type Index = FxHashMap<Id, IndexEntry>;

#[derive(Clone)]
pub struct BeamFile<R> {
    reader: R,
    index: Index,
}

impl<R> fmt::Debug for BeamFile<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BeamFile")
            .field("chunks", &self.index.keys().collect::<FxHashSet<_>>())
            .finish()
    }
}

impl BeamFile<File> {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        Self::from_reader(file)
    }
}

impl<R: Read + Seek> BeamFile<R> {
    pub fn from_reader(mut reader: R) -> Result<Self> {
        let mut magic_number = [0; 4];
        reader.read_exact(&mut magic_number)?;

        if magic_number != *b"FOR1" {
            return Err(BeamFileError::UnexpectedMagicNumber(magic_number.into()));
        }

        let payload_size = reader.read_u32::<BigEndian>()? as u64;

        let mut type_id = [0; 4];
        reader.read_exact(&mut type_id)?;

        if type_id != *b"BEAM" {
            return Err(BeamFileError::UnexpectedFormType(type_id.into()));
        }

        let mut position = reader.seek(SeekFrom::Current(0))?;

        let mut index = Index::default();

        while position < payload_size {
            let mut chunk_id = [0; 4];
            reader.read_exact(&mut chunk_id)?;
            let chunk_len = reader.read_u32::<BigEndian>()? as u64;

            index.insert(
                chunk_id.into(),
                IndexEntry {
                    position: position + 8,
                    len: chunk_len,
                },
            );

            position = reader.seek(SeekFrom::Start(
                position + 8 + 4 * ((chunk_len + 4 - 1) / 4),
            ))?;
        }

        Ok(Self { reader, index })
    }

    pub fn read_raw(&mut self, id: Id) -> Result<Vec<u8>> {
        let entry = self.index.get(&id).ok_or(BeamFileError::MissingChunk(id))?;
        Self::read_entry(&mut self.reader, entry)
    }

    fn read_entry(reader: &mut R, entry: &IndexEntry) -> Result<Vec<u8>> {
        reader.seek(SeekFrom::Start(entry.position))?;

        let mut data = vec![0; entry.len as usize];
        reader.read_exact(&mut data)?;

        Ok(data)
    }

    pub fn iter_raw(&mut self) -> impl Iterator<Item = (Id, Result<Vec<u8>>)> + '_ {
        let reader = &mut self.reader;
        self.index
            .iter()
            .map(move |(id, entry)| (*id, Self::read_entry(reader, entry)))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

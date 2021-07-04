use std::{
    fmt,
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    path::Path,
    str,
};

use byteorder::{BigEndian, ReadBytesExt};
use fxhash::{FxHashMap, FxHashSet};
use thiserror::Error;

mod chunk;

pub use chunk::*;

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

/// A "naive" interner that just allocates the string
#[derive(Default)]
pub struct NaiveInterner;

impl Interner for NaiveInterner {
    type Atom = String;

    fn intern(&self, name: &str) -> Self::Atom {
        name.to_string()
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
        match str::from_utf8(&self.0) {
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

    #[error("Invalid atom")]
    InvalidAtom(#[from] str::Utf8Error),

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
pub struct BeamFile<R, I: Interner> {
    reader: R,
    index: Index,
    atom_index: Option<Vec<I::Atom>>,
}

impl<R, I: Interner> fmt::Debug for BeamFile<R, I>
where
    I::Atom: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BeamFile")
            .field("name", &self.name())
            .field("chunks", &self.index.keys().collect::<FxHashSet<_>>())
            .finish()
    }
}

impl<I: Interner> BeamFile<File, I> {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        Self::from_reader(file)
    }
}

impl<R: Read + Seek, I: Interner> BeamFile<R, I> {
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

        Ok(Self {
            reader,
            index,
            atom_index: None,
        })
    }

    /// Reads a structured chunk representation
    ///
    /// Panics if the atoms weren't index with `index_atoms`.
    pub fn read<C: Chunk<Atom = I::Atom> + Sized>(&mut self) -> Result<C> {
        let raw = self.read_raw(C::ID)?;
        let reader = Cursor::new(raw);
        let atom_index = self.atom_index.as_deref().unwrap();
        C::decode(reader, atom_index)
    }

    pub fn read_raw(&mut self, id: Id) -> Result<Vec<u8>> {
        let entry = self.index.get(&id).ok_or(BeamFileError::MissingChunk(id))?;
        Self::read_entry(&mut self.reader, entry)
    }

    pub fn iter_raw(&mut self) -> impl Iterator<Item = (Id, Result<Vec<u8>>)> + '_ {
        let reader = &mut self.reader;
        self.index
            .iter()
            .map(move |(id, entry)| (*id, Self::read_entry(reader, entry)))
    }

    fn read_entry(reader: &mut R, entry: &IndexEntry) -> Result<Vec<u8>> {
        reader.seek(SeekFrom::Start(entry.position))?;

        let mut data = vec![0; entry.len as usize];
        reader.read_exact(&mut data)?;

        Ok(data)
    }

    /// Decodes the atom chunk and stores the result for further processing
    pub fn index_atoms(&mut self, interner: I) -> Result<()> {
        let raw = match self.read_raw((*b"AtU8").into()) {
            Ok(raw) => raw,
            Err(BeamFileError::MissingChunk(_)) => self.read_raw((*b"Atom").into())?,
            Err(err) => return Err(err),
        };
        let mut reader = Cursor::new(raw);

        let count = reader.read_u32::<BigEndian>()? as usize;
        let mut atoms = Vec::with_capacity(count);

        let mut buf = Vec::new();

        for _ in 0..count {
            let len = reader.read_u8()? as usize;
            buf.resize(len, 0);
            reader.read_exact(&mut buf)?;
            let name = str::from_utf8(&buf)?;
            atoms.push(interner.intern(name));
        }

        self.atom_index = Some(atoms);

        Ok(())
    }
}

impl<R, I: Interner> BeamFile<R, I> {
    /// Returns the module name
    ///
    /// Returns `None` if atoms weren't indexed yet.
    /// Relies on the fact that the module name is the first atom in the atom table.
    pub fn name(&self) -> Option<&I::Atom> {
        self.atom_index.as_ref().and_then(|index| index.get(0))
    }

    pub fn atom_index(&self) -> Option<&[I::Atom]> {
        self.atom_index.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_atoms() {
        let mut file = BeamFile::<_, NaiveInterner>::from_file("fixtures/test.beam").unwrap();

        assert_eq!(file.name(), None);
        file.index_atoms(NaiveInterner::default()).unwrap();
        assert_eq!(file.name(), Some(&"test".to_string()));

        assert_eq!(file.atom_index().unwrap().len(), 4);
    }

    #[test]
    fn impt_chunk() {
        let mut file = BeamFile::<_, NaiveInterner>::from_file("fixtures/test.beam").unwrap();
        file.index_atoms(NaiveInterner::default()).unwrap();
        let chunk: ImpTChunk<String> = file.read().unwrap();

        assert_eq!(chunk.imports.len(), 2);
        assert_eq!(
            chunk.imports[0],
            Import {
                module: "erlang".to_string(),
                function: "get_module_info".to_string(),
                arity: 1,
            }
        );
        assert_eq!(
            chunk.imports[1],
            Import {
                module: "erlang".to_string(),
                function: "get_module_info".to_string(),
                arity: 2,
            }
        );
    }

    #[test]
    fn expt_chunk() {
        let mut file = BeamFile::<_, NaiveInterner>::from_file("fixtures/test.beam").unwrap();
        file.index_atoms(NaiveInterner::default()).unwrap();
        let chunk: ExpTChunk<String> = file.read().unwrap();

        assert_eq!(chunk.exports.len(), 2);
        assert_eq!(
            chunk.exports[0],
            Export {
                function: "module_info".to_string(),
                arity: 1,
                label: 4,
            }
        );
        assert_eq!(
            chunk.exports[1],
            Export {
                function: "module_info".to_string(),
                arity: 0,
                label: 2,
            }
        );
    }
}

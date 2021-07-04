use std::io::Read;

use byteorder::{BigEndian, ReadBytesExt};

use crate::{Result, Id};

pub trait Chunk {
    const ID: Id;
    type Atom: Clone;

    fn decode<R: Read>(reader: R, atom_index: &[Self::Atom]) -> Result<Self>
    where
        Self: Sized;
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Import<A> {
    pub module: A,
    pub function: A,
    pub arity: u32,
}

pub struct ImpTChunk<A> {
    pub imports: Vec<Import<A>>,
}

impl<A: Clone> Chunk for ImpTChunk<A> {
    const ID: Id = Id(*b"ImpT");
    type Atom = A;

    fn decode<R: Read>(mut reader: R, atom_index: &[A]) -> Result<Self> {
        let count = reader.read_u32::<BigEndian>()? as usize;
        let mut imports = Vec::with_capacity(count);

        for _ in 0..count {
            imports.push(Import {
                module: atom_index[reader.read_u32::<BigEndian>()? as usize - 1].clone(),
                function: atom_index[reader.read_u32::<BigEndian>()? as usize - 1].clone(),
                arity: reader.read_u32::<BigEndian>()?,
            })
        }

        Ok(ImpTChunk { imports })
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Export<A> {
    pub function: A,
    pub arity: u32,
    pub label: u32,
}

pub struct ExpTChunk<A> {
    pub exports: Vec<Export<A>>,
}

impl<A: Clone> Chunk for ExpTChunk<A> {
    const ID: Id = Id(*b"ExpT");
    type Atom = A;

    fn decode<R: Read>(mut reader: R, atom_index: &[A]) -> Result<Self> {
        let count = reader.read_u32::<BigEndian>()? as usize;
        let mut exports = Vec::with_capacity(count);

        for _ in 0..count {
            exports.push(Export {
                function: atom_index[reader.read_u32::<BigEndian>()? as usize - 1].clone(),
                arity: reader.read_u32::<BigEndian>()?,
                label: reader.read_u32::<BigEndian>()?,
            })
        }

        Ok(ExpTChunk { exports })
    }
}

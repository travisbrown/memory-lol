#![feature(cursor_remaining)]

use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use bzip2::read::MultiBzDecoder;
use futures::stream::{LocalBoxStream, StreamExt, TryStreamExt};
use memory_lol::error::Error;
use piz::ZipArchive;
use rocksdb::{IteratorMode, MergeOperands, Options, DB};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = std::env::args().collect::<Vec<_>>();
    let zip_file = File::open(&args[1])?;
    let mapping = unsafe { memmap::Mmap::map(&zip_file)? };
    let archive = ZipArchive::new(&mapping)?;

    let iter = ZipIter {
        archive,
        current: None,
        state: None,
    };

    for res in iter {
        let line = res?;

        println!("{}", line);
    }

    /*
    let ext = OsStr::new("bz2");
    let mut entries = Vec::with_capacity(1024);

    for entry in archive.entries() {
        if entry.is_file() && entry.path.extension() == Some(ext) {
            entries.push(entry);
        }
    }*/

    /*let s = stream(&args[1])?;

    s.try_for_each(|line| async move {
        println!("{}", line);

        Ok(())
    })
    .await?;*/

    Ok(())
}

struct ZipIter<'a> {
    archive: ZipArchive<'a>,
    current: Option<Box<dyn Iterator<Item = std::io::Result<String>> + 'a>>,
    state: Option<Vec<Box<dyn Read + Send + 'a>>>,
}

impl<'a> Iterator for ZipIter<'a> {
    type Item = std::io::Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut current = match self.current.take() {
            Some(current) => current,
            None => {
                let mut readers = match self.state.take() {
                    Some(readers) => readers,
                    None => {
                        let mut entries = Vec::with_capacity(1024);

                        for entry in self.archive.entries() {
                            if entry.is_file() && entry.path.extension() == Some(OsStr::new("bz2"))
                            {
                                entries.push(self.archive.read(entry).unwrap());
                            }
                        }

                        entries
                    }
                };

                let last = readers.pop();

                match last {
                    Some(reader) => {
                        self.state.insert(readers);
                        Box::new(BufReader::new(MultiBzDecoder::new(reader)).lines())
                    }
                    None => {
                        return None;
                    }
                }
            }
        };

        match current.next() {
            Some(value) => {
                self.current.insert(current);
                Some(value)
            }
            None => self.next(),
        }
    }
}

/*
fn stream<'a, P: AsRef<Path> + 'a>(
    path: P,
) -> Result<LocalBoxStream<'a, Result<String, Error>>, Error> {
    let ext = OsStr::new("bz2");

    /*let stream = futures::stream::iter(entries).map(move |entry| archive.read(entry).map_err(Error::from).map(|reader| {
        let buf = BufReader::new(MultiBzDecoder::new(reader));

        futures::stream::iter(buf.lines()).map_err(Error::from)
    })).try_flatten();

    Ok(stream.boxed())*/

    let stream = futures::stream::unfold(None, move |mut state| async move {
        let (mapping, archive, entries, index) = match state.take() {
            Some((mapping, archive, entries, index)) => (mapping, archive, entries, index),
            None => {
                let zip_file = File::open(path).unwrap();
                let mapping = unsafe { memmap::Mmap::map(&zip_file).unwrap() };
                let archive = ZipArchive::new(&mapping).unwrap();
                let mut entries = Vec::with_capacity(1024);

                for entry in archive.entries() {
                    if entry.is_file() && entry.path.extension() == Some(ext) {
                        entries.push(entry);
                    }
                }

                (mapping, archive, entries, 0)
            }
        };

        if index < entries.len() {
            let lines = archive
                .read(entries[index])
                .map_err(Error::from)
                .map(|reader| {
                    let buf = BufReader::new(MultiBzDecoder::new(reader));

                    futures::stream::iter(buf.lines()).map_err(Error::from)
                });

            Some((lines, Some((mapping, archive, entries, index + 1))))
        } else {
            None
        }
    })
    .try_flatten();

    Ok(stream.boxed_local())
}
*/

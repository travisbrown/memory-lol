#![feature(cursor_remaining)]

use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use memory_lol::error::Error;
use rocksdb::{MergeOperands, Options, DB};
use std::io::{Cursor, Read};
use std::path::Path;

fn main() -> Result<(), Error> {
    let args = std::env::args().collect::<Vec<_>>();
    let mut file = std::io::BufReader::new(std::fs::File::open(&args[1])?);

    let mut length_buf = [0; 4];
    let mut res = file.read_exact(&mut length_buf);
    let mut count = 0;

    let db = Mapping::new("output.db")?;

    while res.is_ok() {
        let length = u32::from_be_bytes(length_buf);
        //println!("{}", length);

        //println!("{:?}, {}, {}", length_buf, count, length);
        let mut key_buf = vec![0; length as usize];
        file.read_exact(&mut key_buf)?;

        file.read_exact(&mut length_buf)?;
        let length = u32::from_be_bytes(length_buf);
        let mut value_buf = vec![0; length as usize];
        file.read_exact(&mut value_buf)?;

        let mut cursor = Cursor::new(key_buf.clone());
        let tag = cursor.read_u8()?;
        let _id = if tag == 1 {
            0
        } else {
            cursor.read_u64::<BE>()?
        };
        //println!("{}, {}", tag, key_buf.len());

        db.db.merge(key_buf, value_buf)?;

        res = file.read_exact(&mut length_buf);
        count += 1;
    }

    println!("{}", count);

    Ok(())
}

pub struct Mapping {
    pub db: DB,
}

impl Mapping {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Mapping, Error> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_disable_auto_compactions(true);
        options.set_merge_operator_associative("merge", Self::merge);
        let db = DB::open(&options, path)?;

        Ok(Mapping { db })
    }

    fn merge(
        key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
    ) -> Option<Vec<u8>> {
        if key[0] == 0 {
            // || key[0] == 1 {
            //println!("HIT: {} {}", key.len(), existing_val.is_some());
            Some(Self::merge_longs(existing_val, operands))
        } else {
            //operands.last().map(|v| v.to_vec())
            None
        }
    }

    fn to_longs(bytes: &[u8]) -> Vec<u64> {
        let mut cursor = Cursor::new(bytes);
        let mut result = Vec::with_capacity(bytes.len() / 8);
        while !cursor.is_empty() {
            result.push(cursor.read_u64::<BE>().unwrap());
        }
        result
    }

    fn merge_longs(existing_val: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
        let mut result = Vec::with_capacity(operands.size_hint().0 * 8 + 8);

        if let Some(bytes) = existing_val {
            result.extend(Self::to_longs(bytes));
        }

        for operand in operands {
            result.extend(Self::to_longs(operand));
        }

        result.sort_unstable();
        result.dedup();

        let mut bytes = Vec::with_capacity(result.len() * 8);

        for r in result {
            bytes.write_u64::<BE>(r).unwrap();
        }
        /*if bytes.len() > 8 {
            println!("{}", bytes.len());
        }*/

        bytes
    }
}

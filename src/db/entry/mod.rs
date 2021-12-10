pub mod full_status;
pub mod screen_name;
pub mod user;

pub use full_status::FullStatusEntry;
pub use screen_name::ScreenNameEntry;
pub use user::UserEntry;

use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use chrono::{DateTime, Utc};
use std::collections::BinaryHeap;
use std::io::Cursor;

pub trait Entry {
    fn get_tag(&self) -> u8 {
        self.get_key()[0]
    }
    fn get_key(&self) -> &[u8];
    fn get_value(&self) -> &[u8];
    fn validate(&self) -> bool;

    fn raw<'a>(key: &'a [u8], value: &'a [u8]) -> Self
    where
        Self: Sized;

    fn parse<'a>(key: &'a [u8], value: &'a [u8]) -> Option<Self>
    where
        Self: Sized,
    {
        let raw = Self::raw(key, value);
        if raw.validate() {
            Some(raw)
        } else {
            None
        }
    }

    fn merge<'a, I: Iterator<Item = &'a [u8]>>(
        existing_value: Option<&'a [u8]>,
        operands: &'a mut I,
    ) -> (Option<Vec<u8>>, Option<MergeCollision>);
}

pub enum MergeCollision {
    UserId {
        previous: u64,
        update: u64,
    },
    Timestamp {
        previous: DateTime<Utc>,
        update: DateTime<Utc>,
    },
}

pub struct U64Iter<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl U64Iter<'_> {
    fn empty() -> U64Iter<'static> {
        U64Iter {
            cursor: Cursor::new(&[]),
        }
    }
}

impl Iterator for U64Iter<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        if self.cursor.is_empty() {
            None
        } else {
            Some(self.cursor.read_u64::<BE>().unwrap())
        }
    }
}

fn merge_sorted_u64s<'a, I: Iterator<Item = &'a [u8]>>(
    existing_value: Option<&'a [u8]>,
    operands: &'a mut I,
) -> Option<Vec<u8>> {
    let mut heap = BinaryHeap::with_capacity(operands.size_hint().0 + 1);

    if let Some(bytes) = existing_value {
        if bytes.len() % 8 == 0 {
            log::error!("Unexpected key length (not multiple of 8): {}", bytes.len());
        }
        let mut cursor = Cursor::new(bytes);
        while !cursor.is_empty() {
            heap.push(cursor.read_u64::<BE>().unwrap())
        }
    }

    for operand in operands {
        let mut cursor = Cursor::new(operand);
        while !cursor.is_empty() {
            heap.push(cursor.read_u64::<BE>().unwrap())
        }
    }

    let mut values = heap.into_sorted_vec();

    if values.is_empty() {
        None
    } else {
        values.dedup();

        let mut result = Vec::with_capacity(values.len() * 8);

        for value in values {
            result.write_u64::<BE>(value).unwrap();
        }

        Some(result)
    }
}

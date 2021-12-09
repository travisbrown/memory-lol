use super::Tag;
use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use std::collections::BinaryHeap;
use std::convert::TryInto;
use std::io::Cursor;

pub trait Entry {
    fn get_tag(&self) -> u8 {
        self.get_key()[0]
    }
    fn get_key(&self) -> &[u8];
    fn get_value(&self) -> &[u8];

    fn merge<'a, I: Iterator<Item = &'a [u8]>>(
        existing_value: Option<&'a [u8]>,
        operands: &'a mut I,
    ) -> Option<Vec<u8>>;
}

pub struct UserEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Entry for UserEntry {
    fn get_key(&self) -> &[u8] {
        &self.key
    }
    fn get_value(&self) -> &[u8] {
        &self.value
    }
    fn merge<'a, I: Iterator<Item = &'a [u8]>>(
        existing_value: Option<&'a [u8]>,
        operands: &'a mut I,
    ) -> Option<Vec<u8>> {
        merge_sorted_u64s(existing_value, operands)
    }
}

impl UserEntry {
    pub fn new<I: IntoIterator<Item = u64>>(
        user_id: u64,
        screen_name: &str,
        status_ids: I,
    ) -> UserEntry {
        let screen_name_bytes = screen_name.as_bytes();
        let mut key = Vec::with_capacity(screen_name_bytes.len() + 9);
        key.write_u8(Tag::ScreenName.value()).unwrap();
        key.write_u64::<BE>(user_id).unwrap();
        key.extend_from_slice(screen_name_bytes);

        let mut status_ids_vec = status_ids.into_iter().collect::<Vec<_>>();
        status_ids_vec.sort_unstable();
        status_ids_vec.dedup();

        let mut value = Vec::with_capacity(status_ids_vec.len() * 8);
        for status_id in status_ids_vec {
            value.write_u64::<BE>(status_id).unwrap();
        }

        UserEntry { key, value }
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
    values.dedup();

    let mut result = Vec::with_capacity(values.len() * 8);

    for value in values {
        result.write_u64::<BE>(value);
    }

    Some(result)
}

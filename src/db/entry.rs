use super::Tag;
use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use chrono::{DateTime, Utc};
use std::collections::BinaryHeap;
use std::convert::TryInto;
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
    ) -> Option<Vec<u8>>;
}

#[derive(Debug, PartialEq, Eq)]
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

    fn validate(&self) -> bool {
        let mut cursor = Cursor::new(&self.key);

        if let Ok(tag) = cursor.read_u8() {
            if tag == Tag::User.value() {
                if cursor.read_u64::<BE>().is_ok()
                    && std::str::from_utf8(cursor.remaining_slice()).is_ok()
                {
                    let mut cursor = Cursor::new(&self.value);
                    while !cursor.is_empty() {
                        if !cursor.read_u64::<BE>().is_ok() {
                            return false;
                        }
                    }
                    return true;
                }
            }
        }

        false
    }

    fn raw<'a>(key: &'a [u8], value: &'a [u8]) -> Self
    where
        Self: Sized,
    {
        Self {
            key: key.to_vec(),
            value: value.to_vec(),
        }
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
    ) -> Self {
        let screen_name_bytes = screen_name.as_bytes();
        let mut key = Vec::with_capacity(screen_name_bytes.len() + 9);
        key.write_u8(Tag::User.value()).unwrap();
        key.write_u64::<BE>(user_id).unwrap();
        key.extend_from_slice(screen_name_bytes);

        let mut status_ids_vec = status_ids.into_iter().collect::<Vec<_>>();
        status_ids_vec.sort_unstable();
        status_ids_vec.dedup();

        let mut value = Vec::with_capacity(status_ids_vec.len() * 8);
        for status_id in status_ids_vec {
            value.write_u64::<BE>(status_id).unwrap();
        }

        Self { key, value }
    }

    pub fn get_user_id(&self) -> u64 {
        u64::from_be_bytes(self.key[1..9].try_into().unwrap())
    }

    pub fn get_screen_name(&self) -> &str {
        std::str::from_utf8(&self.key[9..]).unwrap()
    }

    pub fn get_status_ids(&self) -> U64Iter {
        U64Iter {
            cursor: Cursor::new(&self.value),
        }
    }
}

#[derive(PartialEq, Eq)]
pub struct ScreenNameEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Entry for ScreenNameEntry {
    fn get_key(&self) -> &[u8] {
        &self.key
    }

    fn get_value(&self) -> &[u8] {
        &self.value
    }

    fn validate(&self) -> bool {
        true
    }

    fn raw<'a>(key: &'a [u8], value: &'a [u8]) -> Self
    where
        Self: Sized,
    {
        Self {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn merge<'a, I: Iterator<Item = &'a [u8]>>(
        existing_value: Option<&'a [u8]>,
        operands: &'a mut I,
    ) -> Option<Vec<u8>> {
        merge_sorted_u64s(existing_value, operands)
    }
}

impl ScreenNameEntry {
    pub fn new<I: IntoIterator<Item = u64>>(screen_name: &str, user_ids: I) -> Self {
        let screen_name_lc = screen_name.to_lowercase();
        let screen_name_bytes = screen_name_lc.as_bytes();
        let mut key = Vec::with_capacity(screen_name_bytes.len() + 1);
        key.write_u8(Tag::ScreenName.value()).unwrap();
        key.extend_from_slice(screen_name_bytes);

        let mut user_ids_vec = user_ids.into_iter().collect::<Vec<_>>();
        user_ids_vec.sort_unstable();
        user_ids_vec.dedup();

        let mut value = Vec::with_capacity(user_ids_vec.len() * 8);
        for user_id in user_ids_vec {
            value.write_u64::<BE>(user_id).unwrap();
        }

        Self { key, value }
    }
}

#[derive(PartialEq, Eq)]
pub struct FullStatusEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Entry for FullStatusEntry {
    fn get_key(&self) -> &[u8] {
        &self.key
    }

    fn get_value(&self) -> &[u8] {
        &self.value
    }

    fn validate(&self) -> bool {
        true
    }

    fn raw<'a>(key: &'a [u8], value: &'a [u8]) -> Self
    where
        Self: Sized,
    {
        Self {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn merge<'a, I: Iterator<Item = &'a [u8]>>(
        existing_value: Option<&'a [u8]>,
        operands: &'a mut I,
    ) -> Option<Vec<u8>> {
        todo![]
    }
}

impl FullStatusEntry {
    fn make_key(status_id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(9);
        key.write_u8(Tag::FullStatus.value()).unwrap();
        key.write_u64::<BE>(status_id).unwrap();
        key
    }

    pub fn new_retweet(
        status_id: u64,
        user_id: u64,
        timestamp: DateTime<Utc>,
        retweeted_id: u64,
    ) -> Self {
        let key = Self::make_key(status_id);
        let mut value = Vec::with_capacity(25);
        value.write_u8(4).unwrap();
        value.write_u64::<BE>(user_id);

        Self { key, value }
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
        result.write_u64::<BE>(value).unwrap();
    }

    Some(result)
}

pub struct U64Iter<'a> {
    cursor: Cursor<&'a [u8]>,
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

#[cfg(test)]
mod tests {
    use super::{Entry, UserEntry};
    use proptest::prelude::*;
    use std::collections::HashSet;

    proptest! {
      #[test]
      fn user_entry_round_trip(user_id: u64, screen_name: String, status_ids: HashSet<u64>) {
          let entry = UserEntry::new(user_id, &screen_name, status_ids.clone());
          assert_eq!(entry.validate(), true);
          assert_eq!(entry.get_user_id(), user_id);
          assert_eq!(entry.get_screen_name(), screen_name);
          assert_eq!(entry.get_status_ids().collect::<HashSet<_>>(), status_ids);
      }
    }
}

use super::{super::Tag, merge_sorted_u64s, Entry, U64Iter};
use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use chrono::{DateTime, Utc};
use std::io::Cursor;

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

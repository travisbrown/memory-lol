use super::{super::Tag, merge_sorted_u64s, Entry, MergeCollision, U64Iter};
use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use chrono::{DateTime, Utc};
use std::convert::TryInto;
use std::io::Cursor;

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
    ) -> (Option<Vec<u8>>, Option<MergeCollision>) {
        (merge_sorted_u64s(existing_value, operands), None)
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

#[cfg(test)]
mod tests {
    use super::{super::Entry, UserEntry};
    use proptest::prelude::*;
    use std::collections::HashSet;

    proptest! {
        #[test]
        fn round_trip(user_id: u64, screen_name: String, status_ids: HashSet<u64>) {
            let entry = UserEntry::new(user_id, &screen_name, status_ids.clone());
            assert_eq!(entry.validate(), true);
            assert_eq!(entry.get_user_id(), user_id);
            assert_eq!(entry.get_screen_name(), screen_name);
            assert_eq!(entry.get_status_ids().collect::<HashSet<_>>(), status_ids);
        }
    }
}

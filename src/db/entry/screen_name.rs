use super::{super::Tag, merge_sorted_u64s, Entry, MergeCollision, U64Iter};
use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use std::io::Cursor;

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
        let mut cursor = Cursor::new(&self.key);

        if let Ok(tag) = cursor.read_u8() {
            if tag == Tag::ScreenName.value()
                && std::str::from_utf8(cursor.remaining_slice()).is_ok()
            {
                let mut cursor = Cursor::new(&self.value);
                while !cursor.is_empty() {
                    if cursor.read_u64::<BE>().is_err() {
                        return false;
                    }
                }
                return true;
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

    pub fn get_screen_name(&self) -> &str {
        std::str::from_utf8(&self.key[1..]).unwrap()
    }

    pub fn get_user_ids(&self) -> U64Iter {
        U64Iter {
            cursor: Cursor::new(&self.value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{super::Entry, ScreenNameEntry};
    use proptest::prelude::*;
    use std::collections::HashSet;

    proptest! {
        #[test]
        fn round_trip(screen_name: String, user_ids: HashSet<u64>) {
            let entry = ScreenNameEntry::new(&screen_name, user_ids.clone());
            assert_eq!(entry.validate(), true);
            assert_eq!(entry.get_screen_name(), screen_name.to_lowercase());
            assert_eq!(entry.get_user_ids().collect::<HashSet<_>>(), user_ids);
        }
    }
}

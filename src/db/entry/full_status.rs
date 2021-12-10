use super::{super::Tag, Entry, MergeCollision, U64Iter};
use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use chrono::{DateTime, TimeZone, Utc};
use std::convert::TryInto;
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
        let mut cursor = Cursor::new(&self.key);

        if let Ok(tag) = cursor.read_u8() {
            if tag == Tag::FullStatus.value() {
                if cursor.read_u64::<BE>().is_ok()
                    && std::str::from_utf8(cursor.remaining_slice()).is_ok()
                {
                    let mut cursor = Cursor::new(&self.value);

                    if let Ok(tag) = cursor.read_u8() {
                        if tag == 4 {
                            return cursor.read_u64::<BE>().is_ok()
                                && cursor.read_u64::<BE>().is_ok()
                                && cursor.read_u64::<BE>().is_ok()
                                && cursor.is_empty();
                        } else {
                            while !cursor.is_empty() {
                                if !cursor.read_u64::<BE>().is_ok() {
                                    return false;
                                }
                            }
                            return true;
                        }
                    }
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
        let mut user_id = existing_value.map(Self::extract_user_id);
        let mut timestamp = existing_value.map(Self::extract_timestamp);
        let mut tag = existing_value.map(|bytes| bytes[0]);

        for operand in operands {
            let next_user_id = Self::extract_user_id(operand);
            let next_timestamp = Self::extract_timestamp(operand);
            let next_tag = operand[0];

            match user_id {
                Some(previous_user_id) => {
                    if previous_user_id != next_user_id {
                        return (
                            None,
                            Some(MergeCollision::UserId {
                                previous: previous_user_id,
                                update: next_user_id,
                            }),
                        );
                    }
                }
                None => {
                    user_id.insert(next_user_id);
                }
            }

            match timestamp {
                Some(previous_timestamp) => {
                    if previous_timestamp != next_timestamp {
                        return (
                            None,
                            Some(MergeCollision::Timestamp {
                                previous: previous_timestamp,
                                update: next_timestamp,
                            }),
                        );
                    }
                }
                None => {
                    timestamp.insert(next_timestamp);
                }
            }

            match tag {
                Some(previous_tag) => {
                    if previous_tag != next_timestamp {
                        return (
                            None,
                            Some(MergeCollision::Timestamp {
                                previous: previous_timestamp,
                                update: next_timestamp,
                            }),
                        );
                    }
                }
                None => {
                    tag.insert(next_tag);
                }
            }
        }
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
        retweeted_status_id: u64,
    ) -> Self {
        let key = Self::make_key(status_id);
        let mut value = Vec::with_capacity(25);
        value.write_u8(4).unwrap();
        value.write_u64::<BE>(user_id);
        value.write_u64::<BE>(timestamp.timestamp_millis() as u64);
        value.write_u64::<BE>(retweeted_status_id);

        Self { key, value }
    }

    pub fn new_tweet<I: IntoIterator<Item = u64>>(
        status_id: u64,
        user_id: u64,
        timestamp: DateTime<Utc>,
        replied_to_status_id: Option<u64>,
        quoted_status_id: Option<u64>,
        mentioned_user_ids: I,
    ) -> Self {
        let key = Self::make_key(status_id);

        let tag = match (replied_to_status_id, quoted_status_id) {
            (None, None) => 0,
            (Some(_), None) => 1,
            (None, Some(_)) => 2,
            (Some(_), Some(_)) => 3,
        };

        let mut value = Vec::with_capacity(25);
        value.write_u8(tag).unwrap();
        value.write_u64::<BE>(user_id);
        value.write_u64::<BE>(timestamp.timestamp_millis() as u64);

        if let Some(status_id) = replied_to_status_id {
            value.write_u64::<BE>(status_id);
        }

        if let Some(status_id) = quoted_status_id {
            value.write_u64::<BE>(status_id);
        }

        for user_id in mentioned_user_ids {
            value.write_u64::<BE>(user_id);
        }
        println!("{}", value.len());

        Self { key, value }
    }

    pub fn get_status_id(&self) -> u64 {
        u64::from_be_bytes(self.key[1..9].try_into().unwrap())
    }

    pub fn get_user_id(&self) -> u64 {
        Self::extract_user_id(&self.value)
    }

    pub fn get_timestamp(&self) -> DateTime<Utc> {
        Self::extract_timestamp(&self.value)
    }

    pub fn get_retweeted_status_id(&self) -> Option<u64> {
        Self::extract_retweeted_status_id(&self.value)
    }

    pub fn get_replied_to_status_id(&self) -> Option<u64> {
        Self::extract_replied_to_status_id(&self.value)
    }

    pub fn get_quoted_status_id(&self) -> Option<u64> {
        Self::extract_quoted_status_id(&self.value)
    }

    pub fn get_mentioned_user_ids(&self) -> U64Iter {
        Self::extract_mentioned_user_ids(&self.value)
    }

    fn extract_user_id(value: &[u8]) -> u64 {
        u64::from_be_bytes(value[1..9].try_into().unwrap())
    }

    fn extract_timestamp(value: &[u8]) -> DateTime<Utc> {
        Utc.timestamp_millis(u64::from_be_bytes(value[9..17].try_into().unwrap()) as i64)
    }

    fn extract_retweeted_status_id(value: &[u8]) -> Option<u64> {
        if value[0] == 4 {
            Some(u64::from_be_bytes(value[17..25].try_into().unwrap()))
        } else {
            None
        }
    }

    fn extract_replied_to_status_id(value: &[u8]) -> Option<u64> {
        let tag = value[0];

        if tag == 1 || tag == 3 {
            Some(u64::from_be_bytes(value[17..25].try_into().unwrap()))
        } else {
            None
        }
    }

    fn extract_quoted_status_id(value: &[u8]) -> Option<u64> {
        let tag = value[0];

        if tag == 2 {
            Some(u64::from_be_bytes(value[17..25].try_into().unwrap()))
        } else if tag == 3 {
            Some(u64::from_be_bytes(value[25..33].try_into().unwrap()))
        } else {
            None
        }
    }

    fn extract_mentioned_user_ids(value: &[u8]) -> U64Iter {
        let tag = value[0];

        if tag == 4 {
            U64Iter::empty()
        } else {
            let offset = if tag == 0 {
                17
            } else if tag == 1 || tag == 2 {
                25
            } else {
                33
            };
            U64Iter {
                cursor: Cursor::new(&value[offset..]),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{super::Entry, FullStatusEntry};
    use chrono::{DateTime, TimeZone, Utc};
    use proptest::prelude::*;
    use std::collections::HashSet;

    // Somewhat arbitrarily test dates up to 2050.
    const timestamp_millis_max: i64 = 2521843200000;

    proptest! {
        #[test]
        fn round_trip_retweet(
            status_id: u64,
            user_id: u64,
            timestamp_millis in 0i64..timestamp_millis_max,
            retweeted_status_id: u64
        ) {
            let timestamp = Utc.timestamp_millis(timestamp_millis);
            let entry = FullStatusEntry::new_retweet(status_id, user_id, timestamp, retweeted_status_id);

            assert_eq!(entry.validate(), true);
            assert_eq!(entry.get_status_id(), status_id);
            assert_eq!(entry.get_user_id(), user_id);
            assert_eq!(entry.get_timestamp(), timestamp);
            assert_eq!(entry.get_retweeted_status_id(), Some(retweeted_status_id));
            assert_eq!(entry.get_replied_to_status_id(), None);
            assert_eq!(entry.get_quoted_status_id(), None);
            assert_eq!(entry.get_mentioned_user_ids().next(), None);
        }

        #[test]
        fn round_trip_tweet(
            status_id: u64,
            user_id: u64,
            timestamp_millis in 0i64..timestamp_millis_max,
            replied_to_status_id: Option<u64>,
            quoted_status_id: Option<u64>,
            mentioned_user_ids: HashSet<u64>,
        ) {
            let timestamp = Utc.timestamp_millis(timestamp_millis);
            let entry = FullStatusEntry::new_tweet(status_id, user_id, timestamp, replied_to_status_id, quoted_status_id, mentioned_user_ids.clone());

            assert_eq!(entry.validate(), true);
            assert_eq!(entry.get_status_id(), status_id);
            assert_eq!(entry.get_user_id(), user_id);
            assert_eq!(entry.get_timestamp(), timestamp);
            assert_eq!(entry.get_retweeted_status_id(), None);
            assert_eq!(entry.get_replied_to_status_id(), replied_to_status_id);
            assert_eq!(entry.get_quoted_status_id(), quoted_status_id);
            assert_eq!(entry.get_mentioned_user_ids().collect::<HashSet<_>>(), mentioned_user_ids);
        }
    }
}

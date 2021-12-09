use super::error::Error;
use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use chrono::{DateTime, NaiveDateTime, Utc};
use rocksdb::{MergeOperands, Options, DB};
use std::io::Cursor;
use std::path::Path;

#[derive(Copy, Clone)]
enum Tag {
    User,
    ScreenName,
    ShortStatus,
    FullStatus,
    Delete,
    CompletedFile,
}

impl Tag {
    fn value(self) -> u8 {
        match self {
            Tag::User => 0,
            Tag::ScreenName => 1,
            Tag::FullStatus => 2,
            Tag::ShortStatus => 3,
            Tag::Delete => 4,
            Tag::CompletedFile => 16,
        }
    }
}

type Delete = (u64, Option<DateTime<Utc>>);

pub struct Lookup {
    db: DB,
}

impl Lookup {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Lookup, Error> {
        let mut options = Options::default();
        options.set_merge_operator_associative("merge", Self::merge);
        let db = DB::open(&options, path)?;

        Ok(Lookup { db })
    }

    fn user_id_to_prefix(tag: Tag, user_id: u64) -> Result<Vec<u8>, Error> {
        let mut prefix = Vec::with_capacity(9);
        prefix.write_u8(tag.value())?;
        prefix.write_u64::<BE>(user_id)?;
        Ok(prefix)
    }

    fn decode_user_key(bytes: &[u8]) -> Result<(u8, u64, String), Error> {
        let mut cursor = Cursor::new(bytes);
        let tag = cursor.read_u8()?;
        let user_id = cursor.read_u64::<BE>()?;
        let screen_name = std::str::from_utf8(cursor.remaining_slice())?.to_string();
        Ok((tag, user_id, screen_name))
    }

    fn decode_delete_key(bytes: &[u8]) -> Result<(u8, u64, u64), Error> {
        let mut cursor = Cursor::new(bytes);
        let tag = cursor.read_u8()?;
        let user_id = cursor.read_u64::<BE>()?;
        let status_id = cursor.read_u64::<BE>()?;
        Ok((tag, user_id, status_id))
    }

    fn decode_completed_file_key(bytes: &[u8]) -> Result<(u8, String, String), Error> {
        let mut cursor = Cursor::new(bytes);
        let tag = cursor.read_u8()?;
        let value = std::str::from_utf8(cursor.remaining_slice())?.to_string();
        let parts = value.split('|').collect::<Vec<_>>();

        if parts.len() != 2 {
            Err(Error::UnexpectedKey(value))
        } else {
            Ok((tag, parts[0].to_string(), parts[1].to_string()))
        }
    }

    pub fn lookup_user(&self, user_id: u64) -> Result<Vec<(String, Vec<u64>)>, Error> {
        let prefix = Self::user_id_to_prefix(Tag::User, user_id)?;
        let iter = self.db.prefix_iterator(prefix);
        let mut result = vec![];

        for (key, value) in iter {
            let (tag, current_user_id, screen_name) = Self::decode_user_key(&key)?;

            if tag != Tag::User.value() || current_user_id != user_id {
                break;
            }

            let mut cursor = Cursor::new(value);
            let mut status_ids = vec![];
            while !cursor.is_empty() {
                status_ids.push(cursor.read_u64::<BE>()?);
            }

            result.push((screen_name.to_string(), status_ids));
        }

        Ok(result)
    }

    /// If the user has had multiple screen names, returns the most attested one.
    pub fn lookup_user_screen_name(&self, user_id: u64) -> Result<Option<String>, Error> {
        let by_screen_name = self.lookup_user(user_id)?;

        Ok(by_screen_name
            .into_iter()
            .max_by_key(|(_, ids)| ids.len())
            .map(|(screen_name, _)| screen_name))
    }

    pub fn lookup_screen_name(&self, screen_name: &str) -> Result<Vec<u64>, Error> {
        let screen_name_lc = screen_name.to_lowercase();
        let screen_name_bytes = screen_name_lc.as_bytes();
        let mut key = Vec::with_capacity(screen_name_bytes.len() + 1);
        key.write_u8(Tag::ScreenName.value())?;
        key.extend_from_slice(screen_name_bytes);

        let result = match self.db.get_pinned(key)? {
            Some(value) => {
                let mut cursor = Cursor::new(value);
                let mut user_ids = vec![];
                while !cursor.is_empty() {
                    user_ids.push(cursor.read_u64::<BE>()?);
                }
                user_ids
            }
            None => vec![],
        };

        Ok(result)
    }

    pub fn lookup_tweet_metadata(&self, status_id: u64) -> Result<Option<TweetMetadata>, Error> {
        let mut key = Vec::with_capacity(9);
        key.write_u8(Tag::FullStatus.value())?;
        key.write_u64::<BE>(status_id)?;

        let result = match self.db.get_pinned(key)? {
            Some(value) => {
                let mut cursor = Cursor::new(value);
                let tag = cursor.read_u8()?;
                let user_id = cursor.read_u64::<BE>()?;

                let timestamp = millis_to_date_time(cursor.read_u64::<BE>()?);

                if tag == 4 {
                    let retweeted_id = cursor.read_u64::<BE>()?;

                    Some(TweetMetadata::Retweet {
                        status_id,
                        timestamp,
                        user_id,
                        retweeted_id,
                    })
                } else {
                    let replied_to_id = if tag == 1 || tag == 3 {
                        Some(cursor.read_u64::<BE>()?)
                    } else {
                        None
                    };

                    let quoted_id = if tag == 2 || tag == 3 {
                        Some(cursor.read_u64::<BE>()?)
                    } else {
                        None
                    };

                    let mut mentioned_user_ids = vec![];
                    while !cursor.is_empty() {
                        mentioned_user_ids.push(cursor.read_u64::<BE>()?);
                    }

                    Some(TweetMetadata::Full {
                        status_id,
                        timestamp,
                        user_id,
                        replied_to_id,
                        quoted_id,
                        mentioned_user_ids,
                    })
                }
            }
            None => None,
        };

        Ok(result)
    }

    pub fn lookup_deletes(&self, user_id: u64) -> Result<Vec<Delete>, Error> {
        let prefix = Self::user_id_to_prefix(Tag::Delete, user_id)?;
        let iter = self.db.prefix_iterator(prefix);
        let mut result = vec![];

        for (key, value) in iter {
            let (tag, current_user_id, status_id) = Self::decode_delete_key(&key)?;

            if tag != Tag::Delete.value() || current_user_id != user_id {
                break;
            }

            let timestamp_millis = if !value.is_empty() {
                let mut cursor = Cursor::new(value);

                Some(cursor.read_u64::<BE>()?)
            } else {
                None
            };

            result.push((status_id, timestamp_millis.map(millis_to_date_time)));
        }

        Ok(result)
    }

    pub fn get_estimated_key_count(&self) -> Result<u64, Error> {
        Ok(self
            .db
            .property_int_value("rocksdb.estimate-num-keys")?
            .unwrap())
    }

    pub fn get_completed_files(&self) -> Result<Vec<(String, String, u64)>, Error> {
        let mut prefix = Vec::with_capacity(9);
        prefix.write_u8(Tag::CompletedFile.value())?;

        let iter = self.db.prefix_iterator(prefix);
        let mut result = vec![];

        for (key, value) in iter {
            let (tag, archive_path, file_path) = Self::decode_completed_file_key(&key)?;

            if tag != Tag::CompletedFile.value() {
                break;
            }

            let mut cursor = Cursor::new(value);
            let status_count = cursor.read_u64::<BE>()?;

            result.push((archive_path, file_path, status_count));
        }

        Ok(result)
    }

    fn get_entries_for_tag(&self, tag: Tag) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> + '_ {
        let mut prefix = Vec::with_capacity(1);
        prefix.push(tag.value());
        self.db
            .prefix_iterator(&prefix)
            .take_while(move |(key, _)| {
                let mut cursor = Cursor::new(key);
                match cursor.read_u8() {
                    Ok(next_tag) => next_tag == tag.value(),
                    Err(_) => false,
                }
            })
    }

    pub fn get_user_ids(&self) -> Result<Vec<u64>, Error> {
        let mut result = vec![];

        for (key, _) in self.get_entries_for_tag(Tag::User) {
            let mut cursor = Cursor::new(key);
            cursor.read_u8()?;
            let user_id = cursor.read_u64::<BE>()?;
            result.push(user_id);
        }

        Ok(result)
    }

    pub fn get_full_status_ids(&self) -> Result<Vec<u64>, Error> {
        let mut result = vec![];

        for (key, _) in self.get_entries_for_tag(Tag::FullStatus) {
            let mut cursor = Cursor::new(key);
            cursor.read_u8()?;
            let status_id = cursor.read_u64::<BE>()?;
            result.push(status_id);
        }

        Ok(result)
    }

    pub fn get_short_status_ids(&self) -> Result<Vec<u64>, Error> {
        let mut result = vec![];

        for (key, _) in self.get_entries_for_tag(Tag::ShortStatus) {
            let mut cursor = Cursor::new(key);
            cursor.read_u8()?;
            let status_id = cursor.read_u64::<BE>()?;
            result.push(status_id);
        }

        Ok(result)
    }

    pub fn get_delete_ids(&self) -> Result<Vec<u64>, Error> {
        let mut result = vec![];

        for (key, _) in self.get_entries_for_tag(Tag::Delete) {
            let mut cursor = Cursor::new(key);
            cursor.read_u8()?;
            let status_id = cursor.read_u64::<BE>()?;
            result.push(status_id);
        }

        Ok(result)
    }

    pub fn get_stats(&self) -> Result<Stats, Error> {
        let mut pair_count = 0;
        let mut user_id_count = 0;
        let mut appearance_count = 0;
        let mut screen_name_count = 0;
        let mut full_status_count = 0;
        let mut short_status_count = 0;
        let mut delete_count = 0;
        let mut completed_file_count = 0;

        let mut last_user_id = 0;

        for (key, value) in self.get_entries_for_tag(Tag::User) {
            let mut cursor = Cursor::new(key);
            cursor.read_u8()?;

            pair_count += 1;
            let user_id = cursor.read_u64::<BE>()?;
            if user_id != last_user_id {
                user_id_count += 1;
                last_user_id = user_id;
            }
            appearance_count += value.len() / 8;
        }

        screen_name_count += self.get_entries_for_tag(Tag::ScreenName).count();
        full_status_count += self.get_entries_for_tag(Tag::FullStatus).count();
        short_status_count += self.get_entries_for_tag(Tag::ShortStatus).count();
        delete_count += self.get_entries_for_tag(Tag::Delete).count();
        completed_file_count += self.get_entries_for_tag(Tag::CompletedFile).count();

        Ok(Stats {
            pair_count,
            user_id_count,
            appearance_count,
            screen_name_count,
            full_status_count,
            short_status_count,
            delete_count,
            completed_file_count,
        })
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

const FIRST_SNOWFLAKE: u64 = 250000000000000;

fn is_snowflake(value: u64) -> bool {
    value >= FIRST_SNOWFLAKE
}

fn millis_to_date_time(timestamp_millis: u64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp(
            timestamp_millis as i64 / 1000,
            (timestamp_millis as u32 % 1000) * 1000000,
        ),
        Utc,
    )
}

fn snowflake_to_date_time(value: u64) -> DateTime<Utc> {
    let timestamp_millis = (value >> 22) + 1288834974657;

    millis_to_date_time(timestamp_millis)
}

#[derive(Debug, Eq, PartialEq)]
pub struct Stats {
    pub pair_count: usize,
    pub user_id_count: usize,
    pub appearance_count: usize,
    pub screen_name_count: usize,
    pub full_status_count: usize,
    pub short_status_count: usize,
    pub delete_count: usize,
    pub completed_file_count: usize,
}

#[derive(Debug)]
pub enum TweetMetadata {
    Short {
        status_id: u64,
        user_id: u64,
    },
    Full {
        status_id: u64,
        timestamp: DateTime<Utc>,
        user_id: u64,
        replied_to_id: Option<u64>,
        quoted_id: Option<u64>,
        mentioned_user_ids: Vec<u64>,
    },
    Retweet {
        status_id: u64,
        timestamp: DateTime<Utc>,
        user_id: u64,
        retweeted_id: u64,
    },
}

impl TweetMetadata {
    pub fn status_id(&self) -> u64 {
        match self {
            Self::Short { status_id, .. } => *status_id,
            Self::Full { status_id, .. } => *status_id,
            Self::Retweet { status_id, .. } => *status_id,
        }
    }
    pub fn user_id(&self) -> u64 {
        match self {
            Self::Short { user_id, .. } => *user_id,
            Self::Full { user_id, .. } => *user_id,
            Self::Retweet { user_id, .. } => *user_id,
        }
    }
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Short { status_id, .. } => {
                if is_snowflake(*status_id) {
                    Some(snowflake_to_date_time(*status_id))
                } else {
                    None
                }
            }
            Self::Full { timestamp, .. } => Some(*timestamp),
            Self::Retweet { timestamp, .. } => Some(*timestamp),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tar::Archive;

    #[test]
    fn lookup_operations() {
        let mut archive =
            Archive::new(File::open("examples/databases/user-2021-01-01.tar").unwrap());
        let dir = tempfile::tempdir().unwrap();

        archive.unpack(&dir).unwrap();

        let db = Lookup::new(dir.path().join("user-2021-01-01")).unwrap();

        assert_eq!(db.lookup_screen_name("genflynn").unwrap(), vec![240454812]);

        let expected_stats = Stats {
            pair_count: 10757,
            user_id_count: 10757,
            appearance_count: 10852,
            screen_name_count: 10757,
            full_status_count: 8130,
            short_status_count: 1743,
            delete_count: 831,
            completed_file_count: 2,
        };

        assert_eq!(db.get_stats().unwrap(), expected_stats);
    }
}

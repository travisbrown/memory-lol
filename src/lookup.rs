use super::error::Error;
use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use chrono::{DateTime, NaiveDateTime, Utc};
use rocksdb::{IteratorMode, Options, DB};
use std::io::Cursor;
use std::path::Path;

#[derive(Copy, Clone)]
enum Tag {
    User,
    ScreenName,
    Status,
    Delete,
}

impl Tag {
    fn value(self) -> u8 {
        match self {
            Tag::User => 0,
            Tag::ScreenName => 1,
            Tag::Status => 2,
            Tag::Delete => 3,
        }
    }
}

type Delete = (u64, Option<DateTime<Utc>>);

pub struct Lookup {
    db: DB,
}

impl Lookup {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Lookup, Error> {
        let options = Options::default();
        let db = DB::open(&options, path)?;

        Ok(Lookup { db })
    }

    fn user_id_to_prefix(tag: Tag, user_id: u64) -> Result<Vec<u8>, Error> {
        let mut prefix = Vec::with_capacity(9);
        prefix.write_u8(tag.value())?;
        prefix.write_u64::<BE>(user_id)?;
        Ok(prefix)
    }

    fn decode_user_key(bytes: &[u8]) -> Result<(u64, String), Error> {
        let mut cursor = Cursor::new(bytes);
        cursor.read_u8()?;
        let user_id = cursor.read_u64::<BE>()?;
        let screen_name = std::str::from_utf8(cursor.remaining_slice())?.to_string();
        Ok((user_id, screen_name))
    }

    fn decode_delete_key(bytes: &[u8]) -> Result<(u64, u64), Error> {
        let mut cursor = Cursor::new(bytes);
        cursor.read_u8()?;
        let user_id = cursor.read_u64::<BE>()?;
        let status_id = cursor.read_u64::<BE>()?;
        Ok((user_id, status_id))
    }

    pub fn lookup_user(&self, user_id: u64) -> Result<Vec<(String, Vec<u64>)>, Error> {
        let prefix = Self::user_id_to_prefix(Tag::User, user_id)?;
        let iter = self.db.prefix_iterator(prefix);
        let mut result = vec![];

        for (key, value) in iter {
            let (current_user_id, screen_name) = Self::decode_user_key(&key)?;

            if current_user_id != user_id {
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
        key.write_u8(Tag::Status.value())?;
        key.write_u64::<BE>(status_id)?;

        let result = match self.db.get_pinned(key)? {
            Some(value) => {
                let mut cursor = Cursor::new(value);
                let user_id = cursor.read_u64::<BE>()?;

                if cursor.is_empty() {
                    Some(TweetMetadata::Short { status_id, user_id })
                } else {
                    let timestamp = if is_snowflake(status_id) {
                        snowflake_to_date_time(status_id)
                    } else {
                        millis_to_date_time(cursor.read_u64::<BE>()?)
                    };

                    let tag = cursor.read_u8()?;

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
            let (current_user_id, status_id) = Self::decode_delete_key(&key)?;

            if current_user_id != user_id {
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

    pub fn get_stats(&self) -> Result<Stats, Error> {
        let mut pair_count = 0;
        let mut user_id_count = 0;
        let mut appearance_count = 0;
        let mut screen_name_count = 0;
        let mut status_count = 0;
        let mut delete_count = 0;

        let iter = self.db.iterator(IteratorMode::Start);
        let mut last_user_id = 0;

        for (key, value) in iter {
            let mut cursor = Cursor::new(key);
            let tag = cursor.read_u8()?;

            match tag {
                0 => {
                    pair_count += 1;
                    let user_id = cursor.read_u64::<BE>()?;
                    if user_id != last_user_id {
                        user_id_count += 1;
                        last_user_id = user_id;
                    }
                    appearance_count += (value.len() / 8) as u64;
                }
                1 => {
                    screen_name_count += 1;
                }
                2 => {
                    status_count += 1;
                }
                3 => {
                    delete_count += 2;
                }
                _ => {
                    return Err(Error::UnexpectedKey(format!("Invalid tag: {}", tag)));
                }
            }
        }

        Ok(Stats {
            pair_count,
            user_id_count,
            appearance_count,
            screen_name_count,
            status_count,
            delete_count,
        })
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
    pub pair_count: u64,
    pub user_id_count: u64,
    pub appearance_count: u64,
    pub screen_name_count: u64,
    pub status_count: u64,
    pub delete_count: u64,
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
            pair_count: 9360,
            user_id_count: 9360,
            appearance_count: 9745,
            screen_name_count: 9360,
            status_count: 9841,
            delete_count: 1662,
        };

        assert_eq!(db.get_stats().unwrap(), expected_stats);
    }
}

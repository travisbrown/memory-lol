use super::{
    avro::{User, UserCodec},
    error::Error,
};
use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use chrono::{DateTime, NaiveDateTime, Utc};
use rocksdb::{Options, DB};
use std::io::Cursor;
use std::path::Path;

pub struct UserLookup {
    db: DB,
    codec: UserCodec,
}

impl UserLookup {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<UserLookup, Error> {
        let options = Options::default();
        let db = DB::open(&options, path)?;
        let codec = UserCodec::default();

        Ok(UserLookup { db, codec })
    }

    fn user_id_to_prefix(user_id: u64) -> Result<Vec<u8>, Error> {
        let mut prefix = Vec::with_capacity(9);
        prefix.write_u8(0)?;
        prefix.write_u64::<BE>(user_id)?;
        Ok(prefix)
    }

    fn decode_user_key(bytes: &[u8]) -> Result<(u64, u64), Error> {
        let mut cursor = Cursor::new(bytes);
        cursor.read_u8()?;
        let user_id = cursor.read_u64::<BE>()?;
        let timestamp_millis = cursor.read_u64::<BE>()?;
        Ok((user_id, timestamp_millis))
    }

    pub fn lookup_user(&self, user_id: u64) -> Result<Vec<(DateTime<Utc>, User)>, Error> {
        let prefix = Self::user_id_to_prefix(user_id)?;
        let iter = self.db.prefix_iterator(prefix);
        let mut result = vec![];

        for (key, value) in iter {
            let (current_user_id, timestamp_millis) = Self::decode_user_key(&key)?;

            if current_user_id != user_id {
                break;
            }

            let user = self.codec.from_bytes(&value).unwrap();

            result.push((millis_to_date_time(timestamp_millis), user));
        }

        Ok(result)
    }
}

const FIRST_SNOWFLAKE: u64 = 250000000000000;

pub fn is_snowflake(value: u64) -> bool {
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

pub fn snowflake_to_date_time(value: u64) -> DateTime<Utc> {
    let timestamp_millis = (value >> 22) + 1288834974657;

    millis_to_date_time(timestamp_millis)
}

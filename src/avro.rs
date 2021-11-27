use avro_rs::{
    types::{Record, Value},
    Schema,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;

const TWITTER_DATE_TIME_FMT: &str = "%a %b %d %H:%M:%S %z %Y";

pub fn parse_date(input: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_str(input, TWITTER_DATE_TIME_FMT)
        .ok()
        .map(From::from)
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("AVRO error: {0}")]
    Avro(#[from] avro_rs::Error),
    #[error("Date parsing error: {0}")]
    DateFormat(#[from] chrono::format::ParseError),
    #[error("Missing field: {0}")]
    MissingField(String),
}

pub struct UserCodec {
    schema: Schema,
}

impl UserCodec {
    pub fn new(input: &str) -> Result<UserCodec, Error> {
        let schema = Schema::parse_str(input)?;

        Ok(UserCodec { schema })
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<UserCodec, Error> {
        let contents = std::fs::read_to_string(path)?;

        Self::new(&contents)
    }

    pub fn from_json(&self, value: &serde_json::Value) -> Result<Vec<u8>, Error> {
        let user = User::from_json(value)?;
        let bytes = self.to_bytes(&user)?;

        Ok(bytes)
    }

    pub fn from_bytes(&self, mut value: &[u8]) -> Result<User, Error> {
        let value = avro_rs::from_avro_datum(&self.schema, &mut value, Some(&self.schema))?;

        Ok(Self::from_record(value).unwrap())
    }

    pub fn to_bytes(&self, user: &User) -> Result<Vec<u8>, Error> {
        Ok(avro_rs::to_avro_datum(&self.schema, self.to_record(user))?)
    }

    fn from_record(value: avro_rs::types::Value) -> Option<User> {
        if let Value::Record(pairs) = value {
            let mut map = pairs.into_iter().collect::<HashMap<_, _>>();

            let id = get_field::<u64>(&mut map, "id")?;
            let screen_name = get_field::<String>(&mut map, "screen_name")?;
            let name = get_field::<String>(&mut map, "name")?;
            let location = get_field::<Option<String>>(&mut map, "location")?;
            let url = get_field::<Option<String>>(&mut map, "url")?;
            let description = get_field::<Option<String>>(&mut map, "description")?;
            let protected = get_field(&mut map, "protected")?;
            let verified = get_field(&mut map, "verified")?;
            let followers_count = get_field(&mut map, "followers_count")?;
            let friends_count = get_field(&mut map, "friends_count")?;
            let listed_count = get_field(&mut map, "listed_count")?;
            let favourites_count = get_field(&mut map, "favourites_count")?;
            let statuses_count = get_field(&mut map, "statuses_count")?;
            let created_at = get_field(&mut map, "created_at")?;
            let profile_image_url = get_field(&mut map, "profile_image_url")?;
            let profile_banner_url = get_field(&mut map, "profile_banner_url")?;
            let profile_background_image_url = get_field(&mut map, "profile_background_image_url")?;
            let default_profile = get_field(&mut map, "default_profile")?;
            let default_profile_image = get_field(&mut map, "default_profile_image")?;
            let withheld_in_countries = get_field(&mut map, "withheld_in_countries")?;
            let time_zone = get_field(&mut map, "time_zone")?;
            let lang = get_field(&mut map, "lang")?;
            let geo_enabled = get_field(&mut map, "geo_enabled")?;

            Some(User {
                id,
                screen_name,
                name,
                location,
                url,
                description,
                protected,
                verified,
                followers_count,
                friends_count,
                listed_count,
                favourites_count,
                statuses_count,
                created_at,
                profile_image_url,
                profile_banner_url,
                profile_background_image_url,
                default_profile,
                default_profile_image,
                withheld_in_countries,
                time_zone,
                lang,
                geo_enabled,
            })
        } else {
            None
        }
    }

    fn to_record(&self, user: &User) -> Record {
        let mut record = Record::new(&self.schema).unwrap();

        record.put("id", user.id as i64);
        record.put("screen_name", user.screen_name.clone());
        record.put("name", user.name.clone());
        record.put("location", user.location.clone());
        record.put("url", user.url.clone());
        record.put("description", user.description.clone());
        record.put("protected", user.protected);
        record.put("verified", user.verified);
        record.put("followers_count", user.followers_count as i64);
        record.put("friends_count", user.friends_count as i64);
        record.put("listed_count", user.listed_count as i64);
        record.put("favourites_count", user.favourites_count as i64);
        record.put("statuses_count", user.statuses_count as i64);
        record.put("created_at", user.created_at.timestamp_millis());
        record.put("profile_image_url", user.profile_image_url.clone());
        record.put("profile_banner_url", user.profile_banner_url.clone());
        record.put(
            "profile_background_image_url",
            user.profile_background_image_url.clone(),
        );
        record.put("default_profile", user.default_profile);
        record.put("default_profile_image", user.default_profile_image);
        record.put(
            "withheld_in_countries",
            avro_rs::types::Value::Array(
                user.withheld_in_countries
                    .clone()
                    .into_iter()
                    .map(avro_rs::types::Value::from)
                    .collect::<Vec<_>>(),
            ),
        );
        record.put("time_zone", user.time_zone.clone());
        record.put("lang", user.lang.clone());
        record.put("geo_enabled", user.geo_enabled);
        record
    }
}

impl Default for UserCodec {
    fn default() -> Self {
        Self::from_file("schemas/avro/User.avsc").unwrap()
    }
}

#[derive(Serialize, Debug)]
pub struct User {
    pub id: u64,
    pub screen_name: String,
    pub name: String,
    pub location: Option<String>,
    pub url: Option<String>,
    pub description: Option<String>,
    pub protected: bool,
    pub verified: bool,
    pub followers_count: u64,
    pub friends_count: u64,
    pub listed_count: u64,
    pub favourites_count: u64,
    pub statuses_count: u64,
    pub created_at: DateTime<Utc>,
    pub profile_image_url: String,
    pub profile_banner_url: Option<String>,
    pub profile_background_image_url: Option<String>,
    pub default_profile: bool,
    pub default_profile_image: bool,
    pub withheld_in_countries: Vec<String>,
    pub time_zone: Option<String>,
    pub lang: Option<String>,
    pub geo_enabled: Option<bool>,
}

impl User {
    pub fn from_json(value: &serde_json::Value) -> Result<User, Error> {
        let id = value
            .get("id_str")
            .and_then(|v| v.as_str().and_then(|v| v.parse::<u64>().ok()))
            .ok_or_else(|| Error::MissingField("id_str".to_string()))?;
        let screen_name = value
            .get("screen_name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::MissingField("screen_name".to_string()))?
            .to_string();
        let name = value
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::MissingField("name".to_string()))?
            .to_string();
        let location = value
            .get("location")
            .and_then(|v| v.as_str().map(|v| v.to_string()));
        let url = value
            .get("url")
            .and_then(|v| v.as_str().map(|v| v.to_string()));
        let description = value
            .get("description")
            .and_then(|v| v.as_str().map(|v| v.to_string()));
        let protected = value
            .get("protected")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let verified = value
            .get("verified")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let followers_count = value
            .get("followers_count")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| Error::MissingField("followers_count".to_string()))?;
        let friends_count = value
            .get("friends_count")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| Error::MissingField("friends_count".to_string()))?;
        let listed_count = value
            .get("listed_count")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| Error::MissingField("listed_count".to_string()))?;
        let favourites_count = value
            .get("favourites_count")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| Error::MissingField("favourites_count".to_string()))?;
        let statuses_count = value
            .get("statuses_count")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| Error::MissingField("statuses_count".to_string()))?;
        let created_at = DateTime::parse_from_str(
            value
                .get("created_at")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::MissingField("created_at".to_string()))?,
            TWITTER_DATE_TIME_FMT,
        )?
        .into();
        let profile_image_url = value
            .get("profile_image_url_https")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::MissingField("profile_image_url_https".to_string()))?
            .to_string();
        let profile_banner_url = value
            .get("profile_banner_url")
            .and_then(|v| v.as_str().map(|v| v.to_string()));
        let profile_background_image_url = value
            .get("profile_background_image_url_https")
            .and_then(|v| v.as_str().map(|v| v.to_string()));
        let default_profile = value
            .get("default_profile")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| Error::MissingField("default_profile".to_string()))?;
        let default_profile_image = value
            .get("default_profile_image")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| Error::MissingField("default_profile_image".to_string()))?;
        let withheld_in_countries = value
            .get("withheld_in_countries")
            .and_then(|v| {
                v.as_array()
                    .and_then(|vs| vs.iter().map(|v| v.as_str()).collect::<Option<Vec<_>>>())
            })
            .unwrap_or_default()
            .into_iter()
            .map(|v| v.to_string())
            .collect();
        let time_zone = value
            .get("time_zone")
            .and_then(|v| v.as_str().map(|v| v.to_string()));
        let lang = value
            .get("lang")
            .and_then(|v| v.as_str().map(|v| v.to_string()));
        let geo_enabled = value.get("geo_enabled").and_then(|v| v.as_bool());

        Ok(User {
            id,
            screen_name,
            name,
            location,
            url,
            description,
            protected,
            verified,
            followers_count,
            friends_count,
            listed_count,
            favourites_count,
            statuses_count,
            created_at,
            profile_image_url,
            profile_banner_url,
            profile_background_image_url,
            default_profile,
            default_profile_image,
            withheld_in_countries,
            time_zone,
            lang,
            geo_enabled,
        })
    }
}

fn get_field<T: FromValue>(record: &mut HashMap<String, Value>, name: &str) -> Option<T> {
    record.remove(name).and_then(T::from_value)
}

trait FromValue {
    fn from_value(value: Value) -> Option<Self>
    where
        Self: Sized;
}

impl FromValue for u64 {
    fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::Long(v) => Some(v as u64),
            _ => None,
        }
    }
}

impl FromValue for bool {
    fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::Boolean(v) => Some(v),
            _ => None,
        }
    }
}

impl FromValue for String {
    fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::String(v) => Some(v),
            _ => None,
        }
    }
}

impl FromValue for DateTime<Utc> {
    fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::TimestampMillis(v) => Some(DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(v as i64 / 1000, (v as u32 % 1000) * 1000000),
                Utc,
            )),
            _ => None,
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::Union(boxed) => match *boxed {
                Value::Null => Some(None),
                other => T::from_value(other).map(Some),
            },
            _ => None,
        }
    }
}

impl<T: FromValue> FromValue for Vec<T> {
    fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::Array(values) => values.into_iter().map(T::from_value).collect(),
            _ => None,
        }
    }
}

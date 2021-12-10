use super::{
    error::Error,
    lookup::{Lookup, TweetMetadata},
};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;

#[derive(Debug, Default, Serialize)]
pub struct UserReport {
    screen_name_dates: HashMap<String, (Option<(DateTime<Utc>, DateTime<Utc>)>, usize)>,
    retweets: HashMap<u64, (String, Vec<u64>)>,
    replies_to: HashMap<u64, (String, Vec<u64>)>,
    quotes: HashMap<u64, (String, Vec<u64>)>,
    mentions: HashMap<u64, (String, Vec<u64>)>,
    retweeted_by: HashMap<u64, (String, Vec<u64>)>,
    replied_to_by: HashMap<u64, (String, Vec<u64>)>,
    quoted_by: HashMap<u64, (String, Vec<u64>)>,
    mentioned_by: HashMap<u64, (String, Vec<u64>)>,
    not_found: Vec<u64>,
}

#[derive(Debug, Default)]
struct UserRelations {
    retweets: HashSet<(u64, u64)>,
    replies_to: HashSet<(u64, u64)>,
    quotes: HashSet<(u64, u64)>,
    mentions: HashSet<(u64, u64)>,
    retweeted_by: HashSet<(u64, u64)>,
    replied_to_by: HashSet<(u64, u64)>,
    quoted_by: HashSet<(u64, u64)>,
    mentioned_by: HashSet<(u64, u64)>,
}

impl UserRelations {
    fn user_ids(&self) -> HashSet<u64> {
        let mut result = HashSet::new();
        result.extend(self.retweets.iter().map(|(id, _)| id));
        result.extend(self.replies_to.iter().map(|(id, _)| id));
        result.extend(self.quotes.iter().map(|(id, _)| id));
        result.extend(self.mentions.iter().map(|(id, _)| id));
        result.extend(self.retweeted_by.iter().map(|(id, _)| id));
        result.extend(self.replied_to_by.iter().map(|(id, _)| id));
        result.extend(self.quoted_by.iter().map(|(id, _)| id));
        result.extend(self.mentioned_by.iter().map(|(id, _)| id));
        result
    }

    fn expand(
        user_db: &HashMap<u64, String>,
        batch: &HashSet<(u64, u64)>,
    ) -> HashMap<u64, (String, Vec<u64>)> {
        let mut result = HashMap::new();

        for (user_id, group) in &batch.iter().group_by(|(user_id, _)| user_id) {
            if let Some(screen_name) = user_db.get(user_id) {
                let mut status_ids = group.map(|(_, status_id)| *status_id).collect::<Vec<_>>();
                status_ids.sort_unstable();
                status_ids.dedup();

                result.insert(*user_id, (screen_name.to_string(), status_ids));
            }
        }

        result
    }

    fn add(&mut self, target_user_id: u64, db: &HashMap<u64, TweetMetadata>, status_id: u64) {
        if let Some(metadata) = db.get(&status_id) {
            match metadata {
                TweetMetadata::Retweet {
                    user_id,
                    retweeted_id,
                    ..
                } => {
                    if let Some(retweeted_metadata) = db.get(retweeted_id) {
                        if *user_id == target_user_id {
                            self.retweets
                                .insert((retweeted_metadata.user_id(), *retweeted_id));
                        } else if retweeted_metadata.user_id() == target_user_id {
                            self.retweeted_by.insert((*user_id, *retweeted_id));
                        }
                    }
                }
                TweetMetadata::Full {
                    user_id,
                    replied_to_id,
                    quoted_id,
                    mentioned_user_ids,
                    ..
                } => {
                    let replied_to_metadata = replied_to_id.and_then(|id| db.get(&id));
                    let quoted_metadata = quoted_id.and_then(|id| db.get(&id));

                    if *user_id == target_user_id {
                        if let Some(replied_to_metadata) = replied_to_metadata {
                            self.replies_to.insert((
                                replied_to_metadata.user_id(),
                                replied_to_metadata.status_id(),
                            ));
                        }
                        if let Some(quoted_metadata) = quoted_metadata {
                            self.quotes
                                .insert((quoted_metadata.user_id(), quoted_metadata.status_id()));
                        }
                        for mentioned_user_id in mentioned_user_ids {
                            self.mentions.insert((*mentioned_user_id, status_id));
                        }
                    }

                    if let Some(replied_to_metadata) = replied_to_metadata {
                        if replied_to_metadata.user_id() == target_user_id {
                            self.replied_to_by.insert((*user_id, status_id));
                        }
                    }
                    if let Some(_quoted_metadata) = quoted_metadata {
                        self.quoted_by.insert((*user_id, status_id));
                    }

                    for mentioned_user_id in mentioned_user_ids {
                        if *mentioned_user_id == target_user_id {
                            self.mentioned_by.insert((*user_id, status_id));
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

pub fn generate_user_report(lookup: &Lookup, user_id: u64) -> Result<UserReport, Error> {
    let by_screen_name = lookup.lookup_user(user_id)?;
    let status_ids = by_screen_name
        .iter()
        .flat_map(|(_, ids)| ids)
        .collect::<HashSet<_>>();

    let status_maybe_metadata = status_ids
        .iter()
        .map(|status_id| {
            lookup
                .lookup_tweet_metadata(**status_id)
                .map(|metadata| (**status_id, metadata))
        })
        .collect::<Result<HashMap<u64, _>, Error>>()?;

    let mut status_metadata = status_maybe_metadata
        .into_iter()
        .filter_map(|(k, v)| v.map(|v| (k, v)))
        .collect::<HashMap<u64, TweetMetadata>>();

    let new_status_metadata = status_metadata
        .values()
        .flat_map(|status| match status {
            TweetMetadata::Retweet { retweeted_id, .. } => {
                if !status_metadata.contains_key(retweeted_id) {
                    vec![lookup
                        .lookup_tweet_metadata(*retweeted_id)
                        .map(|v| (*retweeted_id, v))]
                } else {
                    vec![]
                }
            }
            TweetMetadata::Full {
                replied_to_id,
                quoted_id,
                ..
            } => {
                let mut new_pairs = vec![];

                if let Some(replied_to_id) = replied_to_id {
                    if !status_metadata.contains_key(replied_to_id) {
                        new_pairs.push(
                            lookup
                                .lookup_tweet_metadata(*replied_to_id)
                                .map(|v| (*replied_to_id, v)),
                        );
                    }
                }

                if let Some(quoted_id) = quoted_id {
                    if !status_metadata.contains_key(quoted_id) {
                        new_pairs.push(
                            lookup
                                .lookup_tweet_metadata(*quoted_id)
                                .map(|v| (*quoted_id, v)),
                        );
                    }
                }

                new_pairs
            }
            _ => vec![],
        })
        .collect::<Result<Vec<_>, _>>()?;

    for (k, v) in new_status_metadata {
        if let Some(v) = v {
            status_metadata.insert(k, v);
        }
    }

    let mut screen_name_dates = HashMap::new();
    let mut user_relations = UserRelations::default();
    let mut not_found = vec![];

    for (screen_name, status_ids) in by_screen_name {
        let mut first_seen = None;
        let mut last_seen = None;
        let mut count = 0;

        for status_id in status_ids {
            match status_metadata.get(&status_id) {
                Some(status) => {
                    if let Some(timestamp) = status.timestamp() {
                        let new_first_seen = first_seen
                            .filter(|value| *value <= timestamp)
                            .take()
                            .unwrap_or(timestamp);
                        first_seen = Some(new_first_seen);

                        let new_last_seen = last_seen
                            .filter(|value| *value >= timestamp)
                            .take()
                            .unwrap_or(timestamp);
                        last_seen = Some(new_last_seen);
                    }

                    count += 1;

                    user_relations.add(user_id, &status_metadata, status_id);
                }
                _ => {
                    not_found.push(status_id);
                }
            }
        }

        screen_name_dates.insert(screen_name, (first_seen.zip(last_seen), count));
    }

    let user_ids = user_relations.user_ids();
    let mut user_db = HashMap::new();

    for user_id in user_ids {
        if let Some(screen_name) = lookup.lookup_user_screen_name(user_id)? {
            user_db.insert(user_id, screen_name);
        }
    }

    Ok(UserReport {
        screen_name_dates,
        retweets: UserRelations::expand(&user_db, &user_relations.retweets),
        replies_to: UserRelations::expand(&user_db, &user_relations.replies_to),
        quotes: UserRelations::expand(&user_db, &user_relations.quotes),
        mentions: UserRelations::expand(&user_db, &user_relations.mentions),
        retweeted_by: UserRelations::expand(&user_db, &user_relations.retweeted_by),
        replied_to_by: UserRelations::expand(&user_db, &user_relations.replied_to_by),
        quoted_by: UserRelations::expand(&user_db, &user_relations.quoted_by),
        mentioned_by: UserRelations::expand(&user_db, &user_relations.mentioned_by),
        not_found,
    })
}

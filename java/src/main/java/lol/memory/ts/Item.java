package lol.memory.ts;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import lol.memory.ts.avro.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Item {
    private static final Logger logger = LoggerFactory.getLogger(Item.class);

    private final long statusId;
    private final long userId;

    public final long getStatusId() {
        return this.statusId;
    }

    public final long getUserId() {
        return this.userId;
    }

    Item(long statusId, long userId) {
        this.statusId = statusId;
        this.userId = userId;
    }

    public abstract boolean isDelete();

    public abstract boolean isTweet();

    public final Optional<Delete> asDelete() {
        if (this.isDelete()) {
            return Optional.of((Delete) this);
        } else {
            return Optional.empty();
        }
    }

    public final Optional<Tweet> asTweet() {
        if (this.isTweet()) {
            return Optional.of((Tweet) this);
        } else {
            return Optional.empty();
        }
    }

    private static Optional<Delete> decodeDelete(JSONObject value) {
        try {
            var statusObject = value.getJSONObject("status");
            Optional<Instant> timestamp = Optional.ofNullable(value.getLong("timestamp_ms")).map(Instant::ofEpochMilli);
            var statusId = statusObject.getLong("id_str");
            var userId = statusObject.getLong("user_id_str");
            if (statusId != null && userId != null) {
                return Optional.of(new Delete(statusId, userId, timestamp));
            } else {
                logger.error("Error decoding delete value ({})", value.toString());
            }
        } catch (NullPointerException | JSONException error) {
            logger.error("Error decoding delete value ({}): {}", value.toString(), error.getMessage());
        }
        return Optional.empty();
    }

    public static Optional<Tweet> decodeTweet(JSONObject value, Optional<Long> sourceStatusId,
            Optional<Instant> snapshot) {
        try {
            var statusId = value.getLong("id_str");
            var userObject = value.getJSONObject("user");
            var userId = userObject.getLong("id_str");
            var screenName = userObject.getString("screen_name");
            var name = userObject.getString("name");

            if (statusId != null && userId != null && screenName != null && name != null) {
                return Optional.of(new Tweet(statusId, sourceStatusId, snapshot, userId, screenName, name, value));
            } else {
                logger.error("Error decoding tweet ({})", value.toString());
            }
        } catch (NullPointerException | NumberFormatException error) {
            logger.error("Error decoding tweet ({}): {}", value.toString(), error.getMessage());
        }
        return Optional.empty();
    }

    public static Optional<? extends Item> fromJson(JSONObject value) {
        var deleteObject = value.getJSONObject("delete");

        if (deleteObject != null) {
            return Item.decodeDelete(deleteObject);
        } else {
            return Item.decodeTweet(value, Optional.empty(), Optional.empty());
        }
    }

    public static final class Delete extends Item {
        private final Optional<Instant> timestamp;

        public Optional<Instant> getTimestamp() {
            return this.timestamp;
        }

        public Optional<Long> getTimestampMillis() {
            return this.timestamp.map(Instant::toEpochMilli);
        }

        Delete(long statusId, long userId, Optional<Instant> timestamp) {
            super(statusId, userId);
            this.timestamp = timestamp;
        }

        public boolean isDelete() {
            return true;
        }

        public boolean isTweet() {
            return false;
        }

        @Override
        public String toString() {
            return String.format("Delete: %d %d%s", this.getStatusId(), this.getUserId(),
                    this.timestamp.map(v -> String.format(" (%s)", v.toString())).orElse(""));
        }
    }

    public static final class Tweet extends Item {
        private final Instant timestamp;
        private final Instant snapshot;
        private final long sourceStatusId;
        private final String screenName;
        private final String name;
        private final JSONObject value;
        private Instant createdAt = null;

        Tweet(long statusId, Optional<Long> sourceStatusId, Optional<Instant> snapshot, long userId, String screenName,
                String name, JSONObject value) {
            super(statusId, userId);
            this.sourceStatusId = sourceStatusId.orElse(statusId);
            this.timestamp = Twitter.extractTimestamp(statusId).orElseGet(() -> Tweet.parseCreatedAt(value, statusId));
            this.snapshot = snapshot.orElse(this.timestamp);
            this.value = value;
            this.screenName = screenName;
            this.name = name;
        }

        public long getSourceStatusId() {
            return this.sourceStatusId;
        }

        public Instant getTimestamp() {
            return this.timestamp;
        }

        public long getTimestampMillis() {
            return this.timestamp.toEpochMilli();
        }

        public Instant getSnapshot() {
            return this.snapshot;
        }

        public long getSnapshotMillis() {
            return this.snapshot.toEpochMilli();
        }

        public JSONObject getValue() {
            return this.value;
        }

        public UserInfo.Full getUserInfo() {
            return new UserInfo.Full(this.getUserId(), this.screenName, this.name);
        }

        public boolean isDelete() {
            return false;
        }

        public boolean isTweet() {
            return true;
        }

        public Optional<Tweet> getQuotedStatus() {
            return Optional.ofNullable(this.value.getJSONObject("quoted_status")).flatMap(
                    value -> Item.decodeTweet(value, Optional.of(this.sourceStatusId), Optional.of(this.snapshot)));
        }

        public Optional<Tweet> getRetweetedStatus() {
            return Optional.ofNullable(this.value.getJSONObject("retweeted_status")).flatMap(
                    value -> Item.decodeTweet(value, Optional.of(this.sourceStatusId), Optional.of(this.snapshot)));
        }

        public JSONObject getAugmentedUserObject() {
            var userObject = this.value.getJSONObject("user");
            return userObject.fluentPut("snapshot", this.snapshot.getEpochSecond());
        }

        private static Instant parseCreatedAt(JSONObject value, long statusId) {
            Instant createdAt;
            try {
                createdAt = Twitter.parseDateTime(value.getString("created_at")).get();
            } catch (NullPointerException | NoSuchElementException error) {
                createdAt = Instant.ofEpochMilli(0);
                Item.logger.error("Error decoding created_at for tweet {}: {}", statusId, error.getMessage());
            }
            return createdAt;
        }

        public Instant getCreatedAt() {
            if (this.createdAt == null) {
                this.createdAt = Tweet.parseCreatedAt(this.value, this.getStatusId());
            }
            return this.createdAt;
        }

        public Optional<ReplyInfo> getReplyInfo() {
            try {
                var inReplyToStatusId = this.value.getLong("in_reply_to_status_id_str");
                var inReplyToUserId = this.value.getLong("in_reply_to_user_id_str");
                var inReplyToScreenName = this.value.getString("in_reply_to_screen_name");

                if (inReplyToStatusId != null && inReplyToUserId != null && inReplyToScreenName != null) {
                    return Optional.of(new ReplyInfo(inReplyToStatusId, inReplyToUserId, inReplyToScreenName));
                }
            } catch (NullPointerException | JSONException error) {
                logger.error("Error decoding reply info in tweet {}: {}", this.getStatusId(), error.getMessage());
            }
            return Optional.empty();
        }

        private static JSONArray getExtendedTweetUserMentions(JSONObject value) {
            var current = value.getJSONObject("extended_tweet");

            if (current != null) {
                current = current.getJSONObject("entities");
                if (current != null) {
                    return current.getJSONArray("user_mentions");
                }
            }

            return null;
        }

        public List<UserInfo.Full> getUserMentions() {
            try {
                var userMentions = Tweet.getExtendedTweetUserMentions(this.value);

                if (userMentions == null) {
                    userMentions = this.value.getJSONObject("entities").getJSONArray("user_mentions");
                }

                List<UserInfo.Full> result = new ArrayList<>();

                for (Object obj : userMentions) {
                    var userMention = (JSONObject) obj;

                    var id = userMention.getLong("id_str");
                    var screenName = userMention.getString("screen_name");
                    var name = userMention.getString("name");

                    if (id != null && screenName != null && name != null) {
                        result.add(new UserInfo.Full(id, screenName, name));
                    } else {
                        logger.error("Error decoding user mentions in tweet {}", this.getStatusId());
                    }
                }

                return result;
            } catch (NullPointerException | ClassCastException | JSONException error) {
                logger.error("Error decoding user mentions in tweet {}: {}", this.getStatusId(), error.getMessage());
            }
            return Collections.emptyList();
        }

        private List<CharSequence> decodeWithheldInCountries(Object value) {
            if (value == null) {
                return Collections.emptyList();
            } else {
                try {
                    var asArray = (JSONArray) value;
                    List<CharSequence> result = new ArrayList<>();
                    for (Object obj : asArray) {
                        result.add((String) obj);
                    }
                    return result;
                } catch (NullPointerException | ClassCastException | JSONException error) {
                    logger.error("Error decoding withheld_in_countries for tweet {}: {}", this.getStatusId(),
                            error.getMessage());
                }
                return null;
            }
        }

        public Optional<User> getFullUser() {
            try {
                var userObject = this.value.getJSONObject("user");
                var userId = userObject.getLong("id_str");
                var screenName = userObject.getString("screen_name");
                var name = userObject.getString("name");
                var location = userObject.getString("location");
                var url = userObject.getString("url");
                var description = userObject.getString("description");
                var protected_ = userObject.getBoolean("protected");
                var verified = userObject.getBoolean("verified");
                var followersCount = userObject.getLong("followers_count");
                var friendsCount = userObject.getLong("friends_count");
                var listedCount = userObject.getLong("listed_count");
                var favouritesCount = userObject.getLong("favourites_count");
                var statusesCount = userObject.getLong("statuses_count");
                var createdAt = this.getCreatedAt();
                var profileImageUrl = userObject.getString("profile_image_url_https");
                var profileBannerUrl = userObject.getString("profile_banner_url");
                var profileBackgroundImageUrl = userObject.getString("profile_background_url_image_https");
                var defaultProfile = userObject.getBoolean("default_profile");
                var defaultProfileImage = userObject.getBoolean("default_profile_image");
                var withheldInCountries = this.decodeWithheldInCountries(userObject.get("withheld_in_countries"));
                var timeZone = userObject.getString("time_zone");
                var lang = userObject.getString("lang");
                var geoEnabled = userObject.getBoolean("geo_enabled");

                if (userId != null && screenName != null && name != null && protected_ != null && verified != null
                        && followersCount != null && friendsCount != null && listedCount != null
                        && favouritesCount != null && statusesCount != null && createdAt != null
                        && profileImageUrl != null && defaultProfile != null && defaultProfileImage != null) {
                    return Optional.of(new User(userId, this.snapshot, screenName, name, location, url, description,
                            protected_, verified, followersCount, friendsCount, listedCount, favouritesCount,
                            statusesCount, createdAt, profileImageUrl, profileBannerUrl, profileBackgroundImageUrl,
                            defaultProfile, defaultProfileImage, withheldInCountries, timeZone, lang, geoEnabled));
                } else {
                    Item.logger.error("Error decoding full user for tweet {}", this.getStatusId());
                }
            } catch (NullPointerException | ClassCastException | JSONException error) {
                Item.logger.error("Error decoding full user for tweet {}: {}", this.getStatusId(), error.getMessage());
            }
            return Optional.empty();
        }

        @Override
        public String toString() {
            return String.format("Tweet: %d %d", this.getStatusId(), this.getUserId());
        }
    }
}

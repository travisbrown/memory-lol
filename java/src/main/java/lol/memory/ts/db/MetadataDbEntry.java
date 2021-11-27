package lol.memory.ts.db;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MetadataDbEntry {
    private static final Logger logger = LoggerFactory.getLogger(MetadataDbEntry.class);

    public static Entry makeUserEntry(long userId, String screenName, long statusId) {
        return new UserEntry(userId, screenName, statusId);
    }

    public static Entry makeScreenNameEntry(long userId, String screenName) {
        return new ScreenNameEntry(userId, screenName);
    }

    public static Entry makeShortStatusEntry(long statusId, long userId) {
        return new StatusEntry(statusId, userId);
    }

    public static Entry makeFullStatusEntry(long statusId, long timestampMillis, long userId,
            Optional<Long> repliedToId, Optional<Long> quotedId, List<Long> mentionedIds) {
        return new StatusEntry(statusId, timestampMillis, userId, repliedToId, quotedId, Optional.empty(),
                mentionedIds);
    }

    public static Entry makeRetweetStatusEntry(long statusId, long timestampMillis, long userId, long retweetedId) {
        return new StatusEntry(statusId, timestampMillis, userId, Optional.empty(), Optional.empty(),
                Optional.of(retweetedId), Collections.emptyList());
    }

    public static Entry makeDeleteEntry(long userId, long statusId, Optional<Long> timestampMillis) {
        return new DeleteEntry(userId, statusId, timestampMillis);
    }

    static final class UserEntry extends Entry {
        private static final byte TAG = 0;
        private final long statusId;

        private static byte[] makeKey(long userId, String screenName) {
            byte[] screenNameBytes = screenName.getBytes(StandardCharsets.UTF_8);
            byte[] key = new byte[9 + screenNameBytes.length];
            key[0] = TAG;
            Entry.longToBytes(key, 1, userId);
            System.arraycopy(screenNameBytes, 0, key, 9, screenNameBytes.length);
            return key;
        }

        UserEntry(long userId, String screenName, long statusId) {
            super(UserEntry.makeKey(userId, screenName));
            this.statusId = statusId;
        }

        public Optional<byte[]> updateValue(Optional<byte[]> previousValue) {
            return Entry.insertValue(previousValue, this.statusId);
        }
    }

    static final class ScreenNameEntry extends Entry {
        private static final byte TAG = 1;
        private final long userId;

        private static byte[] makeKey(String screenName) {
            byte[] screenNameBytes = screenName.toLowerCase().getBytes(StandardCharsets.UTF_8);
            byte[] key = new byte[1 + screenNameBytes.length];
            key[0] = TAG;
            System.arraycopy(screenNameBytes, 0, key, 1, screenNameBytes.length);
            return key;
        }

        ScreenNameEntry(long userId, String screenName) {
            super(ScreenNameEntry.makeKey(screenName));
            this.userId = userId;
        }

        public Optional<byte[]> updateValue(Optional<byte[]> previousValue) {
            return Entry.insertValue(previousValue, this.userId);
        }
    }

    static final class StatusEntry extends Entry {
        private static final byte TAG = 2;
        // The actual number is around 295107421000000.
        private static final long FIRST_SNOWFLAKE = 250000000000000L;
        private final Optional<Long> timestampMillis;
        private final long statusId;
        private final long userId;
        private final boolean isFull;
        private final Optional<Long> repliedToId;
        private final Optional<Long> quotedId;
        private final Optional<Long> retweetedId;
        private final List<Long> mentionedIds;

        private static byte[] makeKey(long statusId) {
            byte[] key = new byte[9];
            key[0] = TAG;
            Entry.longToBytes(key, 1, statusId);
            return key;
        }

        StatusEntry(long statusId, long userId) {
            super(StatusEntry.makeKey(statusId));
            this.statusId = statusId;
            this.timestampMillis = Optional.empty();
            this.userId = userId;
            this.isFull = false;
            this.repliedToId = Optional.empty();
            this.quotedId = Optional.empty();
            this.retweetedId = Optional.empty();
            this.mentionedIds = Collections.emptyList();
        }

        StatusEntry(long statusId, long timestampMillis, long userId, Optional<Long> repliedToId,
                Optional<Long> quotedId, Optional<Long> retweetedId, List<Long> mentionedIds) {
            super(StatusEntry.makeKey(statusId));
            this.statusId = statusId;
            this.timestampMillis = Optional.of(timestampMillis);
            this.userId = userId;
            this.isFull = true;
            this.repliedToId = repliedToId;
            this.quotedId = quotedId;
            this.retweetedId = retweetedId;
            this.mentionedIds = mentionedIds;
        }

        // We only encode timestamps for pre-Snowflake IDs.
        private boolean timestampNeeded() {
            return this.statusId < FIRST_SNOWFLAKE;
        }

        private boolean isFullValue(byte[] value) {
            // Short values have no tag, only a user ID.
            return value.length != 8;
        }

        private byte getStatusTag() {
            if (this.retweetedId.isPresent()) {
                return 4;
            } else if (this.repliedToId.isPresent()) {
                if (this.quotedId.isPresent()) {
                    return 3;
                } else {
                    return 1;
                }
            } else {
                if (this.quotedId.isPresent()) {
                    return 2;
                } else {
                    return 0;
                }
            }
        }

        public Optional<byte[]> updateValue(Optional<byte[]> previousValue) {
            if (previousValue.isPresent()) {
                if (isFullValue(previousValue.get())) {
                    // Skip replacing a full value with anything.
                    return Optional.empty();
                } else {
                    if (!this.isFull) {
                        // Skip replacing a short value with a short value.
                        return Optional.empty();
                    }
                }
            }

            int length = 8;
            if (this.isFull) {
                length += 1;
                if (this.timestampNeeded()) {
                    length += 8;
                }
                if (this.retweetedId.isPresent()) {
                    length += 8;
                } else {
                    if (this.repliedToId.isPresent()) {
                        length += 8;
                    }
                    if (this.quotedId.isPresent()) {
                        length += 8;
                    }
                    length += (this.mentionedIds.size() * 8);
                }
            }

            byte[] result = new byte[length];
            ByteBuffer buffer = ByteBuffer.wrap(result);
            buffer.putLong(this.userId);

            if (this.isFull) {
                if (this.timestampNeeded()) {
                    buffer.putLong(this.timestampMillis.get());
                }
                buffer.put(this.getStatusTag());
                if (this.retweetedId.isPresent()) {
                    buffer.putLong(this.retweetedId.get());
                } else {
                    if (this.repliedToId.isPresent()) {
                        buffer.putLong(this.repliedToId.get());
                    }
                    if (this.quotedId.isPresent()) {
                        buffer.putLong(this.quotedId.get());
                    }
                    for (long mentionedId : this.mentionedIds) {
                        buffer.putLong(mentionedId);
                    }
                }
            }

            return Optional.of(result);
        }
    }

    static final class DeleteEntry extends Entry {
        private static final byte TAG = 3;
        private final Optional<Long> timestampMillis;

        private static byte[] makeKey(long userId, long statusId) {
            byte[] key = new byte[17];
            key[0] = TAG;
            Entry.longToBytes(key, 1, userId);
            Entry.longToBytes(key, 9, statusId);
            return key;
        }

        DeleteEntry(long userId, long statusId, Optional<Long> timestampMillis) {
            super(DeleteEntry.makeKey(userId, statusId));
            this.timestampMillis = timestampMillis;
        }

        public Optional<byte[]> updateValue(Optional<byte[]> previousValue) {
            if (previousValue.isPresent()) {
                return Optional.empty();
            } else {
                if (timestampMillis.isPresent()) {
                    var result = new byte[8];
                    Entry.longToBytes(result, 0, timestampMillis.get());
                    return Optional.of(result);
                } else {
                    return Optional.of(new byte[0]);
                }
            }
        }
    }
}

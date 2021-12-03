package lol.memory.ts.db;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class StatusValue {
    private final int tag;
    private final long userId;
    private final long timestamp;
    private final Optional<Long> retweetedStatusId;
    private final Optional<Long> replyToStatusId;
    private final Optional<Long> quotedStatusId;
    private final long[] mentionedIds;

    private StatusValue(int tag, long userId, long timestamp, Optional<Long> retweetedStatusId,
            Optional<Long> replyToStatusId, Optional<Long> quotedStatusId, long[] mentionedIds) {
        this.tag = tag;
        this.userId = userId;
        this.timestamp = timestamp;
        this.retweetedStatusId = retweetedStatusId;
        this.replyToStatusId = replyToStatusId;
        this.quotedStatusId = quotedStatusId;
        this.mentionedIds = mentionedIds;
    }

    public static StatusValue ofRetweet(long userId, long timestamp, long retweetedStatusId) {
        return new StatusValue(4, userId, timestamp, Optional.of(retweetedStatusId), Optional.empty(), Optional.empty(),
                new long[0]);
    }

    private static int getTag(boolean hasReplyToStatusId, boolean hasQuotedStatusId) {
        if (hasReplyToStatusId) {
            return hasQuotedStatusId ? 3 : 1;
        } else {
            return hasQuotedStatusId ? 2 : 0;
        }
    }

    public static StatusValue ofTweet(long userId, long timestamp, Optional<Long> replyToStatusId,
            Optional<Long> quotedStatusId, Collection<Long> mentionedIds) {
        int tag = StatusValue.getTag(replyToStatusId.isPresent(), quotedStatusId.isPresent());
        var sortedMentionedIdList = mentionedIds.stream().distinct().sorted().collect(Collectors.toList());

        long[] mentionedIdArray = sortedMentionedIdList.stream().mapToLong(value -> value).toArray();

        return new StatusValue(tag, userId, timestamp, Optional.empty(), replyToStatusId, quotedStatusId,
                mentionedIdArray);
    }

    public byte[] toByteArray() {
        int size = 17 + this.mentionedIds.length * 8;
        if (this.retweetedStatusId.isPresent()) {
            size += 8;
        }
        if (this.replyToStatusId.isPresent()) {
            size += 8;
        }
        if (this.quotedStatusId.isPresent()) {
            size += 8;
        }

        byte[] result = new byte[size];
        result[0] = (byte) tag;
        Entry.writeLong(result, 1, this.userId);
        Entry.writeLong(result, 9, this.timestamp);

        if (tag == 4) {
            Entry.writeLong(result, 17, this.retweetedStatusId.get());
        } else {
            if (tag == 1 || tag == 3) {
                Entry.writeLong(result, 17, this.replyToStatusId.get());
            }

            if (tag == 2) {
                Entry.writeLong(result, 17, this.quotedStatusId.get());
            } else if (tag == 3) {
                Entry.writeLong(result, 25, this.quotedStatusId.get());
            }

            int mentionedIdsOffset = (tag == 0 ? 2 : ((tag == 1 || tag == 2) ? 3 : 4)) * 8 + 1;

            for (int i = 0; i < this.mentionedIds.length; i += 1) {
                Entry.writeLong(result, mentionedIdsOffset + i * 8, this.mentionedIds[i]);
            }
        }

        return result;
    }

    public static StatusValue parseByteArray(byte[] bytes) {
        if (bytes.length < 17) {
            throw new StatusValueParseException("too few bytes", bytes);
        }

        int tag = bytes[0];
        if (tag < 0 || tag > 4) {
            throw new StatusValueParseException("invalid tag", bytes);
        }

        byte[] rest = new byte[bytes.length - 1];
        System.arraycopy(bytes, 1, rest, 0, bytes.length - 1);

        if (rest.length % 8 != 0) {
            throw new StatusValueParseException("invalid longs", bytes);
        }

        long[] values = Entry.bytesToLongs(rest);
        long userId = values[0];
        long timestamp = values[1];

        if (tag == 4) {
            if (values.length != 3) {
                throw new StatusValueParseException("invalid retweet", bytes);
            }
            return StatusValue.ofRetweet(userId, timestamp, values[2]);
        }

        int mentionedIdsOffset = tag == 0 ? 2 : ((tag == 1 || tag == 2) ? 3 : 4);
        long[] mentionedIds = new long[values.length - mentionedIdsOffset];
        System.arraycopy(values, mentionedIdsOffset, mentionedIds, 0, values.length - mentionedIdsOffset);

        Optional<Long> replyToStatusId;
        Optional<Long> quotedStatusId;

        if (tag == 1 || tag == 3) {
            replyToStatusId = Optional.of(values[2]);
        } else {
            replyToStatusId = Optional.empty();
        }
        if (tag == 2) {
            quotedStatusId = Optional.of(values[2]);
        } else if (tag == 3) {
            quotedStatusId = Optional.of(values[3]);
        } else {
            quotedStatusId = Optional.empty();
        }

        return new StatusValue(tag, userId, timestamp, Optional.empty(), replyToStatusId, quotedStatusId, mentionedIds);
    }

    public StatusValue merge(StatusValue other) {
        if (this.tag != other.tag) {
            throw new StatusValueMergeException(String.format("different tags: %d, %d", this.tag, other.tag),
                    this.tag < other.tag);
        }

        if (this.userId != other.userId) {
            throw new StatusValueMergeException(String.format("different user IDs: %d, %d", this.userId, other.userId),
                    true);
        }

        if (this.timestamp != other.timestamp) {
            throw new StatusValueMergeException(
                    String.format("different timestamps: %d, %d", this.timestamp, other.timestamp), true);
        }

        if (tag == 1 || tag == 3) {
            if (!this.replyToStatusId.get().equals(other.replyToStatusId.get())) {
                throw new StatusValueMergeException(String.format("different reply-to status IDs: %s, %s",
                        this.replyToStatusId, other.replyToStatusId), true);
            }
        }

        if (tag == 2 || tag == 3) {
            if (!this.quotedStatusId.get().equals(other.quotedStatusId.get())) {
                throw new StatusValueMergeException(
                        String.format("different quoted status IDs: %s, %s", this.quotedStatusId, other.quotedStatusId),
                        true);
            }
        }

        if (Arrays.equals(this.mentionedIds, other.mentionedIds)) {
            return this;
        } else {
            Set<Long> mentionedIds = new HashSet<Long>();

            for (int i = 0; i < this.mentionedIds.length; i += 1) {
                mentionedIds.add(this.mentionedIds[i]);
            }

            for (int i = 0; i < other.mentionedIds.length; i += 1) {
                mentionedIds.add(other.mentionedIds[i]);
            }

            return StatusValue.ofTweet(this.userId, this.timestamp, this.replyToStatusId, this.quotedStatusId,
                    mentionedIds);
        }
    }

    private static String formatBytes(byte[] bytes) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < bytes.length - 1; i += 1) {
            builder.append(String.format("%d, ", bytes[i]));
        }
        builder.append(String.format("%d]", bytes[bytes.length - 1]));
        return builder.toString();
    }

    public static class StatusValueParseException extends RuntimeException {
        StatusValueParseException(String message, byte[] bytes) {
            super(String.format("Invalid status value bytes (%s): %d", message, StatusValue.formatBytes(bytes)));
        }
    }

    public static class StatusValueMergeException extends RuntimeException {
        private final boolean shouldAbort;

        StatusValueMergeException(String message, boolean shouldAbort) {
            super(String.format("Invalid status value merge: %s", message));
            this.shouldAbort = shouldAbort;
        }

        public boolean shouldAbort() {
            return this.shouldAbort;
        }
    }
}

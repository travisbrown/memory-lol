package lol.memory.ts;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Miscellaneous Twitter utilties.
 */
public final class Twitter {
    private static final Logger logger = LoggerFactory.getLogger(Twitter.class);
    private static final long SNOWFLAKE_MINIMUM = 100000000000000L;
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("EE MMM dd HH:mm:ss Z yyyy");

    /**
     * Infer a timestamp from a Twitter status ID if it's a Snowflake ID.
     */
    public static Optional<Instant> extractTimestamp(long statusId) {
        return Twitter.extractTimestampMillis(statusId).map(Instant::ofEpochMilli);
    }

    /**
     * Infer a timestamp (as epoch millisecond) from a Twitter status ID if it's a Snowflake ID.
     */
    public static Optional<Long> extractTimestampMillis(long statusId) {
        if (statusId > Twitter.SNOWFLAKE_MINIMUM) {
            return Optional.of((statusId >> 22) + 1288834974657L);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Parse a datetime value from the Twitter API.
     */
    public static Optional<Instant> parseDateTime(String input) {
        try {
            return Optional.of(ZonedDateTime.parse(input, Twitter.DATE_FORMAT).toInstant());
        } catch (DateTimeParseException error) {
            Twitter.logger.error("Error parsing Twitter datetime ({}): {}", input, error.getMessage());
        }
        return Optional.empty();
    }

    protected Twitter() {
        throw new UnsupportedOperationException();
    }
}

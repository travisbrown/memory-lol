package lol.memory.ts.db;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import lol.memory.ts.avro.User;
import org.apache.avro.message.RawMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UserDbEntry {
    private static final Logger logger = LoggerFactory.getLogger(UserDbEntry.class);

    public static Entry makeUserEntry(User user, long timestampMillis) {
        return new UserEntry(user, timestampMillis);
    }

    static final class UserEntry extends Entry {
        private static final byte TAG = 0;
        private final User user;

        private static byte[] makeKey(long userId, long timestampMillis) {
            byte[] key = new byte[17];
            key[0] = TAG;
            Entry.longToBytes(key, 1, userId);
            Entry.longToBytes(key, 9, timestampMillis);
            return key;
        }

        UserEntry(User user, long timestampMillis) {
            super(UserEntry.makeKey(user.getId(), timestampMillis));
            this.user = user;
        }

        public Optional<byte[]> updateValue(Optional<byte[]> previousValue) {
            if (previousValue.isPresent()) {
                return Optional.empty();
            } else {
                try {
                    var encoder = new RawMessageEncoder<User>(user.getSpecificData(), user.getSchema());
                    var stream = new ByteArrayOutputStream();
                    encoder.encode(UserEntry.this.user, stream);
                    stream.close();
                    var bytes = stream.toByteArray();
                    return Optional.of(bytes);
                } catch (IOException error) {
                    UserDbEntry.logger.error("Error encoding user {} as Avro: {}", user.getId(), error);
                }
                return Optional.empty();
            }
        }
    }
}

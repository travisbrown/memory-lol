package lol.memory.ts.db;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lol.memory.ts.archive.Archive;
import lol.memory.ts.archive.Record;
import lol.memory.ts.avro.User;
import lol.memory.ts.Item;
import lol.memory.ts.UserInfo;
import lol.memory.ts.Util;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportUsers {
    private static final Logger logger = LoggerFactory.getLogger(ImportUsers.class);

    public static void main(String[] args) throws IOException, RocksDBException {
        RocksDB.loadLibrary();
        var archive = Archive.load(new File(args[0]));
        var dbPath = args[1];
        Set<Long> selectedUserIds = (args.length > 2) ? Util.readLongs(new File(args[2])) : Collections.emptySet();

        var importer = ImportUsers.create(dbPath, selectedUserIds::contains);
        archive.run(importer);
    }

    public static Consumer<Record<Item>> create(String dbPath, Predicate<Long> selector)
            throws IOException, RocksDBException {
        return new Consumer<Record<Item>>() {
            private final Database db = new Database(dbPath);

            public void accept(Record<Item> record) {
                try {
                    var item = record.getValue();
                    if (item.isTweet()) {
                        var users = new HashSet<User>();
                        var tweet = item.asTweet().get();

                        if (selector.test(item.getUserId())) {
                            tweet.getFullUser().ifPresent(users::add);
                        }

                        tweet.getQuotedStatus().flatMap(
                                status -> selector.test(status.getUserId()) ? status.getFullUser() : Optional.empty())
                                .ifPresent(users::add);
                        tweet.getRetweetedStatus().flatMap(
                                status -> selector.test(status.getUserId()) ? status.getFullUser() : Optional.empty())
                                .ifPresent(users::add);

                        if (!users.isEmpty()) {
                            var tx = db.beginTransaction();
                            for (User user : users) {
                                db.insert(tx, UserDbEntry.makeUserEntry(user, tweet.getTimestampMillis()));
                            }
                            tx.commit();
                        }
                    }
                } catch (Throwable error) {
                    ImportUsers.logger.error("Error during RocksDB writing ({}, {}): {}",
                            record.getFilePath().orElseGet(() -> "<none>"), record.getLineNumber(), error.getMessage());
                }
            }
        };
    }

    protected ImportUsers() {
        throw new UnsupportedOperationException();
    }
}

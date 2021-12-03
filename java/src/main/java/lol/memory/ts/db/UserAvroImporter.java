package lol.memory.ts.db;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lol.memory.ts.archive.Archive;
import lol.memory.ts.archive.FileResult;
import lol.memory.ts.archive.FileResultConsumer;
import lol.memory.ts.avro.User;
import lol.memory.ts.Item;
import lol.memory.ts.Util;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class UserAvroImporter extends FileResultConsumer<Item> {
    private static final Logger logger = LoggerFactory.getLogger(UserAvroImporter.class);
    private final Database db;
    private final Set<Long> selectedUserIds;

    public static void main(String[] args) throws IOException, RocksDBException {
        RocksDB.loadLibrary();
        var archive = Archive.load(new File(args[0]));
        var dbPath = args[1];
        Set<Long> selectedUserIds = (args.length > 2) ? Util.readLongs(new File(args[2])) : Collections.emptySet();

        var importer = new UserAvroImporter(dbPath, selectedUserIds);
        archive.process(importer);
    }

    public UserAvroImporter(String dbPath, Set<Long> selectedUserIds) throws RocksDBException {
        this.db = new Database(dbPath);
        this.selectedUserIds = selectedUserIds;
    }

    public boolean skipFile(Path archivePath, String filePath) {
        return false;
    }

    public boolean accept(Path archivePath, FileResult<Item> result) throws Exception {
        for (Optional<Item> maybeItem : result.getValues()) {
            if (maybeItem.isPresent() && maybeItem.get().isTweet()) {
                try {
                    var users = new HashSet<User>();
                    var tweet = maybeItem.get().asTweet().get();

                    if (this.selectedUserIds.contains(tweet.getUserId())) {
                        tweet.getFullUser().ifPresent(users::add);
                    }

                    tweet.getQuotedStatus().flatMap(status -> this.selectedUserIds.contains(status.getUserId())
                            ? status.getFullUser() : Optional.empty()).ifPresent(users::add);
                    tweet.getRetweetedStatus().flatMap(status -> this.selectedUserIds.contains(status.getUserId())
                            ? status.getFullUser() : Optional.empty()).ifPresent(users::add);

                    if (!users.isEmpty()) {
                        var tx = db.beginTransaction();
                        for (User user : users) {
                            db.insert(tx, UserDbEntry.makeUserEntry(user, tweet.getTimestampMillis()));
                        }
                        tx.commit();
                    }

                } catch (Throwable error) {
                    UserAvroImporter.logger.error("RocksDb error ({}, {}): {}", archivePath, result.getPath(),
                            error.getMessage());
                    throw error;
                }
            }
        }

        return true;
    }
}

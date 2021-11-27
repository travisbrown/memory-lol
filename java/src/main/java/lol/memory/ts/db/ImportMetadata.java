package lol.memory.ts.db;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lol.memory.ts.archive.Archive;
import lol.memory.ts.archive.Record;
import lol.memory.ts.Item;
import lol.memory.ts.UserInfo;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportMetadata {
    private static final Logger logger = LoggerFactory.getLogger(ImportMetadata.class);

    public static void main(String[] args) throws IOException, RocksDBException {
        RocksDB.loadLibrary();
        var archive = Archive.load(new File(args[0]));
        var dbPath = args[1];

        var importer = ImportMetadata.create(dbPath);
        archive.run(importer);
    }

    public static Consumer<Record<Item>> create(String dbPath) throws IOException, RocksDBException {
        return new Consumer<Record<Item>>() {
            private final Database db = new Database(dbPath);

            public void accept(Record<Item> record) {
                try {
                    var tx = db.beginTransaction();
                    var item = record.getValue();

                    if (item.isDelete()) {
                        var delete = item.asDelete().get();
                        var entry = MetadataDbEntry.makeDeleteEntry(delete.getUserId(), delete.getStatusId(),
                                delete.getTimestampMillis());
                        db.insert(tx, entry);
                    } else {
                        ImportMetadata.processTweet(db, tx, item.asTweet().get());
                    }

                    tx.commit();
                } catch (Throwable error) {
                    ImportMetadata.logger.error("Error during RocksDB writing ({}, {}): {}",
                            record.getFilePath().orElseGet(() -> "<none>"), record.getLineNumber(), error.getMessage());
                }
            }
        };
    }

    private static void processTweet(Database db, Transaction tx, Item.Tweet tweet)
            throws IOException, RocksDBException {
        var userInfo = tweet.getUserInfo();

        db.insert(tx, MetadataDbEntry.makeUserEntry(userInfo.getUserId(), userInfo.getScreenName(),
                tweet.getSourceStatusId()));
        db.insert(tx, MetadataDbEntry.makeScreenNameEntry(userInfo.getUserId(), userInfo.getScreenName()));

        var maybeRetweetedStatus = tweet.getRetweetedStatus();

        if (maybeRetweetedStatus.isPresent()) {
            var retweetedStatus = maybeRetweetedStatus.get();

            ImportMetadata.processTweet(db, tx, retweetedStatus);
            db.insert(tx, MetadataDbEntry.makeRetweetStatusEntry(tweet.getStatusId(), tweet.getTimestampMillis(),
                    userInfo.getUserId(), retweetedStatus.getStatusId()));
        } else {
            var maybeReplyInfo = tweet.getReplyInfo();

            if (maybeReplyInfo.isPresent()) {
                var replyInfo = maybeReplyInfo.get();

                db.insert(tx, MetadataDbEntry.makeUserEntry(replyInfo.getUserId(), replyInfo.getScreenName(),
                        tweet.getSourceStatusId()));
                db.insert(tx, MetadataDbEntry.makeScreenNameEntry(replyInfo.getUserId(), replyInfo.getScreenName()));
                db.insert(tx, MetadataDbEntry.makeShortStatusEntry(replyInfo.getStatusId(), replyInfo.getUserId()));
            }

            var quotedStatus = tweet.getQuotedStatus();

            if (quotedStatus.isPresent()) {
                ImportMetadata.processTweet(db, tx, quotedStatus.get());
            }

            var mentionedIds = new HashSet<Long>();
            for (UserInfo.Full user : tweet.getUserMentions()) {
                mentionedIds.add(user.getUserId());
            }

            var entry = MetadataDbEntry.makeFullStatusEntry(tweet.getStatusId(), tweet.getTimestampMillis(),
                    userInfo.getUserId(), maybeReplyInfo.map(replyInfo -> replyInfo.getStatusId()),
                    quotedStatus.map(value -> value.getStatusId()),
                    mentionedIds.stream().distinct().sorted().collect(Collectors.toList()));

            db.insert(tx, entry);
        }
    }

    protected ImportMetadata() {
        throw new UnsupportedOperationException();
    }
}

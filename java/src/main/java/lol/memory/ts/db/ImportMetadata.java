package lol.memory.ts.db;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.stream.Collectors;
import lol.memory.ts.archive.Archive;
import lol.memory.ts.archive.Record;
import lol.memory.ts.archive.RecordConsumer;
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

        var base = new File(args[0]);
        var files = new ArrayList<File>();

        if (base.isFile()) {
            files.add(base);
        } else {
            for (File file : base.listFiles()) {
                var name = file.getName().toLowerCase();
                if (file.isFile() && (name.endsWith("tar") || name.endsWith("zip"))) {
                    files.add(file);
                }
            }
        }

        files.sort(Comparator.comparing(file -> file.getName()));

        var dbPath = args[1];
        var importer = ImportMetadata.create(dbPath);

        for (File file : files) {
            ImportMetadata.logger.info("Importing metadata from {}", file.getName());
            var archive = Archive.load(file);

            archive.run(importer);
        }
    }

    public static RecordConsumer<Item> create(String dbPath) throws IOException, RocksDBException {
        return new DbRecordConsumer<Item>() {
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

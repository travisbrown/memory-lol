package lol.memory.ts.db;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lol.memory.ts.archive.Archive;
import lol.memory.ts.archive.FileResult;
import lol.memory.ts.archive.FileResultConsumer;
import lol.memory.ts.Item;
import lol.memory.ts.UserInfo;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MetadataImporter extends FileResultConsumer<MetadataBatch> {
    private static final Logger logger = LoggerFactory.getLogger(MetadataImporter.class);
    private static final byte COMPLETED_FILE_NAME_TAG = 16;
    private static final byte[] COMPLETED_FILE_NAME_PREFIX = new byte[] { MetadataImporter.COMPLETED_FILE_NAME_TAG };

    private final Database db;
    private final Map<String, Set<String>> completed;
    private final AtomicInteger counter = new AtomicInteger();
    public java.io.DataOutputStream out;

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
        var importer = new MetadataImporter(dbPath);
        importer.out = new java.io.DataOutputStream(new java.io.FileOutputStream("kvs.dat"));

        for (File file : files) {
            MetadataImporter.logger.info("Importing metadata from {}", file.getName());
            var archive = Archive.load(file);

            archive.processWithTransformation(MetadataImporter.ITEMS_TO_BATCH, importer);
        }

        importer.out.close();
    }

    public MetadataImporter(String dbPath) throws RocksDBException {
        this.db = new Database(dbPath);
        this.completed = MetadataImporter.loadCompletedFileMap(this.db);
    }

    public boolean skipFile(Path archivePath, String filePath) {
        var entry = this.completed.get(archivePath.getFileName().toString());
        if (entry == null) {
            return false;
        } else {
            if (entry.contains(filePath)) {
                MetadataImporter.logger.info("Skipping file: {}", filePath);
                return true;
            } else {
                return false;
            }
        }
    }

    private static byte[] makeCompletedFileKey(String archivePath, String filePath) {
        String keyString = String.format("%s|%s", archivePath, filePath);
        byte[] keyStringBytes = keyString.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[1 + keyStringBytes.length];
        key[0] = MetadataImporter.COMPLETED_FILE_NAME_TAG;
        System.arraycopy(keyStringBytes, 0, key, 1, keyStringBytes.length);
        return key;
    }

    public static final Function<List<Optional<Item>>, MetadataBatch> ITEMS_TO_BATCH = new Function<List<Optional<Item>>, MetadataBatch>() {
        public MetadataBatch apply(List<Optional<Item>> items) {
            var batch = new MetadataBatch();

            for (Optional<Item> item : items) {
                if (item.isPresent()) {
                    batch.addItem(item.get());
                }
            }

            return batch;
        }
    };

    public boolean accept(Path archivePath, FileResult<MetadataBatch> result) throws Exception {
        /*
         * if (this.counter.get() % 1000 == 999) { try { MetadataImporter.logger.info("Compacting database");
         * this.db.compact(); } catch (RocksDBException error) {
         * MetadataImporter.logger.error("Error compacting database: {}", error.getMessage()); } }
         */

        MetadataBatch batch = result.getValue();
        Key[] allKeys = new Key[batch.users.size() + batch.screenNames.size() + batch.statuses.size()];
        byte[][] rawKeys = new byte[batch.users.size() + batch.screenNames.size() + batch.statuses.size()][];
        int i = 0;


        for (Key key : batch.users.keySet()) {
            allKeys[i] = key;
            rawKeys[i] = key.bytes;
            i += 1;
        }

        for (Key key : batch.screenNames.keySet()) {
            allKeys[i] = key;
            rawKeys[i] = key.bytes;
            i += 1;
        }

        for (Key key : batch.statuses.keySet()) {
            allKeys[i] = key;
            rawKeys[i] = key.bytes;
            i += 1;
        }

        try {
            var tx = this.db.beginTransaction();
            //System.out.println("Before multiget");
            //var values = tx.multiGet(Database.getReadOptions(), rawKeys);
            //System.out.println("After multiget");
            int j = 0;

            for (; j < batch.users.size(); j += 1) {
                var key = allKeys[j];
                //var value = values[j];
                var newValues = batch.users.get(key);

                /*
                if (value != null) {
                    var longs = Entry.bytesToLongs(value);

                    for (int k = 0; k < longs.length; k += 1) {
                        newValues.add(longs[k]);
                    }
                }*/

                List<Long> asList = new ArrayList<>(newValues);
                java.util.Collections.sort(asList);

                var valBytes = Entry.longsToBytes(asList);

                out.writeInt(key.bytes.length);
                out.write(key.bytes);
                out.writeInt(valBytes.length);
                out.write(valBytes);

                //tx.put(key.bytes, Entry.longsToBytes(asList));
            }

            for (; j < batch.users.size() + batch.screenNames.size(); j += 1) {
                var key = allKeys[j];
                //var value = values[j];
                var newValues = batch.screenNames.get(key);

                /*if (value != null) {
                    var longs = Entry.bytesToLongs(value);
                    for (int k = 0; k < longs.length; k += 1) {
                        newValues.add(longs[k]);
                    }
                }*/

                List<Long> asList = new ArrayList<>(newValues);
                java.util.Collections.sort(asList);

                var valBytes = Entry.longsToBytes(asList);

                out.writeInt(key.bytes.length);
                out.write(key.bytes);
                out.writeInt(valBytes.length);
                out.write(valBytes);

                //tx.put(key.bytes, Entry.longsToBytes(asList));
            }

            for (; j < batch.users.size() + batch.screenNames.size() + batch.statuses.size(); j += 1) {
                var key = allKeys[j];
                //var value = values[j];
                var newValue = batch.statuses.get(key);
                boolean abort = false;

                /*if (value != null) {
                    if (!Arrays.equals(value, newValue)) {
                        var current = StatusValue.parseByteArray(newValue);
                        var previous = StatusValue.parseByteArray(value);

                        try {
                            var updated = current.merge(previous);

                            if (updated != current) {
                                var statusId = Entry.readLong(key.bytes, 1);

                                MetadataImporter.logger.warn("Updated existing status {} ({}, {})", statusId,
                                        archivePath, result.getPath());
                                newValue = updated.toByteArray();
                            }
                        } catch (StatusValue.StatusValueMergeException error) {
                            var statusId = Entry.readLong(key.bytes, 1);

                            MetadataImporter.logger.error("Error processing status {} ({}, {}): {}", statusId,
                                    archivePath, result.getPath(), error.getMessage());
                            abort = error.shouldAbort();
                        }
                    }
                }

                if (!abort) {
                    tx.put(key.bytes, newValue);
                }*/

                out.writeInt(key.bytes.length);
                out.write(key.bytes);
                out.writeInt(newValue.length);
                out.write(newValue);
            }

            /*for (Map.Entry<Key, Long> entry : batch.shortStatuses.entrySet()) {
                byte[] value = new byte[8];
                Entry.writeLong(value, 0, entry.getValue());
                tx.put(entry.getKey().bytes, value);
            }

            for (Map.Entry<Key, Optional<Long>> entry : batch.deletes.entrySet()) {
                byte[] value;
                if (!entry.getValue().isPresent()) {
                    value = new byte[0];
                } else {
                    value = new byte[8];
                    Entry.writeLong(value, 0, entry.getValue().get());
                }
                tx.put(entry.getKey().bytes, value);
            }

            var completedFileKey = MetadataImporter.makeCompletedFileKey(archivePath.getFileName().toString(),
                    result.getPath());
            var completedFileValue = new byte[8];
            Entry.writeLong(completedFileValue, 0, batch.statuses.size());
            tx.put(completedFileKey, completedFileValue);

            tx.commit();
            */
        //} catch (RocksDBException error) {
        } catch (IOException error) {
            MetadataImporter.logger.error("RocksDb error ({}, {}): {}", archivePath, result.getPath(),
                    error.getMessage());
            return false;
        }

        MetadataImporter.logger.info("Completed file ({}; multiget keys: {}): {}", this.counter.get(), allKeys.length,
                result.getPath());

        this.counter.incrementAndGet();

        return true;
    }

    private static Map<String, Set<String>> loadCompletedFileMap(Database db) throws RocksDBException {
        Map<String, Set<String>> result = new HashMap<String, Set<String>>();

        int count = 0;
        var iterator = db.prefixIterator(MetadataImporter.COMPLETED_FILE_NAME_PREFIX);

        for (; iterator.isValid(); iterator.next()) {
            byte[] key = iterator.key();
            if (key[0] != MetadataImporter.COMPLETED_FILE_NAME_TAG) {
                break;
            }

            String keyString = new String(key, 1, key.length - 1);
            String[] parts = keyString.split("\\|");

            var entry = result.get(parts[0]);
            if (entry == null) {
                var newEntry = new HashSet<String>();
                newEntry.add(parts[1]);
                result.put(parts[0], newEntry);
            } else {
                entry.add(parts[1]);
            }
            count += 1;
        }

        MetadataImporter.logger.info("Loaded {} known files", count);
        return result;
    }
}

final class Key {
    final byte[] bytes;

    public Key(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public boolean equals(Object that) {
        if (that == this) {
            return true;
        } else if (!(that instanceof Key)) {
            return false;
        } else {
            return Arrays.equals(this.bytes, ((Key) that).bytes);
        }
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.bytes);
    }
}

final class MetadataBatch {
    private static final Logger logger = LoggerFactory.getLogger(MetadataBatch.class);

    final Map<Key, Set<Long>> users = new HashMap<>();
    final Map<Key, Set<Long>> screenNames = new HashMap<>();
    final Map<Key, byte[]> statuses = new HashMap<>();
    final Map<Key, Long> shortStatuses = new HashMap<>();
    final Map<Key, Optional<Long>> deletes = new HashMap<>();

    private static byte[] makeUserKey(long userId, String screenName) {
        byte[] screenNameBytes = screenName.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[9 + screenNameBytes.length];
        key[0] = 0;
        Entry.writeLong(key, 1, userId);
        System.arraycopy(screenNameBytes, 0, key, 9, screenNameBytes.length);
        return key;
    }

    private static byte[] makeScreenNameKey(String screenName) {
        byte[] screenNameBytes = screenName.toLowerCase().getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[1 + screenNameBytes.length];
        key[0] = 1;
        System.arraycopy(screenNameBytes, 0, key, 1, screenNameBytes.length);
        return key;
    }

    private static byte[] makeFullStatusKey(long statusId) {
        byte[] key = new byte[9];
        key[0] = 2;
        Entry.writeLong(key, 1, statusId);
        return key;
    }

    private static byte[] makeShortStatusKey(long statusId) {
        byte[] key = new byte[9];
        key[0] = 3;
        Entry.writeLong(key, 1, statusId);
        return key;
    }

    private static byte[] makeDeleteKey(long userId, long statusId) {
        byte[] key = new byte[17];
        key[0] = 4;
        Entry.writeLong(key, 1, userId);
        Entry.writeLong(key, 9, statusId);
        return key;
    }

    private void addUser(long userId, String screenName, long sourceStatusId) {
        /*
         * if (userId == 204277565) { System.out.printf("HIT: %d\n", sourceStatusId); }
         */

        var userKey = new Key(MetadataBatch.makeUserKey(userId, screenName));
        var userValue = this.users.get(userKey);
        if (userValue == null) {
            userValue = new HashSet<>();
            this.users.put(userKey, userValue);
        }
        userValue.add(sourceStatusId);

        var screenNameKey = new Key(MetadataBatch.makeScreenNameKey(screenName));
        var screenNameValue = this.screenNames.get(screenNameKey);
        if (screenNameValue == null) {
            screenNameValue = new HashSet<>();
            this.screenNames.put(screenNameKey, screenNameValue);
        }
        screenNameValue.add(userId);
    }

    public void addItem(Item item) {
        if (item.isDelete()) {
            var delete = item.asDelete().get();
            this.deletes.put(new Key(makeDeleteKey(delete.getUserId(), delete.getStatusId())),
                    delete.getTimestampMillis());
        } else {
            this.addTweet(item.asTweet().get());
        }
    }

    public void addTweet(Item.Tweet tweet) {
        var userInfo = tweet.getUserInfo();

        this.addUser(userInfo.getUserId(), userInfo.getScreenName(), tweet.getSourceStatusId());

        var maybeRetweetedStatus = tweet.getRetweetedStatus();
        var maybeReplyInfo = tweet.getReplyInfo();
        var maybeQuotedStatusId = tweet.getQuotedStatusId();
        var maybeQuotedStatus = tweet.getQuotedStatus();

        StatusValue statusValue;

        if (maybeRetweetedStatus.isPresent()) {
            var retweetedStatus = maybeRetweetedStatus.get();

            this.addTweet(retweetedStatus);
            statusValue = StatusValue.ofRetweet(userInfo.getUserId(), tweet.getTimestampMillis(),
                    retweetedStatus.getStatusId());
        } else {
            if (maybeReplyInfo.isPresent()) {
                var replyInfo = maybeReplyInfo.get();
                this.addUser(replyInfo.getUserId(), replyInfo.getScreenName(), tweet.getSourceStatusId());
                this.shortStatuses.put(new Key(MetadataBatch.makeShortStatusKey(replyInfo.getStatusId())),
                        replyInfo.getUserId());
            }

            if (maybeQuotedStatusId.isPresent()) {
                var quotedStatusId = maybeQuotedStatusId.get();
                if (maybeQuotedStatus.isPresent()) {
                    this.addTweet(maybeQuotedStatus.get());
                }
            } else {
                if (maybeQuotedStatus.isPresent()) {
                    MetadataBatch.logger.error("Error: quoted_status without quoted_status_id_str in {}",
                            tweet.getStatusId());
                }
            }

            var mentionedIds = new HashSet<Long>();
            for (UserInfo.Full user : tweet.getUserMentions()) {
                mentionedIds.add(user.getUserId());
                this.addUser(user.getUserId(), user.getScreenName(), tweet.getSourceStatusId());
            }

            statusValue = StatusValue.ofTweet(userInfo.getUserId(), tweet.getTimestampMillis(),
                    maybeReplyInfo.map(replyInfo -> replyInfo.getStatusId()), maybeQuotedStatusId, mentionedIds);
        }

        var statusKey = new Key(MetadataBatch.makeFullStatusKey(tweet.getStatusId()));
        this.statuses.put(statusKey, statusValue.toByteArray());
    }
}

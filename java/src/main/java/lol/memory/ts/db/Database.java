package lol.memory.ts.db;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.LRUCache;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.WriteOptions;

public final class Database {
    private static final Options options = new Options();
    private static final ReadOptions readOptions = new ReadOptions();
    private static final WriteOptions writeOptions = new WriteOptions();
    private static final OptimisticTransactionOptions transactionOptions = new OptimisticTransactionOptions();
    private final OptimisticTransactionDB db;

    private static final int BLOOMFILTER_BITS_PER_KEY = 8;

    static {
        Database.options.setCreateIfMissing(true);
        // Database.options.setUseFsync(true);
        // Database.options.setParanoidChecks(true);
        Database.options.setDisableAutoCompactions(true);
        // Database.options.setMaxWriteBufferNumberToMaintain(4);

        // Database.options.setCompressionType(CompressionType.LZ4_COMPRESSION).setCompactionStyle(CompactionStyle.LEVEL);

        // Database.options.setLevelCompactionDynamicLevelBytes(true);
        // Database.options.setCompactionStyle(CompactionStyle.UNIVERSAL);
        Database.options.setNumLevels(6);
        List<CompressionType> compressionLevels = Arrays.asList(CompressionType.NO_COMPRESSION,
                CompressionType.NO_COMPRESSION, CompressionType.SNAPPY_COMPRESSION, CompressionType.SNAPPY_COMPRESSION,
                CompressionType.SNAPPY_COMPRESSION, CompressionType.SNAPPY_COMPRESSION);

        Database.options.setCompressionPerLevel(compressionLevels);

        BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
        tableOptions.setBlockCache(new LRUCache(8 * 1048));
        tableOptions.setBlockSize(8 * 1048);
        tableOptions.setCacheIndexAndFilterBlocks(true);
        tableOptions.setPinL0FilterAndIndexBlocksInCache(true);
        tableOptions.setFilterPolicy(new BloomFilter(BLOOMFILTER_BITS_PER_KEY, false));
        Database.options.setTableFormatConfig(tableOptions);

        // Database.writeOptions.setDisableWAL(true);
    }

    public Database(String path) throws RocksDBException {
        this.db = OptimisticTransactionDB.open(Database.options, path);
    }

    public static ReadOptions getReadOptions() {
        return Database.readOptions;
    }

    public Transaction beginTransaction() {
        return this.db.beginTransaction(Database.writeOptions);
    }

    public RocksIterator prefixIterator(byte[] prefix) {
        var iterator = this.db.newIterator();
        iterator.seek(prefix);
        return iterator;
    }

    public void compact() throws RocksDBException {
        this.db.compactRange();
    }

    public void insert(Transaction tx, Entry entry) throws RocksDBException {
        var key = entry.getKey();

        Optional<byte[]> previousValue = entry.checkPrevious() ? Optional.ofNullable(tx.get(this.readOptions, key))
                : Optional.empty();
        var newValue = entry.updateValue(previousValue);

        if (newValue.isPresent()) {
            tx.put(key, newValue.get());
        }
    }
}

package lol.memory.ts.db;

import java.util.Optional;
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

    static {
        Database.options.setCreateIfMissing(true);
        Database.options.setUseFsync(true);
        Database.options.setParanoidChecks(true);
        Database.options.setDisableAutoCompactions(true);
        Database.options.setMaxWriteBufferNumberToMaintain(4);
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

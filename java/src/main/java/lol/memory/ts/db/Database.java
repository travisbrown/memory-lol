package lol.memory.ts.db;

import java.util.Optional;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;

public final class Database {
    private static final Options options = new Options();
    private static final ReadOptions readOptions = new ReadOptions();
    private static final WriteOptions writeOptions = new WriteOptions();
    private static final TransactionDBOptions transactionOptions = new TransactionDBOptions();
    private final TransactionDB db;

    static {
        Database.options.setCreateIfMissing(true);
    }

    public Database(String path) throws RocksDBException {
        this.db = TransactionDB.open(Database.options, Database.transactionOptions, path);
    }

    public Transaction beginTransaction() {
        return this.db.beginTransaction(Database.writeOptions);
    }

    public void insert(Transaction tx, Entry entry) throws RocksDBException {
        var key = entry.getKey();
        var previousValue = Optional.ofNullable(tx.get(this.readOptions, key));
        var newValue = entry.updateValue(previousValue);

        if (newValue.isPresent()) {
            tx.put(key, newValue.get());
        }
    }
}

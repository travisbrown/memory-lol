package lol.memory.ts.db;

import lol.memory.ts.archive.RecordConsumer;
import org.rocksdb.RocksDBException;

/**
 * Supports re-throwing fatal exceptions during processing.
 */
public abstract class DbRecordConsumer<T> extends RecordConsumer<T> {
    public final boolean isFatal(Throwable throwable) {
        return throwable instanceof RocksDBException;
    }
}

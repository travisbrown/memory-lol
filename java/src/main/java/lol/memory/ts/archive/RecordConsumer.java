package lol.memory.ts.archive;

import java.util.function.Consumer;

/**
 * Supports re-throwing fatal exceptions during processing.
 */
public abstract class RecordConsumer<T> implements Consumer<Record<T>> {
    public abstract boolean isFatal(Throwable throwable);

    public static final <T> RecordConsumer<T> wrap(Consumer<Record<T>> consumer) {
        return new RecordConsumer<>() {
            public void accept(Record<T> record) {
                consumer.accept(record);
            }

            public boolean isFatal(Throwable throwable) {
                return false;
            }
        };
    }
}

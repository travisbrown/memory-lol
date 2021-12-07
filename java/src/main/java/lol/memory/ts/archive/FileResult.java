package lol.memory.ts.archive;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import lol.memory.ts.Item;

public class FileResult<T> {
    private final String path;
    private final T value;

    public final String getPath() {
        return this.path;
    }

    public final T getValue() {
        return this.value;
    }

    FileResult(String path, T value) {
        this.path = path;
        this.value = value;
    }

    public static final class Item extends FileResult<List<Optional<Item>>> {
        Item(String path, List<Optional<Item>> value) {
            super(path, value);
        }
    }

    public final <U> FileResult<U> map(Function<T, U> transform) {
        return new FileResult(this.path, transform.apply(this.value));
    }
}

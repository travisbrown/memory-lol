package lol.memory.ts.archive;

import java.util.List;
import java.util.Optional;

public final class FileResult<T> {
    private final String path;
    private final List<Optional<T>> values;

    public String getPath() {
        return this.path;
    }

    public List<Optional<T>> getValues() {
        return this.values;
    }

    FileResult(String path, List<Optional<T>> values) {
        this.path = path;
        this.values = values;
    }
}

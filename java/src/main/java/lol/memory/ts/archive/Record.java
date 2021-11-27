package lol.memory.ts.archive;

import java.nio.file.Path;
import java.util.Optional;
import lombok.Value;

@Value
public final class Record<T> {
    private Path archivePath;
    private Optional<String> filePath;
    private int lineNumber;
    private T value;

    public <U> Record<U> withValue(U newValue) {
        return new Record(this.archivePath, this.filePath, this.lineNumber, newValue);
    }
}

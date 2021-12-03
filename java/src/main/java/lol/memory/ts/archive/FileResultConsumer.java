package lol.memory.ts.archive;

import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Consumer;

public abstract class FileResultConsumer<T> {
    public abstract boolean skipFile(Path archivePath, String filePath);

    public abstract boolean accept(Path archivePath, FileResult<T> result) throws Exception;

    public static final <T> FileResultConsumer<T> ofItemConsumer(Consumer<T> consumer) {
        return new FileResultConsumer<T>() {
            public boolean skipFile(Path archivePath, String filePath) {
                return false;
            }

            public boolean accept(Path archivePath, FileResult<T> result) throws Exception {
                for (Optional<T> value : result.getValues()) {
                    value.ifPresent(consumer::accept);
                }
                return true;
            }
        };
    }
}

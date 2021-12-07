package lol.memory.ts.archive;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import lol.memory.ts.Item;

public abstract class FileResultConsumer<T> {
    public abstract boolean skipFile(Path archivePath, String filePath);

    public abstract boolean accept(Path archivePath, FileResult<T> result) throws Exception;

    public static final FileResultConsumer<List<Optional<Item>>> ofItemConsumer(Consumer<Item> consumer) {
        return new FileResultConsumer<List<Optional<Item>>>() {
            public boolean skipFile(Path archivePath, String filePath) {
                return false;
            }

            public boolean accept(Path archivePath, FileResult<List<Optional<Item>>> result) throws Exception {
                for (Optional<Item> value : result.getValue()) {
                    value.ifPresent(consumer::accept);
                }
                return true;
            }
        };
    }
}

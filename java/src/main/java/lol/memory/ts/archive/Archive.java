package lol.memory.ts.archive;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import lol.memory.ts.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Archive<E> {
    /**
     * Demonstration driver (just prints status IDs).
     */
    public static void main(String[] args) throws IOException {
        Archive<?> archive = Archive.load(new File(args[0]));

        archive.process(item -> System.out.println(item.getStatusId()));
    }

    private static final Logger logger = LoggerFactory.getLogger(Archive.class);
    private final Path path;
    private final int numThreads;

    Archive(Path path, int numThreads) {
        this.path = path;
        this.numThreads = numThreads;
    }

    protected abstract Iterator<E> getEntries();

    protected abstract boolean isEntryValidFile(E entry);

    protected abstract String getEntryFilePath(E entry);

    protected abstract InputStream getEntryInputStream(E entry) throws IOException;

    protected final Path getPath() {
        return this.path;
    }

    public static Archive load(File file) {
        return Archive.load(file, Runtime.getRuntime().availableProcessors());
    }

    public static Archive load(Path path) {
        return Archive.load(path, Runtime.getRuntime().availableProcessors());
    }

    public static Archive<?> load(File file, int numThreads) {
        if (file.getName().endsWith("tar")) {
            return new TarArchive(file.toPath(), numThreads);
        } else {
            return new ZipArchive(file.toPath(), numThreads);
        }
    }

    public static Archive load(Path path, int numThreads) {
        if (path.toString().endsWith("tar")) {
            return new TarArchive(path, numThreads);
        } else {
            return new ZipArchive(path, numThreads);
        }
    }

    private static boolean shutdown(ExecutorService service, boolean hard, Optional<Throwable> reason) {
        if (reason.isPresent()) {
            var leftovers = service.shutdownNow();
            Archive.logger.error("Fatal error processing archive ({} tasks unfinished): {}", leftovers.size(),
                    reason.get().getMessage());
        } else if (hard) {
            var leftovers = service.shutdownNow();
            Archive.logger.error("Fatal error processing archive ({} tasks unfinished)", leftovers.size());
        } else {
            service.shutdown();
        }

        try {
            return service.awaitTermination(48, TimeUnit.HOURS);
        } catch (InterruptedException error) {
            Archive.logger.error("Error shutting down execution: {}", error.getMessage());
            return false;
        }
    }

    public final boolean process(Consumer<Item> processor) {
        return this.process(FileResultConsumer.ofItemConsumer(processor));
    }

    public final boolean process(FileResultConsumer<List<Optional<Item>>> processor) {
        var pool = Executors.newFixedThreadPool(this.numThreads);
        var service = new BoundedCompletionService<FileResult<List<Optional<Item>>>>(pool, 32);

        var entries = this.getEntries();
        int count = 0;

        while (entries.hasNext()) {
            var entry = entries.next();

            if (this.isEntryValidFile(entry) && !processor.skipFile(this.path, this.getEntryFilePath(entry))) {
                service.submit(new ItemParser(this, entry));
                count += 1;
            }
        }

        for (int i = 0; i < count; i += 1) {
            try {
                var result = service.take().get();
                if (!processor.accept(Archive.this.path, result)) {
                    return Archive.shutdown(pool, true, Optional.empty());
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                return Archive.shutdown(pool, true, Optional.of(error));
            } catch (Exception error) {
                error.printStackTrace();
                Archive.logger.error("Unhandled exception while processing archive: {}", error.getMessage());
            }
        }

        return Archive.shutdown(pool, false, Optional.empty());
    }

    public final <T> boolean processWithTransformation(Function<List<Optional<Item>>, T> transform,
            FileResultConsumer<T> processor) {
        var pool = Executors.newFixedThreadPool(this.numThreads);
        var service = new BoundedCompletionService<FileResult<T>>(pool, 32);

        var entries = this.getEntries();
        int count = 0;

        while (entries.hasNext()) {
            var entry = entries.next();

            if (this.isEntryValidFile(entry) && !processor.skipFile(this.path, this.getEntryFilePath(entry))) {
                service.submit(new ItemParser(this, entry).andThen(transform));
                count += 1;
            }
        }

        for (int i = 0; i < count; i += 1) {
            try {
                FileResult<T> result = service.take().get();
                if (!processor.accept(Archive.this.path, result)) {
                    return Archive.shutdown(pool, true, Optional.empty());
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                return Archive.shutdown(pool, true, Optional.of(error));
            } catch (Exception error) {
                error.printStackTrace();
                Archive.logger.error("Unhandled exception while processing archive: {}", error.getMessage());
            }
        }

        return Archive.shutdown(pool, false, Optional.empty());
    }
}

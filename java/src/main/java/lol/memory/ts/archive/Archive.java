package lol.memory.ts.archive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.parser.Feature;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lol.memory.ts.Item;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Archive {
    /**
     * Demonstration driver (just prints status IDs).
     */
    public static void main(String[] args) throws IOException {
        var archive = Archive.load(new File(args[0]));

        archive.run(record -> System.out.println(record.getValue().getStatusId()));
    }

    private static final Logger logger = LoggerFactory.getLogger(Archive.class);
    private final Path path;
    private final int numThreads;

    protected abstract Iterable<EntryJob> entryJobs(Consumer<Record<Item>> process);

    protected final Path getPath() {
        return this.path;
    }

    public static Archive load(File file) {
        return Archive.load(file, Runtime.getRuntime().availableProcessors());
    }

    public static Archive load(Path path) {
        return Archive.load(path, Runtime.getRuntime().availableProcessors());
    }

    public static Archive load(File file, int numThreads) {
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

    Archive(Path path, int numThreads) {
        this.path = path;
        this.numThreads = numThreads;
    }

    /**
     * Perform an action on every item (tweet or deletion) in this archive.
     */
    public final boolean run(Consumer<Record<Item>> process) {
        ExecutorService pool = Executors.newFixedThreadPool(this.numThreads);
        for (EntryJob job : this.entryJobs(process)) {
            pool.submit(job);
        }
        pool.shutdown();
        try {
            return pool.awaitTermination(48, TimeUnit.HOURS);
        } catch (InterruptedException error) {
            Archive.logger.error("Error shutting down execution: {}", error.getMessage());
            return false;
        }
    }

    protected abstract class EntryJob implements Runnable {
        private final Consumer<Record<Item>> process;

        EntryJob(Consumer<Record<Item>> process) {
            this.process = process;
        }

        protected abstract boolean isValidFile();

        protected abstract Optional<String> getFilePath();

        protected abstract InputStream getInputStream();

        public final void run() {
            try {
                if (this.isValidFile()) {
                    var filePath = this.getFilePath();
                    InputStream stream = null;
                    BufferedReader reader = null;
                    try {
                        stream = this.getInputStream();
                        var buffered = new BufferedInputStream(stream);
                        var bzip2 = new BZip2CompressorInputStream(buffered, true);
                        reader = new BufferedReader(new InputStreamReader(bzip2));

                        String line = reader.readLine();
                        int lineNumber = 1;
                        while (line != null) {
                            try {
                                var value = JSON.parseObject(line, Feature.OrderedField);
                                var item = Item.fromJson(value);

                                if (item.isPresent()) {
                                    this.process.accept(
                                            new Record(Archive.this.getPath(), filePath, lineNumber, item.get()));
                                }
                            } catch (JSONException error) {
                                Archive.logger.error("Error parsing JSON ({}): {}", filePath.orElseGet(() -> "<none>"),
                                        error.getMessage());
                            }
                            line = reader.readLine();
                            lineNumber += 1;
                        }
                    } catch (IOException error) {
                        Archive.logger.error("Error reading archive file ({}): {}", filePath.orElseGet(() -> "<none>"),
                                error.getMessage());
                    } finally {
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (IOException error) {
                                Archive.logger.error("Error closing archive file ({}): {}",
                                        filePath.orElseGet(() -> "<none>"), error.getMessage());
                            }
                        }

                        if (stream != null) {
                            try {
                                stream.close();
                            } catch (IOException error) {
                                Archive.logger.error("Error closing archive file ({}): {}",
                                        filePath.orElseGet(() -> "<none>"), error.getMessage());
                            }
                        }
                    }
                }
            } catch (Throwable error) {
                Archive.logger.error("Unhandled exception in worker: {}", error.getMessage());
            }
        }
    }
}

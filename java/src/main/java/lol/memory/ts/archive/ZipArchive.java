package lol.memory.ts.archive;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import lol.memory.ts.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ZipArchive extends Archive {
    private static final Logger logger = LoggerFactory.getLogger(ZipArchive.class);

    public ZipArchive(Path path, int numThreads) {
        super(path, numThreads);
    }

    protected Iterable<Archive.EntryJob> entryJobs(Consumer<Record<Item>> process) {
        return new Iterable<Archive.EntryJob>() {
            public Iterator<Archive.EntryJob> iterator() {
                return new Iterator<Archive.EntryJob>() {
                    private final Enumeration<? extends ZipEntry> entries = ZipArchive.this.zipFile.get().entries();

                    public boolean hasNext() {
                        return this.entries.hasMoreElements();
                    }

                    public Archive.EntryJob next() {
                        return new EntryJob(this.entries.nextElement(), process);
                    }
                };
            }
        };
    }

    private final class EntryJob extends Archive.EntryJob {
        private final ZipEntry entry;

        EntryJob(ZipEntry entry, Consumer<Record<Item>> process) {
            super(process);
            this.entry = entry;
        }

        protected boolean isValidFile() {
            return !this.entry.isDirectory() && this.entry.getName().toLowerCase().endsWith("bz2");
        }

        protected Optional<String> getFilePath() {
            return Optional.of(this.entry.getName());
        }

        protected InputStream getInputStream() {
            try {
                return ZipArchive.this.zipFile.get().getInputStream(this.entry);
            } catch (IOException error) {
                ZipArchive.logger.error("Error opening zip file ({}): {}", this.getFilePath().orElseGet(() -> "<none>"),
                        error.getMessage());
            }
            return null;
        }
    }

    private final ThreadLocal<ZipFile> zipFile = new ThreadLocal<ZipFile>() {
        @Override
        protected ZipFile initialValue() {
            try {
                return new ZipFile(ZipArchive.this.getPath().toFile());
            } catch (IOException error) {
                ZipArchive.logger.error("Error initializing zip file ({}): {}", ZipArchive.this.getPath(),
                        error.getMessage());
            }
            return null;
        }
    };
}

package lol.memory.ts.archive;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarFile;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Consumer;
import lol.memory.ts.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TarArchive extends Archive {
    private static final Logger logger = LoggerFactory.getLogger(TarArchive.class);

    TarArchive(Path path, int numThreads) {
        super(path, numThreads);
    }

    protected Iterable<Archive.EntryJob> entryJobs(Consumer<Record<Item>> process) {
        return new Iterable<Archive.EntryJob>() {
            public Iterator<Archive.EntryJob> iterator() {
                return new Iterator<Archive.EntryJob>() {
                    private final Iterator<TarArchiveEntry> entries = TarArchive.this.tarFile.get().getEntries()
                            .iterator();

                    public boolean hasNext() {
                        return this.entries.hasNext();
                    }

                    public Archive.EntryJob next() {
                        return new EntryJob(this.entries.next(), process);
                    }
                };
            }
        };
    }

    private final class EntryJob extends Archive.EntryJob {
        private final TarArchiveEntry entry;

        EntryJob(TarArchiveEntry entry, Consumer<Record<Item>> process) {
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
                return TarArchive.this.tarFile.get().getInputStream(this.entry);
            } catch (IOException error) {
                TarArchive.logger.error("Error opening zip file ({}): {}", this.getFilePath().orElseGet(() -> "<none>"),
                        error.getMessage());
            }
            return null;
        }
    }

    private final ThreadLocal<TarFile> tarFile = new ThreadLocal<TarFile>() {
        @Override
        protected TarFile initialValue() {
            try {
                return new TarFile(TarArchive.this.getPath().toFile());
            } catch (IOException error) {
                TarArchive.logger.error("Error initializing zip file ({}): {}", TarArchive.this.getPath(),
                        error.getMessage());
            }
            return null;
        }
    };
}

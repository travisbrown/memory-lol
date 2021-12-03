package lol.memory.ts.archive;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarFile;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TarArchive extends Archive<TarArchiveEntry> {
    private static final Logger logger = LoggerFactory.getLogger(TarArchive.class);

    TarArchive(Path path, int numThreads) {
        super(path, numThreads);
    }

    protected Iterator<TarArchiveEntry> getEntries() {
        return this.tarFile.get().getEntries().iterator();
    }

    protected boolean isEntryValidFile(TarArchiveEntry entry) {
        return !entry.isDirectory() && entry.getName().toLowerCase().endsWith("bz2");
    }

    protected String getEntryFilePath(TarArchiveEntry entry) {
        return entry.getName();
    }

    protected InputStream getEntryInputStream(TarArchiveEntry entry) throws IOException {
        return this.tarFile.get().getInputStream(entry);
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

package lol.memory.ts.archive;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ZipArchive extends Archive<ZipEntry> {
    private static final Logger logger = LoggerFactory.getLogger(ZipArchive.class);

    public ZipArchive(Path path, int numThreads) {
        super(path, numThreads);
    }

    protected Iterator<ZipEntry> getEntries() {
        return new Iterator<ZipEntry>() {
            private final Enumeration<? extends ZipEntry> entries = ZipArchive.this.zipFile.get().entries();

            public boolean hasNext() {
                return this.entries.hasMoreElements();
            }

            public ZipEntry next() {
                return this.entries.nextElement();
            }
        };
    }

    protected boolean isEntryValidFile(ZipEntry entry) {
        return !entry.isDirectory() && entry.getName().toLowerCase().endsWith("bz2");
    }

    protected String getEntryFilePath(ZipEntry entry) {
        return entry.getName();
    }

    protected InputStream getEntryInputStream(ZipEntry entry) throws IOException {
        return this.zipFile.get().getInputStream(entry);
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

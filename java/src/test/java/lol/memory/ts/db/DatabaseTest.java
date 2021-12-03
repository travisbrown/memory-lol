package lol.memory.ts.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import lol.memory.ts.archive.Archive;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import org.testng.Assert;

public class DatabaseTest {
    @BeforeSuite
    public void load() {
        RocksDB.loadLibrary();
    }

    @Test
    public void importZipExample() throws IOException, RocksDBException {
        var archive = Archive.load(Paths.get("../examples/archives/twitter-stream-2021-01-01.zip"));
        var dbPath = Files.createTempDirectory("metadata-db");
        var importer = new MetadataImporter(dbPath.toString());

        archive.process(importer);
    }
}
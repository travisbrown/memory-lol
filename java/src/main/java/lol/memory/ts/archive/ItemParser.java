package lol.memory.ts.archive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.parser.Feature;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import lol.memory.ts.Item;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ItemParser<E> implements Callable<FileResult<Item>> {
    private static final Logger logger = LoggerFactory.getLogger(ItemParser.class);
    private final Archive<E> archive;
    private final E entry;

    ItemParser(Archive<E> archive, E entry) {
        this.archive = archive;
        this.entry = entry;
    }

    public FileResult<Item> call() {
        String path = this.archive.getEntryFilePath(entry);

        List<Optional<Item>> items = new ArrayList<Optional<Item>>(4096);
        InputStream stream = null;
        BufferedReader reader = null;
        try {
            stream = this.archive.getEntryInputStream(this.entry);
            var buffered = new BufferedInputStream(stream);
            var bzip2 = new BZip2CompressorInputStream(buffered, true);
            reader = new BufferedReader(new InputStreamReader(bzip2));

            String line = reader.readLine();
            while (line != null) {
                Optional<Item> item = null;
                try {
                    item = Item.fromJson(JSON.parseObject(line, Feature.OrderedField));
                } catch (JSONException error) {
                    ItemParser.logger.error("Error parsing JSON ({}): {}", path, error.getMessage());
                }

                items.add(item == null ? Optional.empty() : item);
                line = reader.readLine();
            }
        } catch (IOException error) {
            ItemParser.logger.error("Error reading archive file ({}): {}", path, error.getMessage());
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException error) {
                    ItemParser.logger.error("Error closing archive file ({}): {}", path, error.getMessage());
                }
            }

            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException error) {
                    ItemParser.logger.error("Error closing archive file ({}): {}", path, error.getMessage());
                }
            }
        }

        return new FileResult(path, items);
    }
}

package lol.memory.ts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {
    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    public static Set<Long> readLongs(File file) throws IOException {
        var result = new HashSet<Long>();
        var reader = new BufferedReader(new FileReader(file));

        String line;
        while ((line = reader.readLine()) != null) {
            try {
                result.add(Long.parseLong(line));
            } catch (NumberFormatException error) {
                Util.logger.error("Error parsing long: {}", line);
            }
        }

        return result;
    }

    protected Util() {
        throw new UnsupportedOperationException();
    }
}

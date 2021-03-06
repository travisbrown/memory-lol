package lol.memory.ts.archive;

import java.nio.file.Paths;
import java.util.function.Consumer;
import lol.memory.ts.Item;
import org.testng.annotations.Test;
import org.testng.Assert;

public class ArchiveTest {
    @Test
    public void readZipExample() {
        var archive = Archive.load(Paths.get("../examples/archives/twitter-stream-2021-01-01.zip"));

        var counter = new Consumer<Record<Item>>() {
            int deleteCount = 0;
            int tweetCount = 0;

            public synchronized void accept(Record<Item> record) {
                if (record.getValue().isTweet()) {
                    this.tweetCount += 1;
                } else {
                    this.deleteCount += 1;
                }
            }
        };

        archive.run(counter);

        Assert.assertEquals(counter.deleteCount, 832);
        Assert.assertEquals(counter.tweetCount, 5378);
    }
}
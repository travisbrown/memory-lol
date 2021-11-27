package lol.memory.ts;

import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import lol.memory.ts.archive.Archive;
import lol.memory.ts.archive.Record;

/**
 * Application that exports user JSON objects with a snapshot timestamp (as epoch second).
 */
public class UserJsonExport {

    public static void main(String[] args) throws IOException {

        var dataFile = new File(args[0]);
        Set<Long> selectedUserIds = (args.length > 1) ? Util.readLongs(new File(args[1])) : Collections.emptySet();

        var archive = Archive.load(dataFile);
        var users = new HashSet<JSONObject>();

        archive.runConsumer(new Consumer<Record<Item>>() {
            public void accept(Record<Item> record) {
                if (record.getValue().isTweet()) {
                    UserJsonExport.extractUserObjects(selectedUserIds, users, record.getValue().asTweet().get());

                    for (JSONObject user : users) {
                        synchronized (System.out) {
                            System.out.println(user);
                        }
                    }
                    users.clear();
                }
            }
        });
    }

    private static void extractUserObjects(Set<Long> selectedUserIds, Set<JSONObject> values, Item.Tweet tweet) {
        if (selectedUserIds.contains(tweet.getUserId())) {
            values.add(tweet.getAugmentedUserObject());
        }
        tweet.getRetweetedStatus().ifPresent(status -> extractUserObjects(selectedUserIds, values, status));
        tweet.getQuotedStatus().ifPresent(status -> extractUserObjects(selectedUserIds, values, status));
    }

    protected UserJsonExport() {
        throw new UnsupportedOperationException();
    }
}

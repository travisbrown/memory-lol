package lol.memory.ts;

public class UserInfo {
    private final long userId;
    private final String screenName;

    public long getUserId() {
        return this.userId;
    }

    public String getScreenName() {
        return this.screenName;
    }

    UserInfo(long userId, String screenName) {
        this.userId = userId;
        this.screenName = screenName;
    }

    public static final class Full extends UserInfo {
        private final String name;

        public String getName() {
            return this.name;
        }

        Full(long userId, String screenName, String name) {
            super(userId, screenName);
            this.name = name;
        }
    }
}

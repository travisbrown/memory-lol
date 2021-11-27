package lol.memory.ts;

public class ReplyInfo extends UserInfo {
    private final long statusId;

    public long getStatusId() {
        return this.statusId;
    }

    ReplyInfo(long statusId, long userId, String screenName) {
        super(userId, screenName);
        this.statusId = statusId;
    }
}

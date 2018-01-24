package com.splunk.kafka.connect.transforms;


public class Message {
    private String eventVersion;
    private UserIdentity userIdentity;

    public String getEventVersion() {
        return eventVersion;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public void setEventVersion(String eventVersion) {
        this.eventVersion = eventVersion;
    }

    public void setUserIdentity(UserIdentity userIdentity) {
        this.userIdentity = userIdentity;
    }
}

class UserIdentity {
    private String type;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
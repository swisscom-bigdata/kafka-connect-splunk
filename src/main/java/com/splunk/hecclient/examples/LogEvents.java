package com.splunk.hecclient.examples;


import com.fasterxml.jackson.annotation.JsonIgnore;

public class LogEvents{
    private String id;
    private long timestamp;
    private String message;

    @JsonIgnore
    private Message messageObject;

    @JsonIgnore
    private String unescapedMessage;

    public String getId(){
        return id;
    }
    public void setId(String input){
        this.id = input;
    }
    public long getTimestamp(){
        return timestamp;
    }
    public void setTimestamp(long input){
        this.timestamp = input;
    }
    public Message getMessage() {return messageObject;}

    public void setMessage(String input) {
        this.message = input + "}";
        this.unescapedMessage = input + "}";
    }

    public String getUnescapedMessage() {return unescapedMessage;}
    public Message getMessageObject() { return messageObject; }
    public void setMessageObject(Message messageObject) { this.messageObject = messageObject;}
}
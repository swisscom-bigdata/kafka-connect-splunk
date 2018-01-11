package com.splunk.hecclient.examples;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class CloudWatchEvent{
    private String owner;
    private String logGroup;
    private String logStream;
    private String[] subscriptionFilters;
    private String messageType;

    private LogEvents[] logEvents;

    @JsonIgnore
    private int logEventIndex;

    public String getOwner(){
        return owner;
    }
    public void setOwner(String input){
        this.owner = input;
    }
    public String getLogGroup(){
        return logGroup;
    }
    public void setLogGroup(String input){
        this.logGroup = input;
    }
    public String getLogStream(){
        return logStream;
    }
    public void setLogStream(String input){
        this.logStream = input;
    }
    public String[] getSubscriptionFilters(){
        return subscriptionFilters;
    }
    public void setSubscriptionFilters(String[] input){
        this.subscriptionFilters = input;
    }
    public String getMessageType(){
        return messageType;
    }
    public void setMessageType(String input){
        this.messageType = input;
    }
    public LogEvents getLogEvents() {return logEvents[getLogEventIndex()];}
    public void setLogEvents(LogEvents[] input) {this.logEvents = input;}
    public int getLogEventIndex() {return logEventIndex;}
    public void setLogEventIndex(int input) {this.logEventIndex = input;}

    @JsonIgnore
    public LogEvents[] getLogEventArray() {return logEvents;}

}

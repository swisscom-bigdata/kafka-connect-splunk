/*
 * Copyright 2017 Splunk, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splunk.hecclient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.*;

import java.io.*;
import java.util.Map;

/**
 * Event is an abstract class that represents a bare bones implementation of a Splunk Event. Every event that arrives
 * in Splunk must have a time, host, index, source and sourcetype.
 * <p>
 * This class contains getter and setter methods with a few convenience functions.

 *
 * @version     1.0
 * @since       1.0
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class Event {
    static final String TIME = "time";
    static final String HOST = "host";
    static final String INDEX = "index";
    static final String SOURCE = "source";
    static final String SOURCETYPE = "sourcetype";

    static final ObjectMapper jsonMapper = new ObjectMapper();
    protected static final Logger log = LoggerFactory.getLogger(Event.class);

    protected Long time = null; // epochMillis

    protected String source;
    protected String sourcetype;
    protected String host;
    protected String index;
    protected Object event;

    @JsonIgnore
    protected String lineBreaker = "\n";

    @JsonIgnore
    protected byte[] bytes; // populated once, use forever until invalidate

    @JsonIgnore
    private Object tied; // attached object

    /**
     * Constructor implements Event with 2 parameters.
     *
     * @param eventData      Object representation of the event itself without all the extras. Event Data Only
     * @param tiedObj        Object representation of the entire Record being constructed into an Event.
     *                       Within the Kafka Connect project this class will be of type
     *                       <a href="https://kafka.apache.org/10/javadoc/org/apache/kafka/connect/sink/SinkRecord.html">SinkRecord</a>SinkRecord
     *
     * @since           1.0
     * @see JsonEvent
     * @see RawEvent
     */
    public Event(Object eventData, Object tiedObj) {
        checkEventData(eventData);

        event = eventData;
        tied = tiedObj;
    }

    // for JSON deserialization
    Event() {
    }

    /**
     * Setter implementation of Event. Data passed in is validated to be an acceptable String and then
     *
     * @param  data     Object representation of the event itself without all the extras. Event Data Only
     * @return Event    Current representation of Event.
     * @since           1.0
     */
    public final Event setEvent(final Object data) {
        checkEventData(data);
        event = data;
        invalidate();
        return this;
    }

    public final Event setTied(final Object tied) {
        this.tied = tied;
        return this;
    }

    public final Event setTime(final long epochMillis) {
        this.time = epochMillis;
        invalidate();
        return this;
    }

    public final Event setSource(final String source) {
        this.source = source;
        invalidate();
        return this;
    }

    public final Event setSourcetype(final String sourcetype) {
        this.sourcetype = sourcetype;
        invalidate();
        return this;
    }

    public final Event setHost(final String host) {
        this.host = host;
        invalidate();
        return this;
    }

    public final Event setIndex(final String index) {
        this.index = index;
        invalidate();
        return this;
    }


    public final Long getTime() {
        return time;
    }

    public final String getSource() {
        return source;
    }

    public final String getSourcetype() {
        return sourcetype;
    }

    public final String getHost() {
        return host;
    }

    public final String getIndex() {
        return index;
    }

    public final Object getEvent() {
        return event;
    }

    public final String getLineBreaker() {
        return lineBreaker;
    }

    public final Object getTied() {
        return tied;
    }

    public Event addFields(final Map<String, String> fields) {
        return this;
    }

    public Event setFields(final Map<String, String> fields) {
        return this;
    }

    public Map<String, String> getFields() {
        return null;
    }

    public final int length() {
        byte[] data = getBytes();
        return data.length + lineBreaker.getBytes().length;
    }

    @JsonIgnore
    public final InputStream getInputStream() {
        byte[] data = getBytes();
        InputStream eventStream = new ByteArrayInputStream(data);

        // avoid copying the event
        InputStream carriageReturnStream = new ByteArrayInputStream(lineBreaker.getBytes());
        return new SequenceInputStream(eventStream, carriageReturnStream);
    }

    public final void writeTo(OutputStream out) throws IOException {
        byte[] data = getBytes();
        out.write(data);

        // append line breaker
        byte[] breaker = lineBreaker.getBytes();
        out.write(breaker);
    }

    // if everything is good, no exception. Otherwise HecException will be raised
    public void validate() throws HecException {
        getBytes();
    }

    public void invalidate() {
        bytes = null;
    }

    public abstract byte[] getBytes() throws HecException;

    private static void checkEventData(Object eventData) {
        if (eventData == null) {
            throw new HecException("Null data for event");
        }

        if (eventData instanceof String) {
            if (((String) eventData).isEmpty()) {
                throw new HecException("Empty event");
            }
        }
    }
}
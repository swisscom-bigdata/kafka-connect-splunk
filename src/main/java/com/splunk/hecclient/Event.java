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
     * Event is the data portion of the Event Record. Data passed in is validated to be an acceptable String and the byte[]
     * representation of the Event is cleared as the Event representation has changed.
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

    /**
     * Tied is the full Record Object with associated meta-data.
     *
     * @param tied      Object representation of the event with associated meta-data.
     * @return Event    Current representation of Event.
     * @since           1.0
     */
    public final Event setTied(final Object tied) {
        this.tied = tied;
        return this;
    }

    /**
     * Time is the Long representation of the event time in epoch format. This is to be later used as the time field in
     * an indexed Splunk Event.
     *
     * @param epochMillis   Long representation of the record event time.
     * @return Event        Current representation of Event.
     * @since               1.0
     */
    public final Event setTime(final long epochMillis) {
        this.time = epochMillis;
        invalidate();
        return this;
    }

    /**
     * Source is the default field used within an indexed Splunk event. The source of an event is the name of the file, stream
     * or other input from which the event originates
     *
     * @param source    String representation of the record event source.
     * @return Event    Current representation of Event.
     * @since           1.0
     */
    public final Event setSource(final String source) {
        this.source = source;
        invalidate();
        return this;
    }

    /**
     * Sourcetype is the default field used within an indexed Splunk event. The source type of an event is the format
     * of the data input from which it originates.The source type determines how your data is to be formatted.
     *
     * @param sourcetype  String representation of the record event sourcetype.
     * @return Event      Current representation of Event.
     * @since             1.0
     */
    public final Event setSourcetype(final String sourcetype) {
        this.sourcetype = sourcetype;
        invalidate();
        return this;
    }

    /**
     * Host is the default field used within an indexed Splunk event. An event host value is typically the hostname,
     * IP address, or fully qualified domain name of the network host from which the event originated. The host value
     * lets you locate data originating from a specific device.
     *
     * @param host        String representation of the host machine which generated the event.
     * @return Event      Current representation of Event.
     * @since             1.0
     */
    public final Event setHost(final String host) {
        this.host = host;
        invalidate();
        return this;
    }

    /**
     * Index is a required field used to send an event to particular <a href=http://docs.splunk.com/Documentation/Splunk/7.0.0/Indexer/Aboutindexesandindexers>Splunk Index</>.
     *
     * @param index       String representation of the Splunk index
     * @return Event      Current representation of Event.
     * @since             1.0
     */
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
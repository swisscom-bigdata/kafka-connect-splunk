/*
 * Copyright 2017-2018 Splunk, Inc..
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
package com.splunk.kafka.connect;

import com.splunk.hecclient.HecConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * SplunkSinkConnectorConfig is the configuration class for the Splunk Kafka Connector. The configurations
 * have 4 distinct categories.
 *      1. Splunk configurations
 *      2. Security configurations
 *      3. Event and Batch configurations
 *      4. Hec Client configurations
 *
 *
 * <p>
 * This class contains

 *
 * @version     1.0.0
 * @since       1.0.0
 */
public final class SplunkSinkConnectorConfig extends AbstractConfig {

    public static final String INDEX = "index";
    public static final String SOURCETYPE = "sourcetype";
    public static final String SOURCE = "source";

    public static final String INDEX_CONF = "splunk.indexes";
    private static final String INDEX_DOC = "Splunk index names for Kafka topic data, separated by comma";
    public static final String INDEX_DEFAULT = "";

    public static final String SOURCETYPE_CONF = "splunk.sourcetypes";
    private static final String SOURCETYPE_DOC = "Splunk sourcetype names for Kafka topic data, separated by comma";
    public static final String SOURCETYPE_DEFAULT = "";

    public static final String SOURCE_CONF = "splunk.sources";
    private static final String SOURCE_DOC = "Splunk source names for Kafka topic data, separated by comma";
    public static final String SOURCE_DEFAULT = "";

    public static final String TOKEN_CONF = "splunk.hec.token";
    private static final String TOKEN_DOC = "The authorization token to use when writing data to splunk.";

    public static final String HEC_URI_CONF = "splunk.hec.uri";
    private static final String URI_DOC = "The URI of the remote splunk to write data do.";

    public static final String RAW_CONF = "splunk.hec.raw";
    private static final String RAW_DOC = "Flag to determine if use /raw HEC endpoint when indexing data to Splunk.";
    public static final boolean RAW_DEFAULT = false;

    public static final String ACK_CONF = "splunk.hec.ack.enabled";
    private static final String ACK_DOC = "Flag to determine if use turn on HEC ACK when indexing data to Splunk.";
    public static final boolean ACK_DEFAULT = false;

    public static final String SSL_TRUSTSTORE_PATH_CONF = "splunk.hec.ssl.trust.store.path";
    private static final String SSL_TRUSTSTORE_PATH_DOC = "Path on the local disk to the certificate trust store.";

    public static final String SSL_TRUSTSTORE_PASSWORD_CONF = "splunk.hec.ssl.trust.store.password";
    private static final String SSL_TRUSTSTORE_PASSWORD_DOC = "Password for the trust store.";

    public static final String ENRICHMENT_CONF = "splunk.hec.json.event.enrichment";
    private static final String ENRICHMENT_DOC = "Enrich the JSON events by specifying key value pairs separated by comma. "
            + "Is only applicable to splunk.hec.raw=false case";
    public static final String ENRICHMENT_DEFAULT = "";

    public static final String USE_RECORD_TIMESTAMP_CONF = "splunk.hec.use.record.timestamp";
    private static final String USE_RECORD_TIMESTAMP_DOC = "Set event timestamp to Kafka record timestamp";
    public static final boolean USE_RECORD_DEFAULT = true;

    public static final String MAX_BATCH_SIZE_CONF = "splunk.hec.max.batch.size";
    private static final String MAX_BATCH_SIZE_DOC = "Max number of Kafka record to be sent to Splunk HEC for one POST";
    public static final int MAX_BATCH_SIZE_DEFAULT = 500;

    public static final String MAX_OUTSTANDING_EVENTS_CONF = "splunk.hec.max.outstanding.events";
    private static final String MAX_OUTSTANDING_EVENTS_DOC = "Number of outstanding events which are not ACKed kept in memory";
    public static final int MAX_OUTSTANDING_EVENTS_DEFAULT = 1000000;

    public static final String MAX_RETRIES_CONF = "splunk.hec.max.retries";
    private static final String MAX_RETRIES_DOC = "Number of retries for failed batches before giving up";
    public static final int MAX_RETRIES_DEFAULT = -1;

    public static final String LINE_BREAKER_CONF = "splunk.hec.raw.line.breaker";
    private static final String LINE_BREAKER_DOC = "Line breaker for /raw HEC endpoint. The line breaker can help Splunkd to do event breaking";
    public static final String LINE_BREAKER_DEFAULT = "";

    public static final String SSL_VALIDATE_CERTIFICATES_CONF = "splunk.hec.ssl.validate.certs";
    private static final String SSL_VALIDATE_CERTIFICATES_DOC = "Flag to determine if ssl connections should validate the "
            + "certificate of the remote host.";
    public static final boolean SSL_VALIDATE_CERTIFICATES_DEFAULT = true;

    public static final String SOCKET_TIMEOUT_CONF = "splunk.hec.socket.timeout";
    private static final String SOCKET_TIMEOUT_DOC = "Max duration in seconds to read / write data to network before its timeout.";
    public static final int SOCKET_TIMEOUT_DEFAULT = 60;

    public static final String MAX_HTTP_CONNECTION_PER_CHANNEL_CONF = "splunk.hec.max.http.connection.per.channel";
    private static final String MAX_HTTP_CONNECTION_PER_CHANNEL_DOC = "Max HTTP connections pooled for one HEC Channel "
            + "when posting events to Splunk.";
    public static final int MAX_HTTP_CONNECTION_PER_CHANNEL_DEFAULT = 2;

    public static final String TOTAL_HEC_CHANNEL_CONF = "splunk.hec.total.channels";
    private static final String TOTAL_HEC_CHANNEL_DOC = "Total HEC Channels used to post events to Splunk. When enabling HEC ACK, "
            + "setting to the same or 2X number of indexers is generally good.";
    public static final int TOTAL_HEC_CHANNEL_DEFAULT = 2;

    public static final String EVENT_BATCH_TIMEOUT_CONF = "splunk.hec.event.timeout"; // seconds
    private static final String EVENT_BATCH_TIMEOUT_DOC = "Max duration in seconds to wait commit response after sending to Splunk.";
    public static final int EVENT_BATCH_TIMEOUT_DEFAULT = 300;

    public static final String HTTP_KEEPALIVE_CONF = "splunk.hec.http.keepalive";
    private static final String HTTP_KEEPALIVE_DOC = "Boolean configuration to enable/disable Keep-alive HTTP Connection to HEC server";
    public static final boolean HTTP_KEEPALIVE_DEFAULT = true;

    public static final String ACK_POLL_INTERVAL_CONF = "splunk.hec.ack.poll.interval"; // seconds
    private static final String ACK_POLL_INTERVAL_DOC = "Interval in seconds to poll event ACKs from Splunk.";
    public static final int ACK_POLL_INTERVAL_DEFAULT = 10;

    public static final String ACK_POLL_THREADS_CONF = "splunk.hec.ack.poll.threads";
    private static final String ACK_POLL_THREADS_DOC = "Number of threads used to query ACK for single task.";
    public static final int ACK_POLL_THREADS_DEFAULT = 2;

    public static final String HEC_THREDS_CONF = "splunk.hec.threads";
    private static final String HEC_THREADS_DOC = "Number of threads used to POST events to Splunk HEC in single task";
    public static final int HEC_THREADS_DEFAULT = 1;

    public static final String TRACK_DATA_CONF = "splunk.hec.track.data";
    private static final String TRACK_DATA_DOC = "Track data loss, latency or not. Is only applicable to splunk.hec.raw=false case";
    public static final boolean TRACK_DATA_DEFAULT = false;

    final String indexes;
    final String sourcetypes;
    final String sources;
    final String splunkToken;
    final String splunkURI;
    final boolean raw;
    final boolean ack;

    final String trustStorePath;
    final String trustStorePassword;
    final boolean usingTrustStore;

    final Map<String, String> enrichments;
    final boolean useRecordTimestamp;
    final int maxBatchSize;
    final int maxOutstandingEvents;
    final int maxRetries;
    final String lineBreaker;
    final Map<String, Map<String, String>> topicMetas;

    final boolean validateCertificates;
    final int socketTimeout;
    final int maxHttpConnPerChannel;
    final int totalHecChannels;
    final int eventBatchTimeout;
    final boolean httpKeepAlive;
    final int ackPollInterval;
    final int ackPollThreads;
    final boolean trackData;
    final int numberOfThreads;

    SplunkSinkConnectorConfig(Map<String, String> taskConfig) {
        super(conf(), taskConfig);

        //Splunk specific configurations
        this.indexes = getString(INDEX_CONF);
        this.sourcetypes = getString(SOURCETYPE_CONF);
        sources = getString(SOURCE_CONF);
        splunkToken = getPassword(TOKEN_CONF).value();
        splunkURI = getString(HEC_URI_CONF);
        raw = getBoolean(RAW_CONF);
        ack = getBoolean(ACK_CONF);

        //Security specific configurations
        trustStorePath = getString(SSL_TRUSTSTORE_PATH_CONF);
        trustStorePassword = getPassword(SSL_TRUSTSTORE_PASSWORD_CONF).value();
        usingTrustStore = !StringUtils.isBlank(this.trustStorePath);

        // Event and batch specific configuration
        enrichments = parseEnrichments(getString(ENRICHMENT_CONF));
        useRecordTimestamp = getBoolean(USE_RECORD_TIMESTAMP_CONF);
        maxBatchSize = getInt(MAX_BATCH_SIZE_CONF);
        maxOutstandingEvents = getInt(MAX_OUTSTANDING_EVENTS_CONF);
        maxRetries = getInt(MAX_RETRIES_CONF);
        lineBreaker = getString(LINE_BREAKER_CONF);
        topicMetas = initMetaMap(taskConfig);

        // HEC Client specific configurations
        validateCertificates = getBoolean(SSL_VALIDATE_CERTIFICATES_CONF);
        socketTimeout = getInt(SOCKET_TIMEOUT_CONF);
        maxHttpConnPerChannel = getInt(MAX_HTTP_CONNECTION_PER_CHANNEL_CONF);
        totalHecChannels = getInt(TOTAL_HEC_CHANNEL_CONF);
        eventBatchTimeout = getInt(EVENT_BATCH_TIMEOUT_CONF);
        httpKeepAlive = getBoolean(HTTP_KEEPALIVE_CONF);
        ackPollInterval = getInt(ACK_POLL_INTERVAL_CONF);
        ackPollThreads = getInt(ACK_POLL_THREADS_CONF);
        numberOfThreads = getInt(HEC_THREDS_CONF);
        trackData = getBoolean(TRACK_DATA_CONF);

    }

    public static ConfigDef conf() {
        return new ConfigDef()
            .define(INDEX_CONF, ConfigDef.Type.STRING, INDEX_DEFAULT, ConfigDef.Importance.MEDIUM, INDEX_DOC)
            .define(SOURCETYPE_CONF, ConfigDef.Type.STRING, SOURCETYPE_DEFAULT, ConfigDef.Importance.MEDIUM, SOURCETYPE_DOC)
            .define(SOURCE_CONF, ConfigDef.Type.STRING, SOURCE_DEFAULT, ConfigDef.Importance.MEDIUM, SOURCE_DOC)
            .define(TOKEN_CONF, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, TOKEN_DOC)
            .define(HEC_URI_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, URI_DOC)
            .define(RAW_CONF, ConfigDef.Type.BOOLEAN, RAW_DEFAULT, ConfigDef.Importance.MEDIUM, RAW_DOC)
            .define(ACK_CONF, ConfigDef.Type.BOOLEAN, ACK_DEFAULT, ConfigDef.Importance.MEDIUM, ACK_DOC)
            .define(SSL_TRUSTSTORE_PATH_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SSL_TRUSTSTORE_PATH_DOC)
            .define(SSL_TRUSTSTORE_PASSWORD_CONF, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, SSL_TRUSTSTORE_PASSWORD_DOC)
            .define(ENRICHMENT_CONF, ConfigDef.Type.STRING, ENRICHMENT_DEFAULT, ConfigDef.Importance.LOW, ENRICHMENT_DOC)
            .define(USE_RECORD_TIMESTAMP_CONF, ConfigDef.Type.BOOLEAN, USE_RECORD_DEFAULT, ConfigDef.Importance.MEDIUM, USE_RECORD_TIMESTAMP_DOC)
            .define(MAX_BATCH_SIZE_CONF, ConfigDef.Type.INT, MAX_BATCH_SIZE_DEFAULT, ConfigDef.Importance.MEDIUM, MAX_BATCH_SIZE_DOC)
            .define(MAX_OUTSTANDING_EVENTS_CONF, ConfigDef.Type.INT, MAX_OUTSTANDING_EVENTS_DEFAULT, ConfigDef.Importance.MEDIUM, MAX_OUTSTANDING_EVENTS_DOC)
            .define(MAX_RETRIES_CONF, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT, ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC)
            .define(LINE_BREAKER_CONF, ConfigDef.Type.STRING, LINE_BREAKER_DEFAULT, ConfigDef.Importance.MEDIUM, LINE_BREAKER_DOC)
            .define(SSL_VALIDATE_CERTIFICATES_CONF, ConfigDef.Type.BOOLEAN, SSL_VALIDATE_CERTIFICATES_DEFAULT, ConfigDef.Importance.MEDIUM, SSL_VALIDATE_CERTIFICATES_DOC)
            .define(SOCKET_TIMEOUT_CONF, ConfigDef.Type.INT, SOCKET_TIMEOUT_DEFAULT, ConfigDef.Importance.LOW, SOCKET_TIMEOUT_DOC)
            .define(MAX_HTTP_CONNECTION_PER_CHANNEL_CONF, ConfigDef.Type.INT, MAX_HTTP_CONNECTION_PER_CHANNEL_DEFAULT, ConfigDef.Importance.MEDIUM, MAX_HTTP_CONNECTION_PER_CHANNEL_DOC)
            .define(TOTAL_HEC_CHANNEL_CONF, ConfigDef.Type.INT, TOTAL_HEC_CHANNEL_DEFAULT, ConfigDef.Importance.HIGH, TOTAL_HEC_CHANNEL_DOC)
            .define(EVENT_BATCH_TIMEOUT_CONF, ConfigDef.Type.INT, EVENT_BATCH_TIMEOUT_DEFAULT, ConfigDef.Importance.MEDIUM, EVENT_BATCH_TIMEOUT_DOC)
            .define(HTTP_KEEPALIVE_CONF, ConfigDef.Type.BOOLEAN, HTTP_KEEPALIVE_DEFAULT, ConfigDef.Importance.MEDIUM, HTTP_KEEPALIVE_DOC)
            .define(ACK_POLL_INTERVAL_CONF, ConfigDef.Type.INT, ACK_POLL_INTERVAL_DEFAULT, ConfigDef.Importance.MEDIUM, ACK_POLL_INTERVAL_DOC)
            .define(ACK_POLL_THREADS_CONF, ConfigDef.Type.INT, ACK_POLL_THREADS_DEFAULT, ConfigDef.Importance.MEDIUM, ACK_POLL_THREADS_DOC)
            .define(HEC_THREDS_CONF, ConfigDef.Type.INT, HEC_THREADS_DEFAULT, ConfigDef.Importance.LOW, HEC_THREADS_DOC)
            .define(TRACK_DATA_CONF, ConfigDef.Type.BOOLEAN, TRACK_DATA_DEFAULT, ConfigDef.Importance.LOW, TRACK_DATA_DOC);
    }

    /**
    * Configuration Method to setup all settings related to Splunk HEC Client.
    *
    * @see      HecConfig
    * @since    1.0.0
    */
    public HecConfig getHecConfig() {
        HecConfig config = new HecConfig(Arrays.asList(splunkURI.split(",")), splunkToken);
        config.setDisableSSLCertVerification(!validateCertificates)
                .setSocketTimeout(socketTimeout)
                .setMaxHttpConnectionPerChannel(maxHttpConnPerChannel)
                .setTotalChannels(totalHecChannels)
                .setEventBatchTimeout(eventBatchTimeout)
                .setHttpKeepAlive(httpKeepAlive)
                .setAckPollInterval(ackPollInterval)
                .setAckPollThreads(ackPollThreads)
                .setEnableChannelTracking(trackData);
        return config;
    }

    /**
     * Helper function to determine if any metadata values have been provided through
     * the Kafka Connect configuration. Used in conjunction with creating a Raw Event Batch. If Metadata is
     * configured records must be partitioned accordingly.
     *
     * @see       SplunkSinkTask#handleRaw(Collection)
     * @see       SplunkSinkTask#partitionRecords(Collection)
     * @since     1.0.0
     */
    public boolean hasMetaDataConfigured() {
        return (indexes != null && !indexes.isEmpty()
                || (sources != null && !sources.isEmpty())
                || (sourcetypes != null && !sourcetypes.isEmpty()));
    }

    /**
     * String representation of SplunkSinkConnectorConfig
     *
     * @return  current representation of SplunkSinkConnectorConfig
     * @since   1.0.0
     */
    public String toString() {
        return "indexes:" + indexes + ", "
        + "sourcetypes:" + sourcetypes + ", "
        + "sources:" + sources + ", "
        + "splunkURI:" + splunkURI + ", "
        + "raw:" + raw + ", "
        + "ack:" + ack + ", "
        + "trustStorePath:" + trustStorePath + ", "
        + "usingTrustStore" + usingTrustStore + ","
        + "enrichments: " + getString(ENRICHMENT_CONF) + ", "
        + "useRecordTimestamp: " + useRecordTimestamp + ", "
        + "maxBatchSize: " + maxBatchSize + ", "
        + "maxOutstandingEvents: " + maxOutstandingEvents + ", "
        + "maxRetries: " + maxRetries + ", "
        + "lineBreaker: " + lineBreaker + ", "
        + "validateCertificates:" + validateCertificates + ", "
        + "socketTimeout:" + socketTimeout + ", "
        + "maxHttpConnectionPerChannel:" + maxHttpConnPerChannel + ", "
        + "totalHecChannels:" + totalHecChannels + ", "
        + "eventBatchTimeout:" + eventBatchTimeout + ", "
        + "httpKeepAlive:" + httpKeepAlive + ", "
        + "ackPollInterval:" + ackPollInterval + ", "
        + "ackPollThreads:" + ackPollThreads + ", "
        + "numberOfThreads: " + numberOfThreads + ", "
        + "trackData: " + trackData;
    }

    private static String[] split(String data, String sep) {
        if (data != null && !data.trim().isEmpty()) {
            return data.trim().split(sep);
        }
        return null;
    }

    private static Map<String, String> parseEnrichments(String enrichment) {
        String[] kvs = split(enrichment, ",");
        if (kvs == null) {
            return null;
        }

        Map<String, String> enrichmentKvs = new HashMap<>();
        for (final String kv: kvs) {
            String[] kvPairs = split(kv, "=");
            if (kvPairs.length != 2) {
                throw new ConfigException("Invalid enrichment: " + enrichment+ ". Expect key value pairs and separated by comma");
            }
            enrichmentKvs.put(kvPairs[0], kvPairs[1]);
        }
        return enrichmentKvs;
    }

    private String getMetaForTopic(String[] metas, int expectedLength, int curIdx, String confKey) {
        if (metas == null) {
            return null;
        }

        if (metas.length == 1) {
            return metas[0];
        } else if (metas.length == expectedLength) {
            return metas[curIdx];
        } else {
            throw new ConfigException("Invalid " + confKey + " configuration=" + metas);
        }
    }

    private Map<String, Map<String, String>> initMetaMap(Map<String, String> taskConfig) {
        String[] topics = split(taskConfig.get(SinkConnector.TOPICS_CONFIG), ",");
        String[] topicIndexes = split(indexes, ",");
        String[] topicSourcetypes = split(sourcetypes, ",");
        String[] topicSources = split(sources, ",");

        Map<String, Map<String, String>> metaMap = new HashMap<>();
        int idx = 0;
        for (String topic: topics) {
            HashMap<String, String> topicMeta = new HashMap<>();
            String meta = getMetaForTopic(topicIndexes, topics.length, idx, INDEX_CONF);
            if (meta != null) {
                topicMeta.put(INDEX, meta);
            }

            meta = getMetaForTopic(topicSourcetypes, topics.length, idx, SOURCETYPE_CONF);
            if (meta != null) {
                topicMeta.put(SOURCETYPE, meta);
            }

            meta = getMetaForTopic(topicSources, topics.length, idx, SOURCE_CONF);
            if (meta != null) {
                topicMeta.put(SOURCE, meta);
            }

            metaMap.put(topic, topicMeta);
            idx += 1;
        }
        return metaMap;
    }
}

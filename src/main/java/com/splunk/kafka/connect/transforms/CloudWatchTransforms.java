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
package com.splunk.kafka.connect.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.common.config.AbstractConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class CloudWatchTransforms implements Transformation<SinkRecord> {
    private static final Logger log = LoggerFactory.getLogger(CloudWatchTransforms.class);

    public static final String OVERVIEW_DOC =
        "Transformation of nested CloudWatch event from a single JSON object with nested metrics" +
        "into an expanded single message separated by a line break for easier ingestion into Splunk";

    private static final String DELIMITER_CONFIG = "splunk-line-break";
    private static final String DELIMITER_DEFAULT = "####";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(DELIMITER_CONFIG,ConfigDef.Type.STRING, DELIMITER_DEFAULT, ConfigDef.Importance.MEDIUM,
        "Delimiter for splitting CloudWatch events");

    private static final String PURPOSE = "flattening";

    private String delimiter;
    private ObjectMapper mapper = new ObjectMapper();


    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        delimiter = config.getString(DELIMITER_CONFIG);
    }

    @Override
    public SinkRecord apply(SinkRecord record) {

        String concatenatedEvents = "";
        SinkRecord transformedRecord = null;

        try{
            mapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
            CloudWatchEvent cwe = mapper.readValue(record.value().toString(), CloudWatchEvent.class);

            LogEvents[] logEvents = cwe.getLogEventArray();
            int index = 0;

            for(LogEvents logEvent:logEvents) {
                Message message = mapper.readValue(logEvent.getUnescapedMessage(),Message.class);
                CloudWatchEvent tempcwe = cwe;
                tempcwe.setLogEventIndex(index);
                tempcwe.getLogEvents().setMessageObject(message);

                concatenatedEvents += mapper.writeValueAsString(tempcwe);
                index++;
                concatenatedEvents += delimiter;

            }
            log.debug(concatenatedEvents);
            transformedRecord = record.newRecord(record.topic(),record.kafkaPartition(),record.keySchema(),record.key(),record.valueSchema(),concatenatedEvents,record.timestamp());
    }
    catch (Exception e) {
        e.printStackTrace();
    }
        return transformedRecord;
    }

    @Override
    public void close() {

    }

    @Override
    public ConfigDef config(){
    return CONFIG_DEF;
    }
}

class SimpleConfig extends AbstractConfig {

    public SimpleConfig(ConfigDef configDef, Map<?, ?> originals) {
        super(configDef, originals, false);
    }

}

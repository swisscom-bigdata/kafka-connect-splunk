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
package com.splunk.kafka.connect;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.common.config.AbstractConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

        /*log.info("Transformation Attempted");
        String jsonrecord = record.value().toString();
        log.info(jsonrecord);
        Map<String, Object> jsonMap = null;
        try {
            jsonMap = mapper.readValue(record.value().toString(), new TypeReference<Map<String, Object>>(){});
        }catch (IOException ex) {
            log.error("Error reading JSON file");
        }

        Iterator itr = jsonMap.keySet().iterator();

        while(itr.hasNext()) {
        Object element = itr.next();
            log.info(element.toString());
        }

        Object logs = jsonMap.get("logEvents");
        log.info(logs.toString());
        */
        try {
            JsonNode root = mapper.readTree(record.value().toString());
            String resultOriginal = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
            //log.info("Before Update " + resultOriginal);

            JsonNode logEventsNode = root.path("logEvents");
            String logEvents = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(logEventsNode);
           // log.info(logEvents);
            List<String> logEventsList = new ArrayList<String>();

            if(logEventsNode.isArray()) {
                for(final JsonNode eventNode: logEventsNode) {
                    logEventsList.add(eventNode.toString());
                }
            }

            for(int i = 0; i < logEvents.length(); i++) {

            }

        }catch(Exception ex) {

        }



        return record;
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

package com.splunk.kafka.connect;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import java.util.*;

import java.io.IOException;


public class KafkaDiag {
    AdminClient adminClient;
    KafkaFuture<String> kafkaFuture;
    public KafkaDiag() {
        try {
            adminClient = null;
            kafkaFuture = null;

            adminClient = AdminClient.create(createProperties());
            CreateTopicsResult result = adminClient.createTopics(Arrays.asList(new NewTopic("dondondondond", 1, (short)1)));
            System.out.println("Topic Created: ");
            /*ListTopicsResult topics = adminClient.listTopics();
            Set<String> topicNames = topics.names().get();
            for(String topic: topicNames) {
                System.out.println(topic); */
        } catch (Exception ex) {
            System.out.println("Exception Caught");
        }
    }

    public Properties createProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id", "kafka-client-1");
        return props;
    }
}
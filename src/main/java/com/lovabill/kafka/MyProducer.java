package com.lovabill.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {

    private static String groupId;
    private static String topic;
    private static String brokers;

    private KafkaProducer<String, String> producer;

    public void initialize(String topic, String brokers) {

        MyProducer.topic = topic;
        MyProducer.brokers = brokers;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    public void sendMessage(String message) {
        ProducerRecord<String, String> data = new ProducerRecord<>(topic, "message", message);
        long startTime = System.currentTimeMillis();
        producer.send(data);
        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Sent this sentence: " + message + " in " + elapsedTime + " ms");
        producer.flush();
    }

    public void close() {
        producer.close();
    }

}
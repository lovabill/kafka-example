package com.lovabill.kafka;

public class App {
    public static void main(String[] args) {
        MyProducer myProducer = new MyProducer();
        myProducer.initialize("topic-test", "localhost:9092");
        myProducer.sendMessage("Do that!");
        myProducer.close();

        MyConsumer myConsumer = new MyConsumer();
        myConsumer.initialize("group-test", "topic-test", "localhost:9092");
        myConsumer.listen();
    }
}
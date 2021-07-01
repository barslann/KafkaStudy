package com.coderman.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {


    public static void main(String[] args) {
        //Create producer properties
        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");  -> Old way. user producerconfig class
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

       // create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // create a procedur record
        ProducerRecord<String,String> producerRecord =
                new ProducerRecord<>("my-topic","hello2");
        // send the data - asynchronous
        producer.send(producerRecord);

        producer.flush();
        producer.close();

    }
}


/*
*  3 steps to create producer
*
*       Create producer properties
*       create the producer
*       send data
* */
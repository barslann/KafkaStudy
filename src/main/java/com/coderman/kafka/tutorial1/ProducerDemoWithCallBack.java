package com.coderman.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    public static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
    public static void main(String[] args) {
        //Create producer properties
        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");  -> Old way. user producerconfig class
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // create a producer record
            ProducerRecord<String,String> producerRecord =
                    new ProducerRecord<>("my-topic","hello " + i);
            // send the data - asynchronous
            producer.send(producerRecord, new Callback() {
                // executes every time I get record being sent or there is an exception
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if(e == null){
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n"+
                                "Partition: " + recordMetadata.partition() +"\n"+
                                "Offset: " + recordMetadata.offset() + "\n"+
                                "Timestamp: " + recordMetadata.timestamp());
                    }else{
                        logger.error("Error while producing",e);

                    }

                }
            });
        }
        


        //flush data
        producer.flush();

        //flush and close producer
        producer.close();

    }
}

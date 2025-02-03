package dev.gohard.learning_kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class TestProducer {
    private static Properties props = new Properties();

    public static void main(String[] args) {
        
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<String,String>("CustomerCountry", "Precision Products", "France");

        try {
            producer.send(record).get();
            producer.send(record, new DemoProducerCallback());
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

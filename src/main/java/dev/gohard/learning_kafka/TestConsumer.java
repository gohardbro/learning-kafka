package dev.gohard.learning_kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestConsumer {
    Properties props = new Properties();
    
    public void testConsumer() {
        props.put("bootstrap.servers", "broke1:9092,broker2:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("customerCountries"));
        consumer.subscribe(Pattern.compile("test.*"));

        // Commit offset sync
        commitSyncro(consumer);

        // Commit offset async
        // commitAsyncro(consumer);
    }

    // 정상적인 상황에서는 commitAsync, 컨슈머를 닫는 상황에서는 '다음 커밋'이라는 것이 있을 수 없으므로 commitSync를 호출
    private void commitSyncNasync(KafkaConsumer<String, String> consumer) {
        Duration timeout = Duration.ofMillis(100);

        try {
            while (true) { // 원래는 !closing 
                ConsumerRecords<String, String> records = consumer.poll(timeout);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = $s\n", 
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                consumer.commitAsync();
            }
            // consumer.commitSync();
        } catch (Exception e) {
            log.error("Unexpected error", e);
        } finally {
            consumer.close();
        }
    }

    private void commitAsyncro(KafkaConsumer<String, String> consumer) {
        Duration timeout = Duration.ofMillis(100);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = $s\n", 
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
            
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) log.error("Commit failed for offset {}", offsets, exception);
                }
            });
        }
    }

    private void commitSyncro(KafkaConsumer<String, String> consumer) {
        Duration timeout = Duration.ofMillis(100);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = $s\n", 
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }

            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                log.error("commit failed", e);
            }
        }
    }
}

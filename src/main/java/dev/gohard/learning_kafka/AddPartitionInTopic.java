package dev.gohard.learning_kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;

// 5.7.1 토픽에 파티션 추가하기
public class AddPartitionInTopic {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Map<String, NewPartitions> newPartitions = new HashMap<>();
        final String TOPIC_NAME = "test_add_partition";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(props);
        
        // TODO: AdminClient를 사용해서 필요한 작업을 수행한다.
        // 1. 토픽의 현재 파티션 수를 조회합니다.
        DescribeTopicsResult describeTopicsResult = admin.describeTopics(Collections.singleton(TOPIC_NAME));
        Map<String, TopicDescription> descriptionMap = describeTopicsResult.allTopicNames().get();
        TopicDescription topicDescription = descriptionMap.get(TOPIC_NAME);
        int currentPartitionSize = topicDescription.partitions().size();
        
        // 2. createPartitions 메서드를 사용하여 파티션 추가 요청을 합니다.
        // 토픽을 확장할 때는 새로 추가될 파티션의 수가 아닌, 파티션이 추가된 뒤의 파티션 수를 지정
        newPartitions.put(TOPIC_NAME, NewPartitions.increaseTo(currentPartitionSize + 2));
        admin.createPartitions(newPartitions).all().get();

        admin.close(Duration.ofSeconds(30));
    }
}

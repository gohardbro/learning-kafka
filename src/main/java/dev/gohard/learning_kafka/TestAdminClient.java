package dev.gohard.learning_kafka;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;

public class TestAdminClient {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(props);
        
        // TODO: AdminClient를 사용해서 필요한 작업을 수행한다.
        ListTopicsResult topics = admin.listTopics();
        topics.names().get().forEach(System.out::println);

        admin.close(Duration.ofSeconds(30));
    }
}

package dev.gohard.learning_kafka;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;

public class ClusterMetadata {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(props);
        
        // TODO: AdminClient를 사용해서 필요한 작업을 수행한다.
        // 클러스터 메타데이터 보기
        DescribeClusterResult cluster = admin.describeCluster();

        System.out.println("Connected to cluster " + cluster.clusterId().get());
        System.out.println("The brokers in the cluster are:");
        cluster.nodes().get().forEach(node -> System.out.println("* " + node));
        System.out.println("The controller is: " + cluster.controller().get());

        admin.close(Duration.ofSeconds(30));
    }
}

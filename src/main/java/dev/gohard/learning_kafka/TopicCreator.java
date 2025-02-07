package dev.gohard.learning_kafka;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import java.util.*;

// 원하는 수만큼 브로커를 설정해서 초기화할 수 있는 테스트 클래스
public class TopicCreator {
    private AdminClient admin;
    
    public TopicCreator(AdminClient admin) {
        this.admin = admin;
    }

    // 토픽 이름이 "test"로 시작할 경우 생성하는 예제 메서드
    public void maybeCreateTopic(String topicName) throws ExecutionException, InterruptedException {
        Collection<NewTopic> topics = new ArrayList<>();
        topics.add(new NewTopic(topicName, 1, (short) 1));
        if (topicName.toLowerCase().startsWith("test")) {
            admin.createTopics(topics);
            // 설정 변경
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            ConfigEntry compaction = new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            Collection<AlterConfigOp> configOp = new ArrayList<>();
            configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET));
            Map<ConfigResource, Collection<AlterConfigOp>> alterConf = new HashMap<>();
            admin.incrementalAlterConfigs(alterConf).all().get();
        }
    }
}
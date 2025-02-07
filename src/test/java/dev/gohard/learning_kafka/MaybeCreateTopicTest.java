package dev.gohard.learning_kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MaybeCreateTopicTest {
    private AdminClient admin;
    
    @BeforeEach
    public void setup() {
        Node broker = new Node(0, "localhost", 9092);
        this.admin = spy(new MockAdminClient(List.of(broker), broker));

        // 아래 내용이 없으면 테스트가
        // java.lang.UnsupportedOperationException: Not implemented yet 예외를 발생시킨다.
        AlterConfigsResult emptyResult = mock(AlterConfigsResult.class);
        doReturn(KafkaFuture.completedFuture(null)).when(emptyResult).all();
    }

    @Test
    public void testCreateTestTopic() throws ExecutionException, InterruptedException {
        TopicCreator tc = new TopicCreator(admin);
        tc.maybeCreateTopic("test.is.a.test.topic");
        verify(admin, times(1)).createTopics(any());
    }

    // @Test
    // public void testNotTopic() throws ExecutionException, InterruptedException {
    //     TopicCreator tc = new TopicCreator(admin);
    //     tc.maybeCreateTopic("not.a.test");
    //     verify(admin, never()).createTopics(any());
    // }
}

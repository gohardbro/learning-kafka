package dev.gohard.learning_kafka;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CountingProducerInterceptor implements ProducerInterceptor{
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    static AtomicLong numSent = new AtomicLong(0);
    static AtomicLong numAcked = new AtomicLong(0);

    @Override
    public void configure(Map<String, ?> map) {
        Long windowSize = Long.valueOf(
            (String) map.get("counting.intceptor.window.size.ms"));
            executorService.scheduleAtFixedRate(CountingProducerInterceptor::run, windowSize, windowSize, TimeUnit.MILLISECONDS);
    }

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        numSent.incrementAndGet();
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        numAcked.incrementAndGet();
    }

    @Override
    public void close() {
        executorService.shutdownNow();
    }

    public static void run() {
        System.out.println(numSent.getAndSet(0));
        System.out.println(numAcked.getAndSet(0));
    }

}

package io.nermdev.kafka.targetpartitionproducer;

import com.github.javafaker.Faker;
import com.google.gson.Gson;
import lombok.Data;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class Main {
    static final String RECORD_COUNT = "app.target.count";
    static final String TARGET_TOPIC = "app.target.topic";
    static final String TARGET_PARTITION = "app.target.partition";
    static final Logger log = LoggerFactory.getLogger(Main.class);
    static final Gson gson = new Gson();
    public static final String SHUTTING_DOWN = "=== Shutting Down ===";

    public static void main(String[] args) {
        log.info("=== Starting ===");
        final Properties properties = ConfigUtils.getProperties(args);
        log.info("=== App Config === \n{}", properties);
        final int sendCount =Integer.parseInt(properties.getProperty(RECORD_COUNT, "1"));
        final String topic = properties.getProperty(TARGET_TOPIC, "test.topic.tpp");
        final Integer partition = Integer.parseInt(properties.getProperty(TARGET_PARTITION, "0"));


        runTopicValidation(properties, topic, partition);


        log.info("=== Creating Producer ===");
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer())) {
            for (int i = 0; i < sendCount; i++) {
                final Message message = new Message(Main.class.getSimpleName(), Faker.instance().lorem().sentence());
                final String json = gson.toJson(message);
                final ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, null, json);
                producer.send(
                        record,
                        (recordMetadata, e) -> Optional.ofNullable(e)
                            .ifPresentOrElse(
                                    exception -> log.error("ERROR: Could not send {}. Cause: {}", json, exception.getMessage()),
                                    () -> log.info("SUCCESS : {}-{}-{} : {}", topic, partition, recordMetadata.offset(), json)
                            ));
            }
            log.info("=== FINISHED ===");
        } finally {
            log.info("=== Producer Closed ===");
        }
        log.info(SHUTTING_DOWN);

    }

    private static void runTopicValidation(Properties properties, String topic, Integer partition) {
        try (final AdminClient adminClient = AdminClient.create(properties)) {
            try {
                final DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topic));
                final Map<String, KafkaFuture<TopicDescription>> stringKafkaFutureMap = describeTopicsResult.topicNameValues();

                final Collection<KafkaFuture<TopicDescription>> futures = stringKafkaFutureMap.values();
                for (var future : futures) {
                    final List<TopicPartitionInfo> partitions = future.get().partitions();

                    if (partitions.size() < partition) {
                        log.error("Topic ({}) partition size ({}) incompatible with app.target.partition ({})", topic, partitions.size(), partition);
                        log.error(SHUTTING_DOWN);
                        System.exit(1);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Error Message : {}", e.getMessage());
                log.error(SHUTTING_DOWN);
                System.exit(1);
            }
        }
    }

    @Data
    static class Message {
        private final String origin;
        private final String msg;
    }
}

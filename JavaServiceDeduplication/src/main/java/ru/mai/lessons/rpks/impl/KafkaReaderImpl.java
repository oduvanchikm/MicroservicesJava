package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class KafkaReaderImpl implements KafkaReader {
    private final Config config;
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final KafkaWriter kafkaWriter;
    private final RuleProcessor ruleProcessor;

    public KafkaReaderImpl(Config config) {
        this.config = config;
        this.kafkaWriter = new KafkaWriterImpl(config);
        this.ruleProcessor = new RuleProcessorImpl(config);
        this.kafkaConsumer = new KafkaConsumer<>(getKafkaProperties());
        this.kafkaConsumer.subscribe(Collections.singletonList("test_topic_in"));
        log.info("KafkaReader started");
    }

    private Properties getKafkaProperties() {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", config.getString("kafka.consumer.bootstrap.servers"));
        properties.put("group.id", config.getString("kafka.consumer.group.id"));
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("auto.offset.reset", "earliest");

        return properties;
    }

    @Override
    public void processing() {
        log.info("Start processing method in KafkaReaderImpl");
        boolean isTrue = true;
        DbReaderImpl dbReader = new DbReaderImpl(config);
        try {
            dbReader.startPeriodicRuleUpdate().await();
            while (isTrue) {
                var records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (var r : records) {
                    String message = r.value();
                    var rules = dbReader.getRules();
                    log.info("Received message: {}", message);

                    var filterMessages = ruleProcessor.processing(Message.builder().value(message).build(), rules);
                    if (filterMessages.isDeduplicationState()) {
                        kafkaWriter.processing(filterMessages);
                    }
                }
            }
        } catch (Exception e) {
            dbReader.stopPeriodicRuleUpdate();
            log.error("Error: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}

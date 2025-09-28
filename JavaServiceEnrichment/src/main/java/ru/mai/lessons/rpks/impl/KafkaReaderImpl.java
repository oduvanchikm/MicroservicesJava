package ru.mai.lessons.rpks.impl;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
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

        String topic = config.getString("kafka.consumer.topic.in");
        log.info("Subscribing to topic: {}", topic);
        this.kafkaConsumer.subscribe(Collections.singletonList(topic));

        log.info("KafkaReader started");
    }

    private Properties getKafkaProperties() {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", config.getString("kafka.consumer.bootstrap.servers"));
        properties.put("group.id", config.getString("kafka.consumer.group.id"));
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("auto.offset.reset", config.getString("kafka.consumer.auto.offset.reset"));

        return properties;
    }

    @Override
    public void processing() {
        log.info("Start processing method in KafkaReaderImpl");
        boolean isTrue = true;
        DbReaderImpl dbReader = new DbReaderImpl(config);
        try {

            dbReader.startPeriodicRuleUpdate();

            while (isTrue) {
                var records = kafkaConsumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    log.warn("Received 0 records from Kafka. Current subscription: {}", kafkaConsumer.subscription());
                    continue;
                }

                log.info("Received {} records from Kafka", records.count());

                for (var r : records) {
                    String message = r.value();
                    var rules = dbReader.getRules();
                    log.info("Received message: {}", message);

                    var enrichmentMessages = ruleProcessor.processing(Message.builder().value(message).build(), rules);
                    log.info("Received message2: {}", enrichmentMessages);
                    kafkaWriter.processing(enrichmentMessages);
                }
            }
        } catch (Exception e) {
            log.error("Error !!!!!!11!: {}", e.getMessage(), e);
        } finally {
            try {
                dbReader.stopPeriodicRuleUpdate();
                kafkaConsumer.close();
                if (ruleProcessor instanceof RuleProcessorImpl processorImpl) {
                    processorImpl.shutdown();
                }
            } catch (Exception ex) {
                log.error("Error !!11!!!: {}", ex.getMessage(), ex);
            }
            Thread.currentThread().interrupt();
        }
    }
}

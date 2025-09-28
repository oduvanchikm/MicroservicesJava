package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Properties;

@Slf4j
public class KafkaWriterImpl implements KafkaWriter {
    private final Config config;
    private final KafkaProducer<String, String> producer;

    public KafkaWriterImpl(Config config) {
        this.config = config;
        this.producer = new KafkaProducer<>(getKafkaProperties());
    }

    private Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getString("kafka.producer.bootstrap.servers"));
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        return properties;
    }

    @Override
    public void processing(Message message) {
        producer.send(new ProducerRecord<>(config.getString("kafka.consumer.topic.out"), message.getValue()));
        log.info("Message sent");
    }
}

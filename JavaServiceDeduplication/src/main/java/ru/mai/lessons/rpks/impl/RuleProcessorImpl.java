package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RedisClient;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {
    private final Config config;

    RuleProcessorImpl(Config config) {
        this.config = config;
    }

    @Override
    public Message processing(Message message, Rule[] rules) {
        log.info("Processing message in Rule Processor");
        try {
            RedisClient redisClient = new RedisClientImpl(config);

            if (rules == null || rules.length == 0) {
                message.setDeduplicationState(true);
                return message;
            }

            if (message.getValue() == null || message.getValue().isEmpty()) {
                message.setDeduplicationState(false);
                return message;
            }

            boolean boolResult = redisClient.checkDuplicate(message, rules);
            message.setDeduplicationState(boolResult);
            return message;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return message;
    }
}

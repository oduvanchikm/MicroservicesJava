package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {
    private final MongoDBClientEnricher clientEnricher;

    public RuleProcessorImpl(Config config) {
        this.clientEnricher = new MongoDBClientEnricherImpl(config);
    }

    @Override
    public Message processing(Message message, Rule[] rules) {
        log.info("Processing message with {} rules", rules != null ? rules.length : 0);

        if (rules == null || rules.length == 0) {
            log.warn("No rules provided, returning original message");
            return message;
        }

        Message processedMessage = message;
        for (var rule : rules) {
            log.info("Applying rule: {}", rule);
            processedMessage = clientEnricher.processing(processedMessage, rule);
        }

        return processedMessage;
    }

    public void shutdown() {
        if (clientEnricher instanceof MongoDBClientEnricherImpl enricherImpl) {
            enricherImpl.shutdown();
        }
    }
}

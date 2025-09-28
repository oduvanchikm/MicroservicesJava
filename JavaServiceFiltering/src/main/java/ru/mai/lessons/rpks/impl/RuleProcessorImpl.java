package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import org.jooq.exception.IOException;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {
    enum filteringFunctions {
        equals,
        contains,
        not_equals,
        not_contains
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules == null || rules.length == 0) {
            throw new IllegalArgumentException("Rules must not be empty");
        }

        log.info("Processing message {}", message.getValue());

        try {
            JsonNode jsonNode = objectMapper.readTree(message.getValue());
            message.setFilterState(true);

            for (Rule rule : rules) {
                JsonNode fieldValue = jsonNode.get(rule.getFieldName());

                if (fieldValue == null) {
                    log.warn("Field '{}' not found in message", rule.getFieldName());
                    message.setFilterState(false);
                    return message;
                }

                if (!applyFilters(fieldValue.asText(), rule)) {
                    message.setFilterState(false);
                    return message;
                }
            }

        } catch (IOException | JsonProcessingException ex) {
            log.error("Exception while reading json message: {}", ex.getMessage(), ex);
            message.setFilterState(false);
            return message;
        }

        return message;
    }

    private boolean applyFilters(String fieldValue, Rule rule) {
        String filterValue = rule.getFilterValue();

        try {
            filteringFunctions filterFunction = filteringFunctions.valueOf(rule.getFilterFunctionName().toLowerCase());

            switch (filterFunction) {
                case equals:
                    return fieldValue.equals(filterValue);
                case contains:
                    return fieldValue.contains(filterValue);
                case not_equals:
                    return !fieldValue.equals(filterValue);
                case not_contains:
                    return !fieldValue.contains(filterValue);
                default:
                    throw  new UnsupportedOperationException("Unsupported filter function: " + rule.getFilterFunctionName());
            }
        } catch (IllegalArgumentException e) {
            log.error("Invalid filter function in rule: {}", rule.getFilterFunctionName());
            throw new UnsupportedOperationException("Invalid filter function: " + rule.getFilterFunctionName(), e);
        }
    }
}

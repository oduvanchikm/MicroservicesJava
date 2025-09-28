package ru.mai.lessons.rpks.impl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
public class MongoDBClientEnricherImpl implements MongoDBClientEnricher {
    private final MongoCollection<Document> collection;
    private final ObjectMapper mapper;
    private final MongoClient mongoClient;

    public MongoDBClientEnricherImpl(Config config) {
        Config mongoConfig = config.getConfig("mongo");
        String mongoConnectionString = mongoConfig.getString("connectionString");
        String mongoDatabaseName = mongoConfig.getString("database");
        String mongoCollectionName = mongoConfig.getString("collection");

        this.mongoClient = MongoClients.create(mongoConnectionString);
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseName);
        this.collection = database.getCollection(mongoCollectionName);
        this.mapper = new ObjectMapper();
        log.info("MongoDBClientEnricherImpl initialized");
    }

    @Override
    public Message processing(Message message, Rule rule) {
        log.info("start processing in MongoDBClientEnricherImpl");

        try {
            ObjectNode jsonNode = (ObjectNode) mapper.readTree(message.getValue());

            Document document = findDocument(rule);

            log.info("Found document in MongoDB: {}", document);

            if (document == null || !document.getString(rule.getFieldNameEnrichment()).equals(rule.getFieldValue())) {
                log.info("No matching document found in MongoDB, using default value: {}", rule.getFieldValueDefault());
                jsonNode.put(rule.getFieldName(), rule.getFieldValueDefault());
            } else {
                log.info("Matching document found, applying enrichment");
                jsonNode.set(rule.getFieldName(), mapper.readTree(document.toJson()));
            }

            return new Message(jsonNode.toString());

        } catch (Exception e) {
            log.error(e.getMessage());
            return message;
        }
    }

    private Document findDocument(Rule rule) {
        return collection.find(Filters.eq(rule.getFieldNameEnrichment(), rule.getFieldValue()))
                .sort(Sorts.descending("_id"))
                .first();
    }

    public void shutdown() {
        log.info("Shutting down MongoClient");
        mongoClient.close();
    }
}

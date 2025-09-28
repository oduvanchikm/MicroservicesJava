package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.jooq.tools.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.JedisPooled;
import ru.mai.lessons.rpks.RedisClient;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
public class RedisClientImpl implements RedisClient {
    private final JedisPooled jedis;
    private final ObjectMapper mapper = new ObjectMapper();

    public RedisClientImpl(Config config) {
        this.jedis = new JedisPooled(
                config.getString("redis.host"), config.getInt("redis.port")
        );
    }

    @Override
    public boolean checkDuplicate(Message message, Rule[] rules) {
        try {
            JsonNode jsonNode = mapper.readTree(message.getValue());
            StringBuilder keyBuilder = new StringBuilder();

            long ttl = 0L;
            for (Rule rule : rules) {
                if (Boolean.TRUE.equals(rule.getIsActive())) {
                    JsonNode fieldNode = jsonNode.get(rule.getFieldName());
                    if (fieldNode != null) {
                        keyBuilder.append(fieldNode.asText()).append(":");
                        ttl = Math.max(ttl, rule.getTimeToLiveSec());
                    }
                }
            }

            String key = keyBuilder.toString();
            if (StringUtils.isBlank(key)) {
                log.info("True");
                return true;
            }

            if (jedis.exists(key)) {
                log.info("False");
                return false;
            } else {
                jedis.set(key, message.getValue());
                jedis.expire(key, ttl);
            }
            return true;
        } catch (Exception e) {
            log.warn("Invalid: {}", e.getMessage());
            return false;
        }
    }
}

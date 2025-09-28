package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.config.DBConfig;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Slf4j
public class DbReaderImpl implements DbReader {
    private final DSLContext dslContext;
    private final long updateIntervalSec;
    private ScheduledExecutorService scheduler;

    @Getter
    private Rule[] rules;

    public DbReaderImpl(Config config) {
        log.info("Start connecting to database");
        DBConfig dbConfig = new DBConfig(config);
        this.dslContext = dbConfig.getDslContext();
        this.updateIntervalSec = config.getLong("application.updateIntervalSec");
        this.rules = new Rule[0];
        log.info("Database connection done");
    }

    @Override
    public Rule[] readRulesFromDB() {
        log.info("Reading rules from database");
        try {
            List<Rule> ruleList = fetchRulesFromDatabase();
            if (ruleList.isEmpty()) {
                log.warn("Rules not found in the database");
                return new Rule[0];
            }

            rules = ruleList.toArray(new Rule[0]);
            log.info("Successfully read rules from database: {}", Arrays.toString(rules));
            return rules;

        } catch (Exception e) {
            log.error("An unexpected error: {}", e.getMessage(), e);
            return new Rule[0];
        }
    }

    private List<Rule> fetchRulesFromDatabase() {
        try {
            return dslContext.select(
                            field("enrichment_id", Long.class),
                            field("rule_id", Long.class),
                            field("field_name", String.class),
                            field("field_name_enrichment", String.class),
                            field("field_value", String.class),
                            field("field_value_default", String.class))
                    .from(table("enrichment_rules"))
                    .fetchInto(Rule.class);
        } catch (Exception e) {
            log.error("Error : {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    public void startPeriodicRuleUpdate() {
        log.info("Starting periodic rule update");
        scheduler = Executors.newScheduledThreadPool(1);

        rules = readRulesFromDB();

        Runnable updateRunnable = () -> {
            log.info("Updating rules from DB");
            Rule[] updatedRules = readRulesFromDB();
            if (updatedRules.length > 0) {
                rules = updatedRules;
                log.info("Updated rules successfully: {}", Arrays.toString(rules));
            } else {
                log.warn("Rules not found in db");
            }
        };

        schedulePeriodicUpdates(updateRunnable);
    }

    private void schedulePeriodicUpdates(Runnable updateRunnable) {
        scheduler.scheduleAtFixedRate(updateRunnable, 0, updateIntervalSec, TimeUnit.SECONDS);
    }

    public void stopPeriodicRuleUpdate() {
        log.info("Stopping periodic rule updates");
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    log.warn("Error");
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("error");
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}

package ru.mai.lessons.rpks.config;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import javax.sql.DataSource;

public class DbConfig {
    private final Config config;
    private DataSource dataSource;

    public DbConfig(Config config) {
        this.config = config;
    }

    private void initializeDataSource() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
        hikariConfig.setUsername(config.getString("db.user"));
        hikariConfig.setPassword(config.getString("db.password"));
        hikariConfig.setDriverClassName(config.getString("db.driver"));

        this.dataSource = new HikariDataSource(hikariConfig);
    }

    public DSLContext getDslContext() {
        if (dataSource == null) {
            initializeDataSource();
        }

        return DSL.using(dataSource, SQLDialect.POSTGRES);
    }
}

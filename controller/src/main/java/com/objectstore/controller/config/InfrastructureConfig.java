package com.objectstore.controller.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

@Configuration
public class InfrastructureConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * The metadata database: one SQLite file on the controller's volume.
     *
     * <p>SQLite allows a single writer at a time. Rather than relying on lock time-outs between
     * competing connections, the pool holds exactly one connection, so every statement and
     * transaction is serialised in-process. This is safe because no transaction ever spans a
     * network call: they all complete in well under a millisecond. {@code synchronous=FULL} makes
     * a commit durable before it is acknowledged, so a 201 survives a crash. Every connection is
     * checked by {@link com.objectstore.controller.cluster.SqliteGuard}: never on the writer thread,
     * never under the cluster lock.
     */
    @Bean(destroyMethod = "close")
    public GuardedDataSource dataSource(ControllerProperties properties) throws IOException {
        Path dbFile = properties.dbPath().toAbsolutePath().normalize();
        Files.createDirectories(dbFile.getParent());

        SQLiteConfig sqlite = new SQLiteConfig();
        sqlite.enforceForeignKeys(true);                          // required for ON DELETE CASCADE
        sqlite.setJournalMode(SQLiteConfig.JournalMode.WAL);
        sqlite.setSynchronous(SQLiteConfig.SynchronousMode.FULL); // a committed upload survives power loss
        sqlite.setBusyTimeout(5000);
        SQLiteDataSource sqliteDataSource = new SQLiteDataSource(sqlite);
        sqliteDataSource.setUrl("jdbc:sqlite:" + dbFile);

        HikariConfig pool = new HikariConfig();
        pool.setPoolName("metadata-db");
        pool.setDataSource(sqliteDataSource);
        pool.setMaximumPoolSize(1);
        pool.setMinimumIdle(1);
        pool.setConnectionTimeout(10_000);
        return new GuardedDataSource(new HikariDataSource(pool));
    }
}

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrencyTest {

    AtomicLong writeCount = new AtomicLong(0);

    @Test
    void concurrentReadWrite() throws Exception {
        HikariDataSource dataSource = createDataSource();

        int numShards = 3;
        int numRows = 10000000;
        int numTreads = 10;

        setupShards(dataSource, numShards, numRows);
        concurrentWrite(dataSource, numShards, numTreads, numRows);
        monitor();
        TimeUnit.SECONDS.sleep(10000);
    }

    void monitor() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    System.out.println("Write count: " + writeCount.get());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    void concurrentWrite(HikariDataSource dataSource,
                         int numShards,
                         int numThreads,
                         int numRows) {
        AtomicInteger atomicInteger = new AtomicInteger(0);
        var executorService = Executors.newFixedThreadPool(numThreads);
        Random random = new Random();
        for (int i = 0; i < numThreads; i++) {
            executorService.submit(() -> {
                while (true) {
                    try (Connection connection = dataSource.getConnection()) {
                        int shardId = random.nextInt(numShards) % numShards;
                        executeQuery(connection, "use shard" + shardId);
                        int rowId = getNext(atomicInteger, numRows);
                        executeQuery(connection, "update test set amount = amount + 1 where id = " + rowId);
                        connection.commit();
                        writeCount.incrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    int getNext(AtomicInteger integer, int max) {
        if (integer.get() >= max) {
            synchronized (integer) {
                if (integer.get() >= max) {
                    integer.set(0);
                }
                return integer.incrementAndGet();
            }
        }
        return integer.incrementAndGet();
    }

    HikariDataSource createDataSource() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:duckdb:test.db");
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setMaximumPoolSize(100);
        hikariConfig.setAutoCommit(false);
        return new HikariDataSource(hikariConfig);
    }

    void setupShards(HikariDataSource dataSource,
                     int numShards,
                     int numRows) throws Exception {
        Connection connection = dataSource.getConnection();
        for (int i = 0; i < numShards; i++) {
            executeQuery(connection, "attach database 'shard" + i + ".db' as shard" + i);
            executeQuery(connection, "use shard" + i);
            executeQuery(connection, "create or replace table test (id bigint primary key, amount int, description varchar)");
            executeQuery(connection, "insert into test SELECT range id, cast(random() * 100000 as bigint) as amount, repeat('x', 10) as description FROM range(" + numRows + ");");
            connection.commit();
        }
        connection.close();
    }

    void executeQuery(Connection connection, String query) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.execute();
        }
    }
}

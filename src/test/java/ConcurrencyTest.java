import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrencyTest {

    static final String URL = "jdbc:duckdb:test.db";
    AtomicLong writeCount = new AtomicLong(0);

    @Test
    void concurrentReadWrite() throws Exception {

        int numShards = 3;
        int numRows = 1000000;
        int numTreads = 10;

        setupShards(numShards, numRows);
        concurrentWrite(numShards, numTreads, numRows);
        monitor();
        TimeUnit.SECONDS.sleep(10000);
    }

    void monitor() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10000);
                    System.out.println("Write count: " + writeCount.get());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    void concurrentWrite(int numShards, int numThreads, int numRows) {
        AtomicInteger atomicInteger = new AtomicInteger(0);
        var executorService = Executors.newFixedThreadPool(numThreads);
        Random random = new Random();
        for (int i = 0; i < numThreads; i++) {
            executorService.submit(() -> {
                try (Connection conn = DriverManager.getConnection(URL)) {
                    conn.setAutoCommit(false);
                    synchronized (this) {
                        for (int j = 0; j < numShards; j++) {
                            executeQuery(conn, "attach if not exists 'shard" + j + ".db' as shard" + j);
                            conn.commit();
                        }
                    }
                    while (true) {
                        int shardId = random.nextInt(numShards) % numShards;
                        int rowId = getNext(atomicInteger, numRows);
                        executeQuery(conn, "update shard" + shardId + ".main.test set amount = amount + 1 where id = " + rowId);
                        conn.commit();
                        writeCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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

    void setupShards(int numShards, int numRows) throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            for (int i = 0; i < numShards; i++) {
                executeQuery(conn, "attach database 'shard" + i + ".db' as shard" + i);
                executeQuery(conn, "use shard" + i);
                executeQuery(conn, "create or replace table test (id bigint primary key, amount int, description varchar)");
                executeQuery(conn, "insert into test SELECT range id, cast(random() * 100000 as bigint) as amount, repeat('x', 10) as description FROM range(" + numRows + ");");
            }
        }
    }

    void executeQuery(Connection connection, String query) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.execute();
        }
    }
}

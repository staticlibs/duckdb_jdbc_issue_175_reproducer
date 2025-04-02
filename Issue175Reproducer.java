import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Issue175Reproducer {

    static final AtomicLong writeCount = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        int numShards = 3;
        int numRows = 100000;
        int numTreads = 2;

        TestDataSource dataSource = new TestDataSource(numTreads);
        setupShards(dataSource, numShards, numRows);
        concurrentWrite(dataSource, numShards, numTreads, numRows);
        monitor();
        TimeUnit.SECONDS.sleep(10000);
    }

    static void monitor() {
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

    static void concurrentWrite(TestDataSource dataSource,
                         int numShards,
                         int numThreads,
                         int numRows) {
        AtomicInteger atomicInteger = new AtomicInteger(0);
        var executorService = Executors.newFixedThreadPool(numThreads);
        Random random = new Random();
        for (int i = 0; i < numThreads; i++) {
            executorService.submit(() -> {
                while (true) {
                    Connection connection = dataSource.getConnection();
                    try  {
                        int shardId = random.nextInt(numShards) % numShards;
                        int rowId = getNext(atomicInteger, numRows);
                        executeQuery(connection, "update shard" + shardId + ".main.test set amount = amount + 1 where id = " + rowId);
                        connection.commit();
                        writeCount.incrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        dataSource.returnConnection(connection);
                    }
                }
            });
        }
    }

    static int getNext(AtomicInteger integer, int max) {
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

    static void setupShards(TestDataSource dataSource,
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
        dataSource.returnConnection(connection);
    }

    static void executeQuery(Connection connection, String query) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.execute();
        }
    }

    static class TestDataSource {
        final Deque<Connection> connections = new ConcurrentLinkedDeque<>();

        TestDataSource(int size) throws Exception {
            for (int i = 0; i < size; i++) {
                Connection conn = DriverManager.getConnection("jdbc:duckdb:test.db");
                conn.setAutoCommit(false);
                connections.push(conn);
            }
        }

        Connection getConnection() throws Exception {
            return connections.pop();
        }

        void returnConnection(Connection conn) {
            connections.push(conn);
        }

    }
}

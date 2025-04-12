import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Issue175Reproducer {

    static final AtomicLong writeCount = new AtomicLong(0);
    static final AtomicBoolean writeFailed = new AtomicBoolean(false);

    public static void main(String[] args) throws Exception {
        int numberOfCores = Runtime.getRuntime().availableProcessors();
        int numShards = 3;
        int numThreads = numberOfCores / 2;
        int numRows = 100000;

        System.out.println("CPU cores: " + numberOfCores);
        System.out.println("DB shards: " + numShards);
        System.out.println("Worker threads: " + numThreads);

        TestConnPool connPool = new TestConnPool("jdbc:duckdb:test.db", numThreads);
        setupShards(connPool, numShards, numRows);
        concurrentWrite(connPool, numShards, numThreads, numRows);
        while (!writeFailed.get()) {
            Thread.sleep(10000);
            System.out.println("Write count: " + writeCount.get());
        }
    }

    static void concurrentWrite(TestConnPool connPool,
                         int numShards,
                         int numThreads,
                         int numRows) {
        AtomicInteger atomicInteger = new AtomicInteger(0);
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        Lock incrementLock = new ReentrantLock();
        Random random = new Random();
        for (int i = 0; i < numThreads; i++) {
            executorService.submit(() -> {
                while (!writeFailed.get()) {
                    Connection connection = connPool.getConnection();
                    try  {
                        int shardId = random.nextInt(numShards);
                        int rowId = getNext(atomicInteger, numRows, incrementLock);
                        executeQuery(connection, "update shard" + shardId + ".main.test set amount = amount + 1 where id = " + rowId);
                        connection.commit();
                        writeCount.incrementAndGet();
                    } catch (Exception e) {
                        writeFailed.set(true);
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    } finally {
                        connPool.returnConnection(connection);
                    }
                }
            });
        }
    }

    static int getNext(AtomicInteger integer, int max, Lock lock) {
        int preInc = integer.incrementAndGet() - 1;
        if (preInc >= max) {
            lock.lock();
            try {
                if (integer.get() >= max) {
                    integer.set(1);
                }
                return 0;
            } finally {
                lock.unlock();
            }
        }
        return preInc;
    }

    static void setupShards(TestConnPool connPool,
                     int numShards,
                     int numRows) throws Exception {
        Connection connection = connPool.getConnection();
        for (int i = 0; i < numShards; i++) {
            executeQuery(connection, "attach database 'shard" + i + ".db' as shard" + i);
            executeQuery(connection, "use shard" + i);
            executeQuery(connection, "create or replace table test (id bigint primary key, amount int, description varchar)");
            executeQuery(connection, "insert into test SELECT range id, cast(random() * 100000 as bigint) as amount, repeat('x', 10) as description FROM range(" + numRows + ");");
            connection.commit();
        }
        connPool.returnConnection(connection);
    }

    static void executeQuery(Connection connection, String query) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.execute();
        }
    }

    static class TestConnPool {
        final Lock lock = new ReentrantLock();
        final List<Connection> connections = new ArrayList<>();
        final Random random = new Random();

        TestConnPool(String url, int size) throws Exception {
            Properties config = new Properties();
            config.put("threads", 2);
            for (int i = 0; i < size; i++) {
                Connection conn = DriverManager.getConnection(url, config);
                conn.setAutoCommit(false);
                connections.add(conn);
            }
        }

        public Connection getConnection() {
            lock.lock();
            try {
                int idx = random.nextInt(connections.size());
                return connections.remove(idx);
            } finally {
                lock.unlock();
            }
        }

        public void returnConnection(Connection conn) {
            lock.lock();
            try {
                connections.add(conn);
            } finally {
                lock.unlock();
            }
        }
    }
}

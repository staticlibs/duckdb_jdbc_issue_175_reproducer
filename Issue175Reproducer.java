import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Issue175Reproducer {

    public static void main(String[] args) throws Exception {
        int numCores = Runtime.getRuntime().availableProcessors();
        int numShards = 3;
        int numRows = 10_000_000;
        int numConnThreads = numCores;
        int numDbWorkerThreads = numCores;

        System.out.println("CPU cores: " + numCores);
        System.out.println("DB shards: " + numShards);
        System.out.println("Connection threads: " + numConnThreads);
        System.out.println("DB worker threads: " + numDbWorkerThreads);

        TestConnPool connPool = new HikariWrappedConnPool("test.db", numConnThreads, numDbWorkerThreads);
        setupShards(connPool, numShards, numRows);
        concurrentWrite(connPool, numShards, numConnThreads, numRows);
    }

    static void concurrentWrite(TestConnPool connPool, int numShards, int numConnThreads, int numRows)
        throws Exception {
        AtomicInteger atomicInteger = new AtomicInteger(0);
        AtomicLong writeCount = new AtomicLong(0);
        ExecutorService executorService = Executors.newFixedThreadPool(numConnThreads);
        Random random = new Random();

        System.out.println("Starting connection threads, count: " + numConnThreads);
        for (int i = 0; i < numConnThreads; i++) {
            executorService.submit(() -> {
                while (true) {
                    try {
                        Connection conn = connPool.takeConnection();
                        int shardId = random.nextInt(numShards);
                        int rowId = getNext(atomicInteger, numRows);
                        executeQuery(conn, "update shard" + shardId +
                                               ".main.test set amount = amount + 1 where id = " + rowId);
                        conn.commit();
                        writeCount.incrementAndGet();
                        connPool.returnConnection(conn);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            });
        }

        while (true) {
            Thread.sleep(10000);
            System.out.println("Write count: " + writeCount.get());
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

    static void setupShards(TestConnPool connPool, int numShards, int numRows) throws Exception {
        Connection connection = connPool.takeConnection();
        for (int i = 0; i < numShards; i++) {
            System.out.println("Generating data for shard number: " + i + ", rows count: " + numRows);
            executeQuery(connection, "attach database 'shard" + i + ".db' as shard" + i);
            executeQuery(connection, "use shard" + i);
            executeQuery(connection,
                         "create or replace table test (id bigint primary key, amount int, description varchar)");
            executeQuery(
                connection,
                "insert into test SELECT range id, cast(random() * 100000 as bigint) as amount, repeat('x', 10) as description FROM range(" +
                    numRows + ");");
            connection.commit();
        }
        connPool.returnConnection(connection);
    }

    static void executeQuery(Connection connection, String query) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.execute();
        }
    }

    interface TestConnPool {
        Connection takeConnection() throws Exception;

        void returnConnection(Connection conn) throws Exception;
    }

    static class RandomArrayConnPool implements TestConnPool {
        final List<Connection> connections = new ArrayList<>();
        final Random random = new Random();

        RandomArrayConnPool(String dbPath, int numConnThreads, int numDbWorkerThreads) throws Exception {
            Properties config = new Properties();
            config.put("threads", numDbWorkerThreads);
            for (int i = 0; i < numConnThreads; i++) {
                Connection conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath, config);
                conn.setAutoCommit(false);
                connections.add(conn);
            }
        }

        public Connection takeConnection() {
            synchronized (this) {
                int idx = random.nextInt(connections.size());
                return connections.remove(idx);
            }
        }

        public void returnConnection(Connection conn) {
            synchronized (this) {
                connections.add(conn);
            }
        }
    }

    static class HikariWrappedConnPool implements TestConnPool {
        final HikariDataSource hikariDataSource;

        HikariWrappedConnPool(String dbPath, int numConnThreads, int numDbWorkerThreads) throws Exception {
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl("jdbc:duckdb:" + dbPath);
            hikariConfig.setMaximumPoolSize(numConnThreads * 4);
            hikariConfig.setAutoCommit(false);
            Properties dsConfig = new Properties();
            dsConfig.put("threads", numDbWorkerThreads);
            hikariConfig.setDataSourceProperties(dsConfig);
            this.hikariDataSource = new HikariDataSource(hikariConfig);
        }

        @Override
        public Connection takeConnection() throws Exception {
            return hikariDataSource.getConnection();
        }

        @Override
        public void returnConnection(Connection conn) throws Exception {
            conn.close();
        }
    }
}

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Issue175Reproducer {

    static class StickingSpinLock {
        private final AtomicBoolean lock = new AtomicBoolean(false);

        void lock() {
            while (!lock.compareAndSet(false, true)) {
            }
        }

        void unlock() {
            lock.set(false);
        }
    }

    static class YieldingSpinLock {
        private final AtomicBoolean lock = new AtomicBoolean(false);

        void lock() {
            while (!lock.compareAndSet(false, true)) {
                Thread.yield();
            }
        }

        void unlock() {
            lock.set(false);
        }
    }

    interface TestConnPool {
        Connection takeConnection() throws Exception;

        void returnConnection(Connection conn) throws Exception;
    }

    static class HikariConnPool implements TestConnPool {
        final HikariDataSource hikariDataSource;

        HikariConnPool(String dbPath, int numConnThreads, int numDbWorkerThreads) throws Exception {
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl("jdbc:duckdb:" + dbPath);
            hikariConfig.setMaximumPoolSize(numConnThreads * 10);
            Properties driverConfig = new Properties();
            driverConfig.put("threads", numDbWorkerThreads);
            hikariConfig.setDataSourceProperties(driverConfig);
            hikariConfig.setAutoCommit(false);
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

    static void executeQuery(Connection connection, String query) throws Exception {
        PreparedStatement statement = connection.prepareStatement(query);
        statement.execute();
        statement.close();
    }

    static int getNextRowId(AtomicInteger integer, int max) {
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
        Connection conn = connPool.takeConnection();
        for (int i = 0; i < numShards; i++) {
            System.out.println("Generating data for shard number: " + i + ", rows count: " + numRows);
            executeQuery(conn, "attach database 'shard" + i + ".db' as shard" + i);
            executeQuery(conn, "use shard" + i);
            executeQuery(conn, "create or replace table test (id bigint primary key, amount int, description varchar)");
            executeQuery(conn, "insert into test" +
                    " SELECT range as id, cast(random() * 100000 as bigint) as amount, repeat('x', 10) as description" +
                    " FROM range(" + numRows + ");");
            conn.commit();
        }
        connPool.returnConnection(conn);
    }

    static void concurrentWrite(TestConnPool connPool, int numShards, int numConnThreads, int numRows) throws Exception {
        AtomicLong writeCount = new AtomicLong(0);
        AtomicInteger atomicInteger = new AtomicInteger(0);
        Random random = new Random();

        System.out.println("Starting connection threads, count: " + numConnThreads);
        for (int i = 0; i < numConnThreads; i++) {
            Thread th = new Thread(() -> {
                while (true) {
                    try  {
                        Connection connection = connPool.takeConnection();

                        int shardId = random.nextInt(numShards) % numShards;
                        int rowId = getNextRowId(atomicInteger, numRows);

                        executeQuery(connection, "update shard" + shardId + ".main.test set amount = amount + 1 where id = " + rowId);
                        connection.commit();
                        writeCount.incrementAndGet();

                        connPool.returnConnection(connection);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            });
            th.start();
        }

        while (true) {
            Thread.sleep(10000);
            System.out.println("Write count: " + writeCount.get());
        }
    }

    public static void main(String[] args) throws Exception {
        int numCores = Runtime.getRuntime().availableProcessors();
        int numShards = 3;
        int numConnThreads = 10;
        int numDbWorkerThreads = numCores;
        int numRows = 10_000_000;

        System.out.println("CPU cores: " + numCores);
        System.out.println("DB shards: " + numShards);
        System.out.println("Connection threads: " + numConnThreads);
        System.out.println("DB worker threads: " + numDbWorkerThreads);

        TestConnPool connPool = new HikariConnPool("test.db", numConnThreads, numDbWorkerThreads);
        setupShards(connPool, numShards, numRows);
        concurrentWrite(connPool, numShards, numConnThreads, numRows);
    }
}

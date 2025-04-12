import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Issue175Reproducer {

    static class SpinLock {
        private final AtomicBoolean lock = new AtomicBoolean(false);

        void lock() {
            while (!lock.compareAndSet(false, true)) {
                // Thread.yield();
            }
        }

        void unlock() {
            lock.set(false);
        }
    }

    static class TestConnPool {
        SpinLock lock = new SpinLock();
        final List<Connection> connections = new ArrayList<>();
        final Random random = new Random();

        TestConnPool(String dbPath, int numConnThreads, int numDbWorkerThreads) throws Exception {
            Properties config = new Properties();
            config.put("threads", numDbWorkerThreads);
            for (int i = 0; i < numConnThreads; i++) {
                Connection conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath, config);
                conn.setAutoCommit(false);
                connections.add(conn);
            }
        }

        Connection getConnection() {
            lock.lock();
            int idx = random.nextInt(connections.size());
            Connection conn = connections.remove(idx);
            lock.unlock();
            return conn;
        }

        void returnConnection(Connection conn) {
            lock.lock();
            connections.add(conn);
            lock.unlock();
        }
    }

    static void executeQuery(Connection connection, String query) throws Exception {
        PreparedStatement statement = connection.prepareStatement(query);
        statement.execute();
        statement.close();
    }

    static int getNextRowId(SpinLock lock, AtomicInteger atomicRowId, int max) {
        int preInc = atomicRowId.incrementAndGet() - 1;
        if (preInc >= max) {
            lock.lock();
            if (atomicRowId.get() >= max) {
                atomicRowId.set(0);
            }
            int res = atomicRowId.incrementAndGet();
            lock.unlock();
            return res;
        }
        return preInc;
    }

    static void concurrentWrite(TestConnPool connPool, int numShards, int numConnThreads, int numRows) throws Exception {
        AtomicInteger atomicRowId = new AtomicInteger(0);
        SpinLock incrementLock = new SpinLock();
        Random random = new Random();
        AtomicLong writeCount = new AtomicLong(0);

        System.out.println("Starting connection threads, count: " + numConnThreads);
        for (int i = 0; i < numConnThreads; i++) {
            Thread th = new Thread(() -> {
                while (true) {
                    Connection connection = connPool.getConnection();
                    try  {
                        int shardId = random.nextInt(numShards);
                        int rowId = getNextRowId(incrementLock, atomicRowId, numRows);
                        executeQuery(connection, "update shard" + shardId + ".main.test set amount = amount + 1 where id = " + rowId);
                        connection.commit();
                        writeCount.incrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    } finally {
                        connPool.returnConnection(connection);
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

    static void setupShards(TestConnPool connPool, int numShards, int numRows) throws Exception {
        Connection connection = connPool.getConnection();
        for (int i = 0; i < numShards; i++) {
            System.out.println("Generating data for shard number: " + i + ", rows count: " + numRows);
            executeQuery(connection, "attach database 'shard" + i + ".db' as shard" + i);
            executeQuery(connection, "use shard" + i);
            executeQuery(connection, "create or replace table test (id bigint primary key, amount int, description varchar)");
            executeQuery(connection, "insert into test" +
                    " SELECT range as id, cast(random() * " + numRows + " as bigint) as amount, repeat('x', 10) as description" +
                    " FROM range(" + numRows + ");");
            connection.commit();
        }
        connPool.returnConnection(connection);
    }

    public static void main(String[] args) throws Exception {
        int numberOfCores = Runtime.getRuntime().availableProcessors();
        int numShards = 3;
        int numConnThreads = numberOfCores;
        int numDbWorkerThreads = numberOfCores;
        int numRows = 1_000_000;

        System.out.println("CPU cores: " + numberOfCores);
        System.out.println("DB shards: " + numShards);
        System.out.println("Connection threads: " + numConnThreads);
        System.out.println("DB worker threads: " + numDbWorkerThreads);

        TestConnPool connPool = new TestConnPool("test.db", numConnThreads, numDbWorkerThreads);
        setupShards(connPool, numShards, numRows);
        concurrentWrite(connPool, numShards, numConnThreads, numRows);
    }
}

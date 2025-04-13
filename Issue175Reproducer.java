import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Issue175Reproducer {

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

    static List<Connection> createConnPool(String dbPath, int numConnThreads, int numDbWorkerThreads) throws Exception {
        Properties config = new Properties();
        config.put("threads", numDbWorkerThreads);
        List<Connection> connections = new ArrayList<>();
        for (int i = 0; i < numConnThreads; i++) {
            Connection conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath, config);
            conn.setAutoCommit(false);
            connections.add(conn);
        }
        return connections;
    }

    static void executeQuery(Connection connection, String query) throws Exception {
        PreparedStatement statement = connection.prepareStatement(query);
        statement.execute();
        statement.close();
    }

    static void setupShards(List<Connection> connPool, int numShards, int numRows) throws Exception {
        Connection connection = connPool.get(0);
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
    }

    static void concurrentWrite(List<Connection> connPool, int numShards, int numConnThreads, int numRows) throws Exception {
        AtomicLong writeCount = new AtomicLong(0);
        Lock fairLock = new ReentrantLock(true);

        System.out.println("Starting connection threads, count: " + numConnThreads);
        for (int i = 0; i < numConnThreads; i++) {
            Thread th = new Thread(() -> {
                Random random = new Random();
                while (true) {
                    try  {

                        fairLock.lock();
                        int connIdx = random.nextInt(connPool.size());
                        Connection connection = connPool.remove(connIdx);
                        fairLock.unlock();

                        int shardId = random.nextInt(numShards);
                        long preInc = writeCount.incrementAndGet() - 1;
                        long rowId = preInc - 1 % numRows;

                        executeQuery(connection, "update shard" + shardId + ".main.test set amount = amount + 1 where id = " + rowId);
                        connection.commit();

                        fairLock.lock();
                        connPool.add(connection);
                        fairLock.unlock();

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
        int numConnThreads = numCores;
        int numDbWorkerThreads = numCores;
        int numRows = 1_000_000;

        System.out.println("CPU cores: " + numCores);
        System.out.println("DB shards: " + numShards);
        System.out.println("Connection threads: " + numConnThreads);
        System.out.println("DB worker threads: " + numDbWorkerThreads);

        List<Connection> connPool = createConnPool("test.db", numConnThreads, numDbWorkerThreads);
        setupShards(connPool, numShards, numRows);
        concurrentWrite(connPool, numShards, numConnThreads, numRows);
    }
}

Reproducer to DuckDB crash
--------------------------

Run the following to reproduce [#175](https://github.com/duckdb/duckdb-java/issues/175) crash:

```
git clone https://github.com/duckdb/duckdb-java.git
cd duckdb-java
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
GEN=ninja make release
$JAVA_HOME/bin/java -cp path/to/duckdb-java/build/release/duckdb_jdbc.jar:/home/<USERNAME>/.m2/repository/com/zaxxer/HikariCP/6.2.1/HikariCP-6.2.1.jar:/home/<USERNAME>/.m2/repository/org/slf4j/slf4j-api/2.0.6/slf4j-api-2.0.6.jar Issue175Reproducer.java
```

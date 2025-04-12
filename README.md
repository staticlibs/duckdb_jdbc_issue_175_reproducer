Reproducer to DuckDB crash
--------------------------

Run the following to reproduce [#175](https://github.com/duckdb/duckdb-java/issues/175) crash:

```
git clone https://github.com/duckdb/duckdb-java.git
cd duckdb-java
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
GEN=ninja make release
$JAVA_HOME/bin/java -cp path/to/duckdb-java/build/release/duckdb_jdbc.jar Issue175Reproducer.java
```

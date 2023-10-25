**Parallel duckdb runner and statistic collector for ocs baseline performance evaluation**

DuckDB Runner
================

```
XX              XXXXX XXX         XX XX           XX       XX XX XXX         XXX
XX             XXX XX XXXX        XX XX           XX       XX XX    XX     XX   XX
XX            XX   XX XX XX       XX XX           XX       XX XX      XX XX       XX
XX           XX    XX XX  XX      XX XX           XX       XX XX      XX XX       XX
XX          XX     XX XX   XX     XX XX           XX XXXXX XX XX      XX XX       XX
XX         XX      XX XX    XX    XX XX           XX       XX XX     XX  XX
XX        XX       XX XX     XX   XX XX           XX       XX XX    XX   XX
XX       XX XX XX XXX XX      XX  XX XX           XX XXXXX XX XX XXX     XX       XX
XX      XX         XX XX       XX XX XX           XX       XX XX         XX       XX
XX     XX          XX XX        X XX XX           XX       XX XX         XX       XX
XX    XX           XX XX          XX XX           XX       XX XX          XX     XX
XXXX XX            XX XX          XX XXXXXXXXXX   XX       XX XX            XXXXXX
```

# Step 1: Compile DuckDB and Install It

In general, it is easier to compile duckdb from source rather than installing a prebuilt version of it. This is because that the prebuilt version shipped by duckdb does not include the httpfs extension needed for reading data from s3. While duckdb supports dynamic extension installation and load, it is in general easier if duckdb is statically built and linked with that extension already so that we don't need to dynamically install and load it, which will require internet connection and increase query latency.

Building duckdb from source requires a g++ compiler, make, cmake, git, and openssl.

```bash
sudo apt-get install gcc g++ make cmake git libssl-dev
git clone -b v0.9.1 https://github.com/duckdb/duckdb.git
cd duckdb
mkdir bu
cd bu
cmake -DCMAKE_BUILD_TYPE=Release \
-DBUILD_TESTING=OFF \
-DBUILD_UNITTESTS=OFF \
-DBUILD_EXTENSIONS='parquet;httpfs' \
-DEXTENSION_STATIC_BUILD=ON \
.. 
make -j
sudo make install
```

# Step 2: Compile DuckDB Runner

```bash
git clone https://github.com/lanl-ocs/ocs-duckdb-runner.git
cd ocs-duckdb-runner
mkdir bu
cd bu
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
```

# Step 3: Run the Runner

The idea is that the runner will read input parquet file object addresses from stdin (**one address per line**) and launch a duckdb instance per parquet file using a thread pool. Detailed usage information below. We can configure s3 endpoint and the number of threads in the thread pool. Default thread count is 4.

```bash
$./duckdb-runner -h
==============
usage: ./duckdb-runner [options]

-i      id           :  s3 access key id
-k      key          :  s3 access secret key
-a      address      :  s3 web address
-p      port         :  s3 port
-j      threads      :  num query thread
==============
```

A test run where we have a minio instance running at 127.0.0.1:9000, a precreated bucket `ocs`, and a preinserted parquet file object `xx_036785.parquet`. See https://github.com/lanl-ocs/laghos-sample-dataset for data scheme and query information.

```bash
$./duckdb-runner -a 127.0.0.1 -p 9000 -j 4 <<EOF
's3://ocs/xx_036785.parquet'
EOF

Chunk - [5 Columns]
- FLAT INTEGER: 8 = [ 166462, 240662, 219780, 240660, 159440, 233498, 212414, 212409]
- FLAT DOUBLE: 8 = [ 1.5645773, 1.5611519000000003, 1.5264729999999997, 1.503907, 1.5057572, 1.5047735, 1.5057572, 1.5047735]
- FLAT DOUBLE: 8 = [ 1.5376104999999998, 1.5749507000000003, 1.5887712, 1.5975798999999997, 1.5352168, 1.5526603, 1.5352168, 1.5526603]
- FLAT DOUBLE: 8 = [ 1.5803627999999998, 1.5417632999999997, 1.5934969, 1.5078072999999999, 1.556996, 1.5391836, 1.556996, 1.5391836]
- FLAT DOUBLE: 8 = [ 1.4755714125000001, 1.4774999125, 1.4871300875, 1.4890849375, 1.49520745, 1.49543775, 1.495636475, 1.4960388500000001]

Number data sources (parquet files): 1
Threads: 4
Total Query time: 0.33 s
Total hits: 8
Total duckdb read ops: 5
Total duckdb read bytes: 46383122
Done
```

Use `./duckdb-runner 1>/dev/null` to discard query results.

# Notes

It is currently assumed that the s3 http server uses http not https. In future, this can be made configurable. DuckDB will try to cache the metadata information of each s3 object to reduce the number of HEAD requests it must issue. This cache can be disabled. It is currently not disabled because each our query will target a different parquet object anyway, making this cache effectively useless.

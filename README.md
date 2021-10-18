# milvus-preformance-benchmark

Benchmark to test the performance of Milvus.

## Steps to run benchmark

### 1.Start Milvus Standalone Server

### 2. Run benchmark

```shell
python benchmark.py > benchmark.txt
```

### 3. Collect the log

```shell
cat /tmp/standalone.log | grep benchmark- > standalone_time.txt
cat benchmark.txt| grep cost > benchmark_time.txt
```

### 4. Parse the log

```shell
python python parse_log.py --log standalone_time.txt
```

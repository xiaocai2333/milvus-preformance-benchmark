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
```

### 4. Parse the log

```shell
python parse_log.py --log standalone_time.txt --sdk benchmark.txt
```


## Test with graceful time

### 1. 启动 Milvus 服务

### 2. 运行 python 脚本

bench-2.py 是测试 Proxy interval = 100ms 的情况，同时要调整 rootcoord 的 interval， 默认是 100ms。
bench-3.py 是测试 Proxy interval = 500ms 的情况。
Proxy 的 time tick interval 是通过接口 set_time_tick_interval 设置的，脚本里有例子，有可能被注释了，看情况使用。

### 3.注意

插入的数据是要从文件中读取还是自动生成，文件读取用 insert_data_from_file 方法，注意设置 pod 对应的目录。
如果自动生成用 insert_parallel， （实际上并不是并发插入）
parse_log3.py 解析bench-2.py 和 bench-3.py 的运行结果


## 运行解析脚本

### 运行解析脚本

```shell
python parse_log3.py --file sdk.log --output text.csv
```
sdk.log 文件代表测试是sdk打印的log， text.csv 表示放统计数据的文件（最好是csv）
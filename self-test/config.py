# TopK = [1, 10, 50, 100, 1000]
TopK = [1]

NQ = [1, 10, 100, 200, 500, 1000, 10000]
# NQ = [100]
# NQ = [1, 5, 10, 20, 100]

# Nprobe = [8, 16, 32, 64, 128, 256, 512]
Nprobe = [16]

# EF = [50, 64, 80, 100, 128, 168, 200, 256]
EF = [150]


NumberOfTestRun = 10
QueryNodeNum = 1

dim = 768
nb = 200000
batch = 50000
thread_nums = 10
vectors_per_file = 2500000

collection_name = "bench_1"
field_name = "field"

default_IVF_FLAT = {"index_type": "IVF_FLAT", "params": {"nlist": 4096}, "metric_type": "L2"}
default_HNSW = {'index_type': "HNSW", 'metric_type': 'L2',
                 'params': {
                     "M": 12,  # int. 4~64
                     "efConstruction": 150  # int. 8~512
                    }
                 }

index_type = "HNSW"
# index_type = "IVF_FLAT"

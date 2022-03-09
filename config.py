# TopK = [1, 10, 50, 100, 1000]
TopK = [1]

NQ = [1, 10, 100, 200, 500, 1000, 1200]
# NQ = [20, 30, 40, 50, 60, 70, 80, 90]
# NQ = [1]

# Nprobe = [8, 16, 32, 64, 128, 256, 512]
Nprobe = [16]


NumberOfTestRun = 100

dim = 768
nb = 5000000
batch = 50000
thread_nums = 10
vectors_per_file = 100000

collection_name = "bench_1"
field_name = "field"

import argparse
import random
import threading
import time
import gc
import numpy as np
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType
# connections.add_connection(default={"host": "10.96.77.48", "port": "19530"})
connections.connect("default")

# TopK = [1, 10, 50, 100, 1000]
TopK = [1]
# NQ = [1, 10, 100, 200, 500, 1000, 1200]
NQ = [1]
# Nprobe = [8, 16, 32, 64, 128, 256, 512]
Nprobe = [128]

time_tick_interval = 500
graceful_times = [0, 50, 100, 300, 500, 1000, 5000]


def time_costing(func):
    def core(*args):
        start = time.time()
        print(func.__name__, "start time: ", start)
        res = func(*args)
        end = time.time()
        print(func.__name__, "end time: ", end)
        print(func.__name__, "time cost: ", end-start)
        return res
    return core


def create_collection(collection_name, field_name, dim, partition=None, auto_id=True):
    pk = FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True, auto_id=auto_id)
    field = FieldSchema(name=field_name, dtype=DataType.FLOAT_VECTOR, dim=dim)
    schema = CollectionSchema(fields=[pk, field], description="example collection")

    collection = Collection(name=collection_name, schema=schema)
    return collection


@time_costing
def create_index(collection, field_name):
    default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 1024}, "metric_type": "L2"}
    collection.create_index(field_name, default_index)
    # print("Successfully build index")
    # print(pymilvus.utility.index_building_progress(collection.name))

    # collection.drop_index()
    # print("Successfully drop index")


@time_costing
def search(collection, query_entities, field_name, topK, nprobe, guarantee_timestamp=0):
    search_params = {"metric_type": "L2", "params": {"nprobe": nprobe}}
    res = collection.search(query_entities, field_name, search_params, limit=topK,
                            guarantee_timestamp=guarantee_timestamp)


@time_costing
def insert(collection, entities, partition):
    mr = collection.insert([entities], partition_name=partition)
    print(mr)


def gen_data_and_insert(collection, nb, batch, dim, partition):
    for i in range(int(nb/batch)):
        entities = generate_entities(dim, nb)
        insert(collection, entities, partition)
        gc.collect()


def insert_parallel(collection, partition, nb, dim, batch, speed):
    for i in range(int(nb/batch)):
        global stop_insert
        if stop_insert:
            return
        entities = generate_entities(dim, batch)
        insert_start = time.time()
        insert(collection, entities, partition)
        insert_end = time.time()
        if speed < (insert_end-insert_start):
            raise Exception("Speed if too small")
        time.sleep(speed-(insert_end-insert_start))
        gc.collect()


def generate_entities(dim, nb) -> list:
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    return vectors


def insert_data_from_file(coll, nb, dim,  vectors_per_file, batch_size):
    # logger.info("Load npy file: %s end" % file_name)
    for j in range(nb // vectors_per_file):
        s = "%05d" % j
        fname = "binary_" + str(dim) + "d_" + s + ".npy"
        data = np.load(fname)
        vectors = data.tolist()
        insert(coll, vectors)


def graceful_time_search(coll, field_name, graceful_time):
    print("time tick interval = ", time_tick_interval, "graceful time = ",
          graceful_time, "start time = ", time.time())
    for i in range(110):
        if i == 10:
            print("time tick interval = ", time_tick_interval, "graceful time = ", graceful_time, "start time = ",
                  time.time())
        for topK in TopK:
            for nq in NQ:
                for nprobe in Nprobe:
                    query_entities = generate_entities(dim, nq)
                    search(coll, query_entities, field_name, topK, nprobe, 0)
    print("time tick interval = ", time_tick_interval, "graceful time = ", graceful_time, "end time = ",
          time.time())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="your script description")  # description参数可以用于插入描述脚本用途的信息，可以为空
    parser.add_argument('--speed', '-s', nargs=1, type=float, help='insert speed, :s')
    parser.add_argument('--num', '-n', nargs=1, type=int, help='insert total num')
    parser.add_argument('--batch', '-b', nargs=1, type=int, help='insert batch num')
    args = parser.parse_args()

    batch = args.batch[0]
    speed = args.speed[0]
    nb = args.num[0]

    collection_name = "bench_1"
    field_name = "field"
    dim = 128
    nbs = [100000, 200000, 400000, 600000, 800000, 1000000]
    thread_nums = 10
    vectors_per_file = 100000

    # coll.set_timetick_interval(time_tick_interval)
    # for nb in nbs:
    #     print("nb = ", nb)
    for graceful_time in graceful_times:
        stop_insert = False
        coll = create_collection(collection_name, field_name, dim)
        partition_name = "cat"
        coll.create_partition(partition_name)
        insert_parallel(coll, None, 1000, dim, 1000, speed)
        coll.set_graceful_time(graceful_time)
        time.sleep(10)
        coll.load()

        threads = []
        t1 = threading.Thread(target=insert_parallel, args=(coll, partition_name, nb, dim, batch, speed))
        threads.append(t1)

        t2 = threading.Thread(target=graceful_time_search, args=(coll, field_name, graceful_time))
        threads.append(t2)

        for t in threads:
            t.start()

        t2.join()
        stop_insert = True

        coll.release()
        coll.drop()

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

time_tick_interval = 100
graceful_times = [0, 10, 20, 50, 100, 200, 500, 1000]


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
def insert(collection, entities):
    mr = collection.insert([entities])
    print(mr)


def gen_data_and_insert(collection, nb, batch, dim):
    for i in range(int(nb/batch)):
        entities = generate_entities(dim, nb)
        insert(collection, entities)
        gc.collect()


def insert_parallel(collection, nb, dim, batch, thread_num=1):
    # threads = []
    # for i in range(thread_num):
    #     x = threading.Thread(target=gen_data_and_insert, args=(collection, int(nb/thread_num), batch, dim))
    #     threads.append(x)
    #     x.start()
    # for t in threads:
    #     t.join()
    for i in range(int(nb/batch)):
        entities = generate_entities(dim, batch)
        insert(collection, entities)
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


if __name__ == "__main__":
    collection_name = "bench_1"
    field_name = "field"
    dim = 128
    nb = 1000000
    batch = 50000
    thread_nums = 10
    vectors_per_file = 100000

    coll = create_collection(collection_name, field_name, dim)

    coll.set_timetick_interval(time_tick_interval)
    time.sleep(10)

    insert_parallel(coll, nb, dim, batch, thread_nums)
    # insert_data_from_file(coll, nb, dim, vectors_per_file, batch)
    create_index(coll, field_name)
    coll.load()
    print("time tick interval = ", time_tick_interval, "guarantee_timestamp = ", 1, "start time = ", time.time())
    for topK in TopK:
        for nq in NQ:
            for nprobe in Nprobe:
                print("nprobe = ", nprobe, "topK = ", topK, "nq = ", nq)
                for _ in range(50):
                    query_entities = generate_entities(dim, nq)
                    search(coll, query_entities, field_name, topK, nprobe, 1)
    print("time tick interval = ", time_tick_interval, "guarantee_timestamp = ", 1, "end time = ", time.time())

    for graceful_time in graceful_times:
        coll.set_graceful_time(graceful_time)
        time.sleep(10)
        # print("time tick interval = ", time_tick_interval,  "graceful time = ",
        #       graceful_time, "start time = ", time.time())
        for i in range(110):
            if i == 10:
                print("time tick interval = ", time_tick_interval, "graceful time = ", graceful_time, "start time = ",
                      time.time())
            for topK in TopK:
                for nq in NQ:
                    for nprobe in Nprobe:
                        query_entities = generate_entities(dim, nq)
                        search(coll, query_entities, field_name, topK, nprobe, 0)
                        time.sleep(random.uniform(0, 0.1))
        print("time tick interval = ", time_tick_interval, "graceful time = ", graceful_time, "end time = ",
              time.time())

    coll.release()

    # coll.drop_index()
    coll.drop()

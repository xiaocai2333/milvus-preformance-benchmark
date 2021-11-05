import random
import threading
import time
import gc
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType
# connections.add_connection(default={"host": "10.96.77.48", "port": "19530"})
connections.connect("default")

# TopK = [1, 10, 50, 100, 1000]
TopK = [10]
# NQ = [1, 10, 100, 200, 500, 1000, 1200]
NQ = [100, 1000]
# Nprobe = [8, 16, 32, 64, 128, 256, 512]
Nprobe = [1, 128, 256]


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
    default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 4096}, "metric_type": "L2"}
    collection.create_index(field_name, default_index)
    # print("Successfully build index")
    # print(pymilvus.utility.index_building_progress(collection.name))

    # collection.drop_index()
    # print("Successfully drop index")


@time_costing
def search(collection, query_entities, field_name, topK, nprobe):
    search_params = {"metric_type": "L2", "params": {"nprobe": nprobe}}
    res = collection.search(query_entities, field_name, search_params, limit=topK, guarantee_timestamp=1)


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


if __name__ == "__main__":
    collection_name = "bench_1"
    field_name = "field"
    dim = 128
    nb = 10000000
    batch = 50000
    thread_nums = 10

    coll = create_collection(collection_name, field_name, dim)

    insert_parallel(coll, nb, dim, batch, thread_nums)
    create_index(coll, field_name)
    coll.load()

    for topK in TopK:
        for nq in NQ:
            for nprobe in Nprobe:
                print("nprobe = ", nprobe, "topK = ", topK, "nq = ", nq)
                for _ in range(5):
                    query_entities = generate_entities(dim, nq)
                    search(coll, query_entities, field_name, topK, nprobe)

    coll.release()

    coll.drop_index()
    coll.drop()

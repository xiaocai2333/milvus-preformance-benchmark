import random
import threading
import time
import gc
import numpy as np
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType
from config import TopK, NQ, Nprobe, NumberOfTestRun, collection_name, field_name, dim, nb, batch, thread_nums

# connections.add_connection(default={"host": "10.96.77.48", "port": "19530"})
connections.connect("default")


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


def insert_data_from_file(coll, nb, dim,  vectors_per_file, batch_size):
    # logger.info("Load npy file: %s end" % file_name)
    for j in range(nb // vectors_per_file):
        s = "%05d" % j
        fname = "binary_" + str(dim) + "d_" + s + ".npy"
        data = np.load(fname)
        vectors = data.tolist()
        insert(coll, vectors)


if __name__ == "__main__":
    coll = create_collection(collection_name, field_name, dim)

    create_index(coll, field_name)
    insert_parallel(coll, nb, dim, batch, thread_nums)
    # insert_data_from_file(coll, nb, dim, vectors_per_file, batch)
    create_index(coll, field_name)
    coll.load()

    query_entities = generate_entities(dim, NQ[0])
    for _ in range(NumberOfTestRun):
        search(coll, query_entities, field_name, TopK[0], Nprobe[0])

    for topK in TopK:
        for nq in NQ:
            for nprobe in Nprobe:
                print("nprobe = ", nprobe, "topK = ", topK, "nq = ", nq)
                start = time.time()
                for _ in range(NumberOfTestRun):
                    query_entities = generate_entities(dim, nq)
                    search(coll, query_entities, field_name, topK, nprobe)

                end = time.time()
                print("nprobe = ", nprobe, "topK = ", topK, "nq = ", nq, "test times = ", NumberOfTestRun, "total time = ",
                      end - start, "avg time = ", (end-start)/NumberOfTestRun)

    coll.drop()

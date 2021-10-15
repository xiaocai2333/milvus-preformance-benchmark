import random
import time
import pymilvus
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType
connections.connect()


def time_costing(func):
    def core(*args):
        start = time.time()
        print("start time: ", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        print("start time: ", start)
        func(*args)
        end = time.time()
        print("end time: ", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        print("end time: ", end)
        print(func.__name__, "time cost: ", end-start)
    return core


def create_collection(collection_name, field_name, dim, partition=None, auto_id=True):
    pk = FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True, auto_id=auto_id)
    field = FieldSchema(name=field_name, dtype=DataType.FLOAT_VECTOR, dim=dim)
    schema = CollectionSchema(fields=[pk, field], description="example collection")

    collection = Collection(name=collection_name, schema=schema)
    return collection


@time_costing
def create_index(collection, field_name):
    default_index = {"index_type": "IVF_SQ8", "params": {"nlist": 64}, "metric_type": "L2"}
    #default_index2 = {'index_type': 'IVF_PQ', 'params': {'nlist': 128, 'm': 16, 'nbits': 8}, 'metric_type': 'L2'}
    # default_index3 = {'index_type': 'HNSW', 'params': {'M': 16, 'efConstruction': 500}, 'metric_type': 'L2'}
    #default_index = {'index_type': 'IVF_FLAT', 'params': {'nlist': 1024}, 'metric_type': 'L2'}
    collection.create_index(field_name, default_index)
    print("Successfully build index")
    print(pymilvus.utility.index_building_progress(collection.name))

    # collection.drop_index()
    # print("Successfully drop index")


@time_costing
def search(collection, dim, nb, field_name):
    nq = 5
    vectors1 = [[random.random() for _ in range(dim)] for _ in range(nq)]
    search_params = {"metric_type": "L2", "params": {"nprobe": 16}}
    res = collection.search(vectors1[:nq], field_name, search_params, limit=5)
    print(res)
    print(len(res))
    for i in range(len(res)):
        print(res[i])
    print(res[0].ids)


@time_costing
def insert(collection, dim, nb):
    for i in range(1):
        # pk = [i for i in range(nb)]
        vectors1 = [[random.random() for _ in range(dim)] for _ in range(nb)]
        entities1 = [vectors1]
        mr = collection.insert(entities1)
        print(mr)


if __name__ == "__main__":
    collection_name = "trace_benchmark"
    field_name = "field"
    dim = 128
    nb = 500000
    coll = create_collection(collection_name, field_name, dim)

    for i in range(20):
        insert(coll, dim, nb)
    create_index(coll, field_name)
    coll.load()

    for i in range(20):
        search(coll, dim, nb, field_name)

    coll.release()
    coll.drop()

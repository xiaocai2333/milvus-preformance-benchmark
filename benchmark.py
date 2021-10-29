import random
import time
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType
connections.connect()

topk = [1, 10, 100, 500, 1000]
nq = [1, 10, 100, 500, 1000]


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
def search(collection, query_entities, field_name, topK):
    search_params = {"metric_type": "L2", "params": {"nprobe": 1}}
    res = collection.search(query_entities, field_name, search_params, limit=topK)


@time_costing
def insert(collection, entities1):
    mr = collection.insert([entities1])


def generate_entities(dim, nb) -> list:
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    return vectors


if __name__ == "__main__":
    collection_name = "trace_benchmark"
    field_name = "field"
    dim = 128
    nb = 100000
    coll = create_collection(collection_name, field_name, dim)

    for i in range(1000):
        entities = generate_entities(dim, nb)
        insert(coll, entities)
    create_index(coll, field_name)
    coll.load()

    for i in topk:
        for j in nq:
            topK = i
            nq = j
            print("topK = ", topK, "nq = ", nq)
            for _ in range(10):
                query_entities = generate_entities(dim, nq)
                search(coll, query_entities, field_name, topK)

    coll.release()

    coll.drop_index()
    # coll.drop()

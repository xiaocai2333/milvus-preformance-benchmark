import random
import time
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType
from config import TopK, NQ, Nprobe, NumberOfTestRun, dim, collection_name, field_name

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
def search(collection, query_entities, field_name, topK, nprobe):
    search_params = {"metric_type": "L2", "params": {"nprobe": nprobe}}
    res = collection.search(query_entities, field_name, search_params, limit=topK, guarantee_timestamp=1)


def generate_entities(dim, nb) -> list:
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    return vectors


if __name__ == "__main__":
    coll = create_collection(collection_name, field_name, dim)

    coll.load()

    for topK in TopK:
        for nq in NQ:
            for nprobe in Nprobe:
                print("nprobe = ", nprobe, "topK = ", topK, "nq = ", nq)
                start = time.time()
                for _ in range(NumberOfTestRun):
                    query_entities = generate_entities(dim, nq)
                    search(coll, query_entities, field_name, topK, nprobe)

                end = time.time()
                print("nprobe = ", nprobe, "topK = ", topK, "nq = ", nq, "test times = 100", "tootal time = ",
                      end - start, "avg time = ", (end-start)/NumberOfTestRun)


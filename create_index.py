import random
import threading
import time
import gc
import numpy as np
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType
from config import dim, nb, batch, thread_nums, vectors_per_file, collection_name, field_name


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
    default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 4096}, "metric_type": "L2"}
    collection.create_index(field_name, default_index)


if __name__ == "__main__":
    coll = create_collection(collection_name, field_name, dim)
    coll.drop_index()
    create_index(coll, field_name)

import random
import time
import gc
import numpy as np
import argparse
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType
from config import dim, nb, batch, collection_name, field_name, default_HNSW, default_IVF_FLAT, index_type


connections.add_connection(default={"host": "172.18.50.4", "port": "19530"})
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
def insert(collection, entities):
    mr = collection.insert(entities)
    gc.collect()
    print(mr)


def gen_data_and_insert(collection, nb, batch, dim):
    for i in range(int(nb/batch)):
        entities = generate_entities(dim, nb)
        insert(collection, entities)


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
        insert(collection, [entities])
        gc.collect()


def generate_entities(dim, nb) -> list:
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    return vectors


def insert_data_from_file(coll, nb, batch, files):
    # logger.info("Load npy file: %s end" % file_name)
    # for j in range(nb // vectors_per_file):
    #     s = "%05d" % j
    #     fname = "binary_" + str(dim) + "d_" + s + ".npy"
    #     data = np.load(fname)
    #     vectors = data.tolist()
    #     insert(coll, vectors)
    # print(str(file))
    # data = []
    # with h5py.File(file, 'r') as f:
    #     data = list(f['train'])
    # for j in range(nb // vectors_per_file):
    #     insert(coll, data)
    insert_num = 0
    flag = True
    while flag:
        for file in files:
            print(file)
            data = np.fromfile(file, dtype=np.float32)
            rows = len(data) // dim
            print(rows)
            data = data[:rows * dim]
            data.shape = -1, dim
            # return
            for i in range(rows//batch):
                entities = data[i*batch:(i+1)*batch, :].tolist()
                if len(entities) < batch:
                    continue
                insert(coll, [[insert_num*batch+i for i in range(batch)], entities])
                insert_num += 1
                print("insert times", insert_num)
                if insert_num*batch >= nb:
                    flag = False
                    break
    # data.shape = -1, 768
    # num_entities = 10000

    # for j in range(nb // vectors_per_file):
    #     insert(coll, data.tolist())


@time_costing
def create_index(collection):
    if index_type == "IVF_FLAT":
        collection.create_index(field_name, default_IVF_FLAT)
    if index_type == "HNSW":
        collection.create_index(field_name, default_HNSW)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="your script description")  # description参数可以用于插入描述脚本用途的信息，可以为空
    parser.add_argument('--files', '-f', nargs='*', type=str, help='verbose mode')

    args = parser.parse_args()  # 将变量以标签-值的字典形式存入args字典
    coll = create_collection(collection_name, field_name, dim, auto_id=True)
    create_index(coll)
    # insert_parallel(coll, nb, dim, batch, thread_nums)
    insert_parallel(coll, nb, dim, batch)
    # insert_data_from_file(coll, nb, batch, args.file)
    create_index(coll)

import random
import time
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType
from config import TopK, NQ, Nprobe, NumberOfTestRun, dim, collection_name, field_name

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


@time_costing
def search(collection, query_entities, field_name, topK, nprobe):
    search_params = {"metric_type": "L2", "params": {"nprobe": nprobe}}
    res = collection.search(query_entities, field_name, search_params, limit=topK, guarantee_timestamp=1)


def generate_entities(dim, nb) -> list:
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    return vectors


if __name__ == "__main__":
    coll = Collection(collection_name)

    coll.release()
    coll.load()

    start_query_entities = generate_entities(dim, NQ[0])
    for _ in range(NumberOfTestRun):
        search(coll, start_query_entities, field_name, TopK[0], Nprobe[0])

    for topK in TopK:
        for nq in NQ:
            for nprobe in Nprobe:
                print("nprobe = ", nprobe, "topK = ", topK, "nq = ", nq)
                query_entities = generate_entities(dim, nq)
                start = time.time()
                for _ in range(NumberOfTestRun):
                    search(coll, query_entities, field_name, topK, nprobe)

                end = time.time()
                print("nprobe = ", nprobe, "topK = ", topK, "nq = ", nq, "test times = ", NumberOfTestRun,
                      "total time = ", end - start, "avg time = ", (end - start) / NumberOfTestRun)

                # i = 0
                # while True:
                #     query_entities = generate_entities(dim, nq)
                #     search(coll, query_entities, field_name, topK, nprobe)
                #     i += 1
                #     if time.time() - start >= 60:
                #         break
                # end = time.time()
                # print("nprobe = ", nprobe, "topK = ", topK, "nq = ", nq, "test times =", i, "total time = ",
                #       end - start, "avg time = ", (end - start) / i)
                # time.sleep(60)

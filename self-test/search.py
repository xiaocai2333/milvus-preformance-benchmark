import random
import time
from pymilvus import connections, Collection
from config import TopK, NQ, Nprobe, NumberOfTestRun, dim, collection_name, field_name, index_type, EF

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
def search(collection, query_entities, search_params, topK):
    # res = collection.search(query_entities, field_name, search_params, limit=topK, guarantee_timestamp=1)
    res = collection.search(query_entities, field_name, search_params, limit=topK)


def do_search(coll):
    if index_type == "IVF_FLAT":
        # cold start
        start_query_entities = generate_entities(dim, NQ[0])
        search_params = {"metric_type": "L2", "params": {"nprobe": Nprobe[0]}}
        for _ in range(NumberOfTestRun):
            search(coll, start_query_entities, search_params, TopK[0])

        # search
        for topK in TopK:
            for nq in NQ:
                for nprobe in Nprobe:
                    search_params = {"metric_type": "L2", "params": {"nprobe": nprobe}}
                    query_entities = generate_entities(dim, nq)
                    print("nprobe = ", nprobe, "topK = ", topK, "nq = ", nq)
                    start = time.time()
                    for _ in range(NumberOfTestRun):
                        search(coll, query_entities, search_params, topK)

                    end = time.time()
                    print("nprobe = ", nprobe, "topK = ", topK, "nq = ", nq, "test times = ", NumberOfTestRun,
                          "total time = ", end - start, "avg time = ", (end - start) / NumberOfTestRun)
    if index_type == "HNSW":
        start_query_entities = generate_entities(dim, NQ[0])
        search_params = {"metric_type": "L2", "params": {"ef": EF[0]}}
        for _ in range(NumberOfTestRun):
            search(coll, start_query_entities, search_params, TopK[0])
        for topK in TopK:
            for nq in NQ:
                for ef in EF:
                    search_params = {"metric_type": "L2", "params": {"ef": ef}}
                    query_entities = generate_entities(dim, nq)
                    print("topK = ", topK, "nq = ", nq, "ef = ", ef)
                    start = time.time()
                    for _ in range(NumberOfTestRun):
                        search(coll, query_entities, search_params, topK)
                    end = time.time()
                    print("topK = ", topK, "nq = ", nq, "ef = ", ef, "test times = ", NumberOfTestRun,
                          "total time = ", end - start, "avg time = ", (end - start)/NumberOfTestRun)


def generate_entities(dim, nb) -> list:
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    return vectors


if __name__ == "__main__":
    coll = Collection(collection_name)

    # coll.release()
    coll.load()
    do_search(coll)

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

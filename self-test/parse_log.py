import argparse
import json
import numpy as np
import pandas
import numpy
from config import TopK, NQ, Nprobe, NumberOfTestRun, QueryNodeNum, index_type, EF


# After parsing, it is in json format, the example is as follows:
# {
#     "Insert": {
#         "CollectionID=428413525825883457": {
#             "MsgID=428413525825683460": {
#                 "time": {
#                     "MsgStream": {
#                         "start": 1634267927799543535,
#                         "end": 1634267927800177783,
#                         "cost": 0.634248
#                     },
#                     "start": 1634267926107002826,
#                     "Proxy": 1092.566149,
#                     "DataNode-Queue": 174.172388,
#                     "InsertBufferNode-bufferInsertMsg": 3010.200005,
#                     "InsertBufferNode-Operate": 3937.750269,
#                     "end": 1634267931911520939,
#                     "Insert-cost": 5804.518113
#                 }
#             },
#         }
#     },
#     "Search": {
#         "CollectionID=428413525825883457": {
#             "MsgID=428413651510362186": {
#                 "time": {
#                     "MsgStream": {
#                         "start": 1634268422274882047,
#                         "end": 1634268422283361609,
#                         "cost": 8.479562
#                     },
#                     "Proxy": 0.833104,
#                     "QueryNode-queue": 286.694318,
#                     "QueryNode-CreatePlan": 0.070193,
#                     "historical-search": 174.244129,
#                     "streaming-search": 174.422271,
#                     "QueryNode-search": 175.422303,
#                     "Proxy-CollectResult": 470.848632,
#                     "Proxy-Reduce": 0.017515,
#                     "Search-cost": 471.911373
#                 }
#             },
#         }
#     }
# }

# "Search": {
#     "CollectionID=123": {
#         "MsgID=456": {
#             "Duration": {
#                 "Pulsar": {
#                     "start": 123
#                     "end": 124
#                     "duration": 1
#                 },
#                 
#             }
#         }
#     }
# }

# ss = ['benchmark-search', 'Role=proxy', 'Step=PreExecute', 'CollectionID=431651428837035329',
# 'MsgID=431651867194344666', 'Duration=5', '']


def parse_log_file(logs, time_dict):
    for s in logs:
        s = s.replace(" ", "").replace("[", "")
        ss = s.split(']')[3:]
        operation = ss[0].split("-")[1]
        if operation not in time_dict.keys():
            time_dict[operation] = {}
        coll = ss[3]
        if coll not in time_dict[operation].keys():
            time_dict[operation][coll] = {}
        msgID = ss[4]
        if msgID not in time_dict[operation][ss[3]].keys():
            time_dict[operation][coll][msgID] = {}
        role = ss[1].split("=")[-1]
        duration = "Duration"
        if duration not in time_dict[operation][coll][msgID].keys():
            time_dict[operation][coll][msgID][duration] = {}
        step = ss[2].split("=")[-1]
        if role == "proxy":
            if step == "SendMsgToMessageStorage":
                if "MessageStorage" not in time_dict[operation][coll][msgID][duration]:
                    time_dict[operation][coll][msgID][duration]["MessageStorage"] = {}
                time_dict[operation][coll][msgID][duration]["MessageStorage"]["start"] = float(ss[5].split("=")[-1])
                # if "end" in time_dict[operation][coll][msgID][duration]["MessageStorage"]:
                #     time_dict[operation][coll][msgID][duration]["MessageStorage"]["cost"] = \
                #         round((time_dict[operation][coll][msgID][duration]["MessageStorage"]["end"] -
                #          time_dict[operation][coll][msgID][duration]["MessageStorage"]["start"])/1000.0/1000.0, 4)
            else:
                time_dict[operation][coll][msgID][duration][step] = round(float(ss[5].split("=")[-1])/1000.0, 4)
        elif role == "querynode":
            if step == "QueryNode-Receive":
                if "MessageStorage" not in time_dict[operation][coll][msgID][duration]:
                    time_dict[operation][coll][msgID][duration]["MessageStorage"] = {}
                if "end" in time_dict[operation][coll][msgID][duration]["MessageStorage"]:
                    if time_dict[operation][coll][msgID][duration]["MessageStorage"]["end"] > float(ss[5].split("=")[-1]):
                        time_dict[operation][coll][msgID][duration]["MessageStorage"]["end"] = float(ss[5].split("=")[-1])
                else:
                    time_dict[operation][coll][msgID][duration]["MessageStorage"]["end"] = float(ss[5].split("=")[-1])
                if "start" in time_dict[operation][coll][msgID][duration]["MessageStorage"]:
                    time_dict[operation][coll][msgID][duration]["MessageStorage"]["cost"] = \
                        round((time_dict[operation][coll][msgID][duration]["MessageStorage"]["end"] -
                         time_dict[operation][coll][msgID][duration]["MessageStorage"]["start"])/1000.0/1000.0, 4)

            else:
                if step not in time_dict[operation][coll][msgID][duration]:
                    time_dict[operation][coll][msgID][duration][step] = float(ss[5].split("=")[-1]) / 1000.0
                # multiple QueryNodes, get the avg of step.
                else:
                    time_dict[operation][coll][msgID][duration][step] = \
                        (time_dict[operation][coll][msgID][duration][step] * QueryNodeNum
                         + float(ss[5].split("=")[-1]) / 1000.0) / QueryNodeNum


def parse_log_files(files, f2):
    all_logs = []
    time_dict = {}
    for file in files:
        for line in file:
            s = str(line.strip('\n'))
            # s = str(json.loads(s))
            if "benchmark-search" in s:
                all_logs.append(s)
    # all_logs.sort()
    # all_logs = sorted(all_logs)
    with open("log.txt", 'w') as f:
        for log in all_logs:
            f.write(log+"\n")
    parse_log_file(all_logs, time_dict)

    json_to_csv(time_dict, f2)
    # json_to_csv2(time_dict)


def add_E2E_time(src, f2):
    e2e_time = {"Insert": {"start": [], "end": [], "e2e": []}, "search": {"start": [], "end": [], "e2e": []}}

    for line in f2:
        s = str(line.strip('\n'))
        if "generate_entities time cost" in s.split(":"):
            continue
        if "insert start time" in s.split(":"):
            e2e_time["Insert"]["start"].append(round(float(s.replace(" ", "").split(":")[-1]) * 1000.0, 4))
        if "insert end time" in s.split(":"):
            e2e_time["Insert"]["end"].append(round(float(s.replace(" ", "").split(":")[-1]) * 1000.0, 4))
        if "insert time cost" in s.split(":"):
            e2e_time["Insert"]["e2e"].append(round(float(s.replace(" ", "").split(":")[-1]) * 1000.0, 4))
        if "search start time" in s.split(":"):
            e2e_time["search"]["start"].append(round(float(s.replace(" ", "").split(":")[-1]) * 1000.0, 4))
        if "search end time" in s.split(":"):
            e2e_time["search"]["end"].append(round(float(s.replace(" ", "").split(":")[-1]) * 1000.0, 4))
        if "search time cost" in s.split(":"):
            e2e_time["search"]["e2e"].append(round(float(s.replace(" ", "").split(":")[-1]) * 1000.0, 4))

    i = 0
    j = 0
    # print("Insert times", len(e2e_time["Insert"]["start"]))
    # print("Search times", len(e2e_time["search"]["e2e"]))
    # print("Search times", len(e2e_time["Search"]["start"]))
    # print("Search times", len(e2e_time["Search"]["end"]))
    for operation in src.keys():
        if operation == "Insert":
            for coll in src[operation].keys():
                for row in src[operation][coll].keys():
                    src[operation][coll][row]["time"]["SDK-Proxy"] = \
                        src[operation][coll][row]["time"]["start"] - e2e_time["Insert"]["start"][i]
                    src[operation][coll][row]["time"]["Proxy-SDK"] = \
                        e2e_time["Insert"]["end"][i] - src[operation][coll][row]["time"]["Proxy-Insert-End"]
                    src[operation][coll][row]["time"]["E2E"] = e2e_time["Insert"]["e2e"][i]
                    i += 1
        pop_keys = []
        if operation == "search":
            for coll in src[operation].keys():
                for row in src[operation][coll].keys():
                    # print(len(src[operation][coll].keys()))
                    # print(len(e2e_time["search"]["e2e"]))
                    # src[operation][coll][row]["time"]["SDK-Proxy"] = \
                    #     src[operation][coll][row]["time"]["start"] - e2e_time["Search"]["start"][j]
                    # src[operation][coll][row]["time"]["Proxy-SDK"] = \
                    #     e2e_time["earch"]["end"][j] - src[operation][coll][row]["time"]["Proxy-Send-Result"]
                    src[operation][coll][row]["Duration"]["E2E"] = e2e_time["search"]["e2e"][j]
                    j += 1
                    if j <= NumberOfTestRun:
                        pop_keys.append(row)
                for key in pop_keys:
                    src[operation][coll].pop(key)
    with open("time_cost.json", 'w') as f:
        f.write(json.dumps(src, indent=4))
    return src


def json_to_csv(src, f2):
    src = add_E2E_time(src, f2)
    for operation in src.keys():
        for col in src[operation].keys():
            field_name = []
            index = []
            k = 0
            for row in src[operation][col].keys():
                k += 1
                index.append(row)
                if operation == "search" and k == NumberOfTestRun:
                    index.append("avg")
                    # index.append(numpy.nan)
                    k = 0
            for row in src[operation][col].keys():
                for field in src[operation][col][row]["Duration"].keys():
                    if field in ["start", "end", "Proxy-Insert-End", "Search-Send-Result", "Search-Receive-Result",
                                 "QueryNode-Proxy", "Proxy-Send-Result"]:
                        continue
                    field_name.append(field)
                break
            data = {}
            avg = {}
            k = 0
            for field in field_name:
                data[field] = []
                avg[field] = 0
                k = 0
                for row in src[operation][col].keys():
                    if field == "MessageStorage":
                        data[field].append(src[operation][col][row]["Duration"][field]["cost"])
                        avg[field] += src[operation][col][row]["Duration"][field]["cost"]
                    else:
                        data[field].append(src[operation][col][row]["Duration"][field])
                        avg[field] += src[operation][col][row]["Duration"][field]
                    k += 1
                    if operation == "search" and k == NumberOfTestRun:
                        data[field].append(round(avg[field] / k, 4))
                        # data[field].append(numpy.nan)
                        avg[field] = 0
                        k = 0
                if k != 0:
                    data[field].append(avg[field] / k)
            if k != 0:
                index.append("avg")
            # print("data", data)
            # for i in data:
            #     print("field, ", i, "length", len(data[i]))
            # print("index", len(index))
            df = pandas.DataFrame(data=data, index=index)
            # print(df)
            df.to_csv(operation + col + '.csv', encoding='gbk')
            file_list = []
            # TopK first
            if index_type == "IVF_FLAT":
                topK, nq, nprobe = 0, 0, -1
                i = 0
                with open(operation + col + '.csv', 'r') as f:
                    for line in f:
                        if (i-1) % (NumberOfTestRun+1) == 0:
                            nprobe += 1
                            if nprobe == len(Nprobe):
                                nq += 1
                                nprobe = 0
                            if nq == len(NQ):
                                topK += 1
                                nq = 0
                            # topK = int(j / (len(NQ)*len(Nprobe)))
                            # nq = int((j-topK*len(NQ)*len(Nprobe))/len(Nprobe))
                            # nprobe = int(j-topK*len(NQ)*len(Nprobe)-nq*len(Nprobe))
                            file_list.append(str("topK = " + str(TopK[topK]) + ", nq = " + str(NQ[nq]) + ", nprobe = " + str(Nprobe[nprobe])) + "\n")
                        file_list.append(line)
                        i += 1
                with open(operation + '_topk.csv', 'w') as f:
                    for line in file_list:
                        f.write(line)

                # NQ first
                lines = [None]*(len(NQ)*len(TopK)*len(Nprobe)*(NumberOfTestRun+2)+1)
                i = 0
                topK, nq, nprobe = 0, 0, -1
                start_pos = 0
                with open(operation + col + '.csv', 'r') as f:
                    for line in f:
                        if (i-1) % (NumberOfTestRun+1) == 0:
                            nprobe += 1
                            if nprobe == len(Nprobe):
                                nq += 1
                                nprobe = 0
                            if nq == len(NQ):
                                topK += 1
                                nq = 0
                            start_pos = (nq * len(TopK) * len(Nprobe) + topK * len(Nprobe) + nprobe) * (NumberOfTestRun + 2) + 1
                            lines[start_pos] = str("topK = " + str(TopK[topK]) + ", nq = " + str(NQ[nq]) + ", nprobe = " + str(Nprobe[nprobe])) + "\n"
                        # print(topK, nq, nprobe)
                        # NumberOfTestRun+2: avg, nq topK nprobe
                        if i == 0:
                            lines[0] = line
                        else:
                            lines[start_pos + (i-1) % (NumberOfTestRun+1)+1] = line
                        i += 1
                with open(operation + '_nq.csv', 'w') as f:
                    for line in lines:
                        f.write(line)
            if index_type == "HNSW":
                topK, nq, ef = 0, 0, -1
                i = 0
                with open(operation + col + '.csv', 'r') as f:
                    for line in f:
                        if (i - 1) % (NumberOfTestRun + 1) == 0:
                            ef += 1
                            if ef == len(EF):
                                nq += 1
                                ef = 0
                            if nq == len(NQ):
                                topK += 1
                                nq = 0
                            # topK = int(j / (len(NQ)*len(Nprobe)))
                            # nq = int((j-topK*len(NQ)*len(Nprobe))/len(Nprobe))
                            # nprobe = int(j-topK*len(NQ)*len(Nprobe)-nq*len(Nprobe))
                            file_list.append(
                                str("topK = " + str(TopK[topK]) + ", nq = " + str(NQ[nq]) + ", ef = " + str(
                                    EF[ef])) + "\n")
                        file_list.append(line)
                        i += 1
                with open(operation + '_topk.csv', 'w') as f:
                    for line in file_list:
                        f.write(line)

                # NQ first
                lines = [None] * (len(NQ) * len(TopK) * len(EF) * (NumberOfTestRun + 2) + 1)
                i = 0
                topK, nq, ef = 0, 0, -1
                start_pos = 0
                with open(operation + col + '.csv', 'r') as f:
                    for line in f:
                        if (i - 1) % (NumberOfTestRun + 1) == 0:
                            ef += 1
                            if ef == len(EF):
                                nq += 1
                                ef = 0
                            if nq == len(NQ):
                                topK += 1
                                nq = 0
                            start_pos = (nq * len(TopK) * len(EF) + topK * len(EF) + ef) * (
                                        NumberOfTestRun + 2) + 1
                            lines[start_pos] = str(
                                "topK = " + str(TopK[topK]) + ", nq = " + str(NQ[nq]) + ", ef = " + str(
                                    EF[ef])) + "\n"
                        # print(topK, nq, nprobe)
                        # NumberOfTestRun+2: avg, nq topK nprobe
                        if i == 0:
                            lines[0] = line
                        else:
                            lines[start_pos + (i - 1) % (NumberOfTestRun + 1) + 1] = line
                        i += 1
                with open(operation + '_nq.csv', 'w') as f:
                    for line in lines:
                        f.write(line)


def json_to_csv2(src):
    for operation in src.keys():
        for col in src[operation].keys():
            field_name = []
            index = []
            for row in src[operation][col].keys():
                index.append(row)
            for row in src[operation][col].keys():
                for field in src[operation][col][row]["Duration"].keys():
                    field_name.append(field)
                break
            data = {}
            for field in field_name:
                data[field] = []
                for row in src[operation][col].keys():
                    if field == "MessageStorage":
                        data[field].append(src[operation][col][row]["Duration"][field]["cost"])
                    else:
                        data[field].append(src[operation][col][row]["Duration"][field])

            df = pandas.DataFrame(data=data, index=index)
            # print(df)
            df.to_csv(operation + col + '.csv', encoding='gbk')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="your script description")  # description??????????????????????????????????????????????????????????????????
    parser.add_argument('--log', '-l', nargs='*', type=argparse.FileType('r'), help='verbose mode')
    parser.add_argument('--sdk', '-s', nargs='*', type=argparse.FileType('r'), help='verbose mode')

    args = parser.parse_args()  # ??????????????????-????????????????????????args??????
    parse_log_files(args.log, args.sdk[0])

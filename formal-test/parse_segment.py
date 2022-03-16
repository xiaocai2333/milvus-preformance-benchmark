import argparse
import json
import numpy as np
import pandas
import numpy
from config import TopK, NQ, Nprobe, NumberOfTestRun


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
        s = s.replace(" ", "").replace(",", "")
        print(s)
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
        duration = "Duration"
        if duration not in time_dict[operation][coll][msgID].keys():
            time_dict[operation][coll][msgID][duration] = {}
        step = ss[2].split("=")[-1]
        if step == "SendMsgToMessageStorage":
            if "MessageStorage" not in time_dict[operation][coll][msgID][duration]:
                time_dict[operation][coll][msgID][duration]["MessageStorage"] = {}
            time_dict[operation][coll][msgID][duration]["MessageStorage"]["start"] = float(ss[5].split("=")[-1])
            if "end" in time_dict[operation][coll][msgID][duration]["MessageStorage"]:
                time_dict[operation][coll][msgID][duration]["MessageStorage"]["cost"] = \
                    round((time_dict[operation][coll][msgID][duration]["MessageStorage"]["end"] -
                     time_dict[operation][coll][msgID][duration]["MessageStorage"]["start"])/1000.0/1000.0, 4)
        elif step == "QueryNode-Receive":
            if "MessageStorage" not in time_dict[operation][coll][msgID][duration]:
                time_dict[operation][coll][msgID][duration]["MessageStorage"] = {}
            time_dict[operation][coll][msgID][duration]["MessageStorage"]["end"] = float(ss[5].split("=")[-1])
            if "start" in time_dict[operation][coll][msgID][duration]["MessageStorage"]:
                time_dict[operation][coll][msgID][duration]["MessageStorage"]["cost"] = \
                    round((time_dict[operation][coll][msgID][duration]["MessageStorage"]["end"] -
                     time_dict[operation][coll][msgID][duration]["MessageStorage"]["start"])/1000.0/1000.0, 4)
        else:
            time_dict[operation][coll][msgID][duration][step] = round(float(ss[5].split("=")[-1])/1000.0, 4)
        # if int(ss[1].split("=")[-1]) not in [0, 1]:
        #     if ss[1] not in time_dict[operation].keys() and int(ss[1].split("=")[-1]) != 1:
        #         time_dict[operation][ss[1]] = {}
        #     if ss[2] not in time_dict[operation][ss[1]].keys():
        #         time_dict[operation][ss[1]][ss[2]] = {}
        #     if "time" not in time_dict[operation][ss[1]][ss[2]].keys():
        #         time_dict[operation][ss[1]][ss[2]]["time"] = {}
        #     if "MsgStream" not in time_dict[operation][ss[1]][ss[2]]["time"]:
        #         time_dict[operation][ss[1]][ss[2]]["time"]["MsgStream"] = {}
        # if ss[3].split("=")[-1] == "MsgStream-send":
        #     time_dict[operation][ss[1]][ss[2]]["time"]["MsgStream"]["start"] = int(ss[4].split("=")[-1])
        # elif ss[3].split("=")[-1] == "MsgStream-receive":
        #     for key in time_dict[operation].keys():
        #         if ss[2] not in time_dict[operation][key]:
        #             continue
        #         if operation == "Search" and "end" in time_dict[operation][key][ss[2]]["time"]["MsgStream"].keys():
        #             continue
        #         if "start" in time_dict[operation][key][ss[2]]["time"]["MsgStream"].keys():
        #             end = int(ss[4].split("=")[-1])
        #             time_dict[operation][key][ss[2]]["time"]["MsgStream"]["end"] = end
        #             time_dict[operation][key][ss[2]]["time"]["MsgStream"]["cost"] = \
        #                 (end - time_dict[operation][key][ss[2]]["time"]["MsgStream"]["start"]) / 1000000.0
        # elif ss[3].split("=")[-1] == "QueryNode-queue":
        #     for key in time_dict[operation].keys():
        #         if ss[2] not in time_dict[operation][key]:
        #             continue
        #         if ss[3].split("=")[-1] in time_dict[operation][key][ss[2]]["time"].keys():
        #             continue
        #         if "end" in time_dict[operation][key][ss[2]]["time"]["MsgStream"].keys():
        #             queue = int(ss[4].split("=")[-1])
        #             time_dict[operation][key][ss[2]]["time"][ss[3].split("=")[-1]] = \
        #                 (queue - time_dict[operation][key][ss[2]]["time"]["MsgStream"]["end"]) / 1000000.0
        #             # print(time_dict[operation][key][ss[2]]["time"])
        # elif ss[3].split("=")[-1] == "DataNode-Queue":
        #     for key in time_dict[operation].keys():
        #         if ss[2] not in time_dict[operation][key]:
        #             continue
        #         if ss[3].split("=")[-1] in time_dict[operation][key][ss[2]]["time"].keys():
        #             continue
        #         if "end" in time_dict[operation][key][ss[2]]["time"]["MsgStream"].keys():
        #             queue = int(ss[4].split("=")[-1])
        #             time_dict[operation][key][ss[2]]["time"][ss[3].split("=")[-1]] = \
        #                 (queue - time_dict[operation][key][ss[2]]["time"]["MsgStream"]["end"]) / 1000000.0
        # elif ss[3].split("=")[-1] == "Proxy-Receive-Request":
        #     for key in time_dict[operation].keys():
        #         if ss[2] not in time_dict[operation][key]:
        #             continue
        #         time_dict[operation][key][ss[2]]["time"]["start"] = int(ss[4].split("=")[-1]) / 1000000.0
        # elif ss[3].split("=")[-1] == "Insert-End":
        #     for key in time_dict[operation].keys():
        #         if ss[2] not in time_dict[operation][key]:
        #             continue
        #         if "start" in time_dict[operation][key][ss[2]]["time"].keys():
        #             end = int(ss[4].split("=")[-1]) / 1000000.0
        #             time_dict[operation][key][ss[2]]["time"]["end"] = end
        #             time_dict[operation][key][ss[2]]["time"]["Insert-cost"] = \
        #                 end - time_dict[operation][key][ss[2]]["time"]["start"]
        # elif ss[3].split("=")[-1] == "Search-Receive-Result":
        #     end = int(ss[4].split("=")[-1])
        #     time_dict[operation][ss[1]][ss[2]]["time"][ss[3].split("=")[-1]] = end / 1000000.0
        #     for key in time_dict[operation].keys():
        #         if ss[2] not in time_dict[operation][key]:
        #             continue
        #         if "Search-Send-Result" in time_dict[operation][key][ss[2]]["time"].keys():
        #             time_dict[operation][key][ss[2]]["time"]["QueryNode-Proxy"] = \
        #                 (end - time_dict[operation][key][ss[2]]["time"]["Search-Receive-Result"]) / 1000000.0
        # elif int(ss[1].split("=")[-1]) not in [0, 1]:
        #     time_dict[operation][ss[1]][ss[2]]["time"][ss[3].split("=")[-1]] = int(ss[4].split("=")[-1]) / 1000000.0


def parse_log_files(files):
    all_logs = []
    time_dict = {}
    for file in files:
        for line in file:
            s = str(line.strip('\n'))
            # s = str(json.loads(s))
            if "benchmark-segment" in s:
                all_logs.append(s)
    # all_logs.sort()
    # all_logs = sorted(all_logs)
    with open("segment.log", 'w') as f:
        for log in all_logs:
            f.write(log+"\n")
    parse_log_file(all_logs, time_dict)

    json_to_csv(time_dict)


def json_to_csv(src):
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
    parser = argparse.ArgumentParser(description="your script description")  # description参数可以用于插入描述脚本用途的信息，可以为空
    parser.add_argument('--log', '-l', nargs='*', type=argparse.FileType('r'), help='verbose mode')

    args = parser.parse_args()  # 将变量以标签-值的字典形式存入args字典
    parse_log_files(args.log)

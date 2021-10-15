import argparse
import json


# After parsing, it is in json format, the example is as follows:
# {
#     "Insert": {
#         "CollectionID=428413087200513345": {
#             "MsgID=428413087200313348": {
#                 "time": {
#                     "Pulsar": {
#                         "start": 1634266254909958397,
#                         "end": 1634266254911624369,
#                         "cost": 1.665972
#                     },
#                     "Proxy": 1103.668329,
#                     "DataNode-Queue": 127.799014,
#                     "InsertBufferNode-bufferInsertMsg": 2987.642785,
#                     "InsertBufferNode-Operate": 3880.092908
#                 }
#             },
#         }
#     },
#     "Search": {
#         "CollectionID=428413087200513345": {
#             "MsgID=428413208900665414": {
#                 "time": {
#                     "Pulsar": {
#                         "start": 1634266731623236003,
#                         "end": 1634266731809931752,
#                         "cost": 186.695749
#                     },
#                     "Proxy": 0.82322,
#                     "QueryNode-queue": 0.046229,
#                     "QueryNode-CreatePlan": 0.061269,
#                     "historical-search": 179.165587,
#                     "streaming-search": 179.352907,
#                     "QueryNode-search": 180.41274,
#                     "Proxy-CollectResult": 186.832587,
#                     "Proxy-Reduce": 0.019133,
#                     "Search-cost": 187.904002
#                 }
#             },
#         }
#     }
# }

def parse_log_file(f):
    time_dict = {}
    for line in f:
        s = str(line.strip('\n'))
        s = s.replace(" ", "").replace("[", "")
        ss = s.split(']')[3:]
        operation = ss[0].split("-")[1]
        if operation not in time_dict.keys():
            time_dict[operation] = {}

        if int(ss[1].split("=")[-1]) != 1:
            if ss[1] not in time_dict[operation].keys() and int(ss[1].split("=")[-1]) != 1:
                time_dict[operation][ss[1]] = {}
            if ss[2] not in time_dict[operation][ss[1]].keys():
                time_dict[operation][ss[1]][ss[2]] = {}
            if "time" not in time_dict[operation][ss[1]][ss[2]].keys():
                time_dict[operation][ss[1]][ss[2]]["time"] = {}
            if "Pulsar" not in time_dict[operation][ss[1]][ss[2]]["time"]:
                time_dict[operation][ss[1]][ss[2]]["time"]["Pulsar"] = {}
        if ss[3].split("=")[-1] == "pulsar-send":
            time_dict[operation][ss[1]][ss[2]]["time"]["Pulsar"]["start"] = int(ss[4].split("=")[-1])
        elif ss[3].split("=")[-1] == "pulsar-receive":
            for key in time_dict[operation].keys():
                if ss[2] not in time_dict[operation][key]:
                    continue
                if "start" in time_dict[operation][key][ss[2]]["time"]["Pulsar"].keys():
                    end = int(ss[4].split("=")[-1])
                    time_dict[operation][key][ss[2]]["time"]["Pulsar"]["end"] = end
                    time_dict[operation][key][ss[2]]["time"]["Pulsar"]["cost"] = \
                        (end - time_dict[operation][key][ss[2]]["time"]["Pulsar"]["start"])/1000000.0
        elif ss[3].split("=")[-1] == "QueryNode-queue":
            for key in time_dict[operation].keys():
                if ss[2] not in time_dict[operation][key]:
                    continue
                if "end" in time_dict[operation][key][ss[2]]["time"]["Pulsar"].keys():
                    queue = int(ss[4].split("=")[-1])
                    time_dict[operation][key][ss[2]]["time"][ss[3].split("=")[-1]] = \
                        (queue - time_dict[operation][key][ss[2]]["time"]["Pulsar"]["end"])/1000000.0
        elif ss[3].split("=")[-1] == "DataNode-Queue":
            for key in time_dict[operation].keys():
                if ss[2] not in time_dict[operation][key]:
                    continue
                if "end" in time_dict[operation][key][ss[2]]["time"]["Pulsar"].keys():
                    queue = int(ss[4].split("=")[-1])
                    time_dict[operation][key][ss[2]]["time"][ss[3].split("=")[-1]] = \
                        (queue - time_dict[operation][key][ss[2]]["time"]["Pulsar"]["end"])/1000000.0
        elif ss[3].split("=")[-1] == "Insert-Start":
            for key in time_dict[operation].keys():
                if ss[2] not in time_dict[operation][key]:
                    continue
                time_dict[operation][key][ss[2]]["time"]["start"] = int(ss[4].split("=")[-1])
        elif ss[3].split("=")[-1] == "Insert-End":
            for key in time_dict[operation].keys():
                if ss[2] not in time_dict[operation][key]:
                    continue
                if "start" in time_dict[operation][key][ss[2]]["time"].keys():
                    end = int(ss[4].split("=")[-1])
                    time_dict[operation][key][ss[2]]["time"]["end"] = end
                    time_dict[operation][key][ss[2]]["time"]["Insert-cost"] = \
                        (end - time_dict[operation][key][ss[2]]["time"]["start"])/1000000.0
        elif int(ss[1].split("=")[-1]) != 1:
            time_dict[operation][ss[1]][ss[2]]["time"][ss[3].split("=")[-1]] = int(ss[4].split("=")[-1])/1000000.0

    with open("time_cost.json", 'w') as f:
        f.write(json.dumps(time_dict, indent=4))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="your script description")  # description参数可以用于插入描述脚本用途的信息，可以为空
    parser.add_argument('--log', '-l', nargs='*', type=argparse.FileType('r'), help='verbose mode')

    args = parser.parse_args()  # 将变量以标签-值的字典形式存入args字典
    for file in args.log:
        parse_log_file(file)

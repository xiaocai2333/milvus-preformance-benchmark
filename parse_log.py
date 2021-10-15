import argparse
import json
import pandas


# After parsing, it is in json format, the example is as follows:
# {
#     "Insert": {
#         "CollectionID=428413525825883457": {
#             "MsgID=428413525825683460": {
#                 "time": {
#                     "Pulsar": {
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
#                     "Pulsar": {
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
                if operation == "Search" and "end" in time_dict[operation][key][ss[2]]["time"]["Pulsar"].keys():
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

    json_to_csv(time_dict)


def add_E2E_time(src):
    insert_e2e_time = []
    search_e2e_time = []
    with open("benchmark_time.txt", 'r') as f:
        for line in f:
            s = str(line.strip('\n'))
            if "generate_entities time cost" in s.split(":"):
                continue
            if "insert time cost" in s.split(":"):
                insert_e2e_time.append(float(s.replace(" ", "").split(":")[-1])*1000.0)
            if "search time cost" in s.split(":"):
                search_e2e_time.append(float(s.replace(" ", "").split(":")[-1])*1000.0)

    i = 0
    j = 0
    for operation in src.keys():
        if operation == "Insert":
            for coll in src[operation].keys():
                for row in src[operation][coll].keys():
                    src[operation][coll][row]["time"]["E2E"] = insert_e2e_time[i]
                    i += 1
        if operation == "Search":
            for coll in src[operation].keys():
                for row in src[operation][coll].keys():
                    src[operation][coll][row]["time"]["E2E"] = insert_e2e_time[j]
                    j += 1
    return src


def json_to_csv(src):
    src = add_E2E_time(src)
    for operation in src.keys():
        field_name = []
        data = {}
        avg = {}
        index = []
        for col in src[operation].keys():
            for row in src[operation][col].keys():
                index.append(row)
            for row in src[operation][col].keys():
                for field in src[operation][col][row]["time"].keys():
                    if field in ["start", "end"]:
                        continue
                    field_name.append(field)
                break
            for field in field_name:
                data[field] = []
                avg[field] = 0

                for row in src[operation][col].keys():
                    if field == "Pulsar":
                        data[field].append(src[operation][col][row]["time"][field]["cost"])
                        avg[field] += src[operation][col][row]["time"][field]["cost"]
                        continue
                    data[field].append(src[operation][col][row]["time"][field])
                    avg[field] += src[operation][col][row]["time"][field]
            for field in field_name:
                data[field].append(avg[field]/len(data[field]))
            index.append("avg")
            df = pandas.DataFrame(data=data, index=index)
            # print(df)
            df.to_csv(operation+col+'.csv', encoding='gbk')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="your script description")  # description参数可以用于插入描述脚本用途的信息，可以为空
    parser.add_argument('--log', '-l', nargs='*', type=argparse.FileType('r'), help='verbose mode')

    args = parser.parse_args()  # 将变量以标签-值的字典形式存入args字典
    for file in args.log:
        parse_log_file(file)

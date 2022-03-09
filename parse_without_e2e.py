import argparse
import json
import numpy as np
import pandas
import numpy
from config import TopK, NQ, Nprobe, NumberOfTestRun


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
                    (time_dict[operation][coll][msgID][duration]["MessageStorage"]["end"] -
                     time_dict[operation][coll][msgID][duration]["MessageStorage"]["start"])/1000000
        elif step == "QueryNode-Receive":
            if "MessageStorage" not in time_dict[operation][coll][msgID][duration]:
                time_dict[operation][coll][msgID][duration]["MessageStorage"] = {}
            time_dict[operation][coll][msgID][duration]["MessageStorage"]["end"] = float(ss[5].split("=")[-1])
            if "start" in time_dict[operation][coll][msgID][duration]["MessageStorage"]:
                time_dict[operation][coll][msgID][duration]["MessageStorage"]["cost"] = \
                    (time_dict[operation][coll][msgID][duration]["MessageStorage"]["end"] -
                     time_dict[operation][coll][msgID][duration]["MessageStorage"]["start"])/1000000
        else:
            time_dict[operation][coll][msgID][duration][step] = float(ss[5].split("=")[-1])


def parse_log_files(files):
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

    json_to_csv(time_dict)
    # json_to_csv2(time_dict)


def json_to_csv(src):
    # src = add_E2E_time(src, f2)
    with open("time_cost.json", 'w') as f:
        f.write(json.dumps(src, indent=4))
    for operation in src.keys():
        for col in src[operation].keys():
            field_name = []
            index = []
            k = 0
            for row in src[operation][col].keys():
                k += 1
                if k == 1:
                    continue
                # index.append(row)
                if operation == "search" and k == NumberOfTestRun:
                    index.append("avg")
                    index.append(numpy.nan)
                    k = 0
            for row in src[operation][col].keys():
                for field in src[operation][col][row]["Duration"].keys():
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
                    if k == 0:
                        k += 1
                        continue
                    if field == "MessageStorage":
                        # data[field].append(src[operation][col][row]["Duration"][field]["cost"])
                        avg[field] += src[operation][col][row]["Duration"][field]["cost"]
                    else:
                        # data[field].append(src[operation][col][row]["Duration"][field])
                        avg[field] += src[operation][col][row]["Duration"][field]
                    k += 1
                    if operation == "search" and k == NumberOfTestRun:
                        data[field].append(avg[field] / (k-1))
                        data[field].append(numpy.nan)
                        avg[field] = 0
                        k = 0
                if k != 0:
                    data[field].append(avg[field] / k)
            if k != 0:
                index.append("avg")
            # print("data", data)
            for i in data:
                print("field, ", i, "length", len(data[i]))
            print("index", len(index))
            df = pandas.DataFrame(data=data, index=index)
            # print(df)
            df.to_csv(operation + col + '.csv', encoding='gbk')
            file_list = []
            l = 0
            i = 0
            j = 0
            with open(operation + col + '.csv', 'r') as f:
                for line in f:
                    l += 1
                    if l == 1:
                        i = 0
                        j = 0
                        file_list.append(line)
                        continue
                    if i % 1 == 0:
                        topK = int(j / (len(NQ)*len(Nprobe)))
                        nq = int((j-topK*len(NQ)*len(Nprobe))/len(Nprobe))
                        nprobe = int(j-topK*len(NQ)*len(Nprobe)-nq*len(Nprobe))
                        file_list.append(str("topK = " + str(TopK[topK]) + ", nq = " + str(NQ[nq]) + ", nprobe = " + str(Nprobe[nprobe])) + "\n")
                    file_list.append(line)
                    i += 1
                    if i % 1 == 0:
                        j += 1
            with open(operation + '.csv', 'w') as f:
                for line in file_list:
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
    parser = argparse.ArgumentParser(description="your script description")  # description参数可以用于插入描述脚本用途的信息，可以为空
    parser.add_argument('--log', '-l', nargs='*', type=argparse.FileType('r'), help='verbose mode')
    # parser.add_argument('--sdk', '-s', nargs='*', type=argparse.FileType('r'), help='verbose mode')

    args = parser.parse_args()  # 将变量以标签-值的字典形式存入args字典
    parse_log_files(args.log)

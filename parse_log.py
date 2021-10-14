import argparse
import json


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
                if "start" in time_dict[operation][key][ss[2]]["time"]["Pulsar"]:
                    end = int(ss[4].split("=")[-1])
                    time_dict[operation][key][ss[2]]["time"]["Pulsar"]["end"] = end
                    time_dict[operation][key][ss[2]]["time"]["Pulsar"]["cost"] = (end - time_dict[operation][key][ss[2]]["time"]["Pulsar"]["start"])/1000000.0
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

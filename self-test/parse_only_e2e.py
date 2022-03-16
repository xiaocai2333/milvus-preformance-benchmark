import argparse
import json
import numpy as np
import pandas
import numpy
from config import TopK, NQ, Nprobe, NumberOfTestRun, QueryNodeNum


def parse_log_files(e2e_file):
    e2e_time = {"search": {"index": [], "e2e": []}}

    i = 0
    j = 0
    for line in e2e_file:
        s = str(line.strip('\n'))
        if "generate_entities time cost" in s.split(":"):
            continue
        if "cost" not in line:
            continue
        i += 1

        if i <= NumberOfTestRun:
            continue
        if "search time cost" in s.split(":"):
            e2e_time["search"]["index"].append(j)
            e2e_time["search"]["e2e"].append(round(float(s.replace(" ", "").split(":")[-1]) * 1000.0, 4))
            j += 1
    topK, nq, nprobe = 0, 0, -1
    lines = []
    avg = 0
    lines.append("index, e2e\n")
    for i in e2e_time["search"]["index"]:
        if i % NumberOfTestRun == 0:
            nprobe += 1
            if nprobe == len(Nprobe):
                nq += 1
                nprobe = 0
            if nq == len(NQ):
                topK += 1
                nq = 0
            lines.append(str("topK = " + str(TopK[topK]) + ", nq = " + str(NQ[nq]) + ", nprobe = " + str(Nprobe[nprobe])) + "\n")
        lines.append(str(i)+", " + str(e2e_time["search"]["e2e"][i]) + "\n")
        avg += e2e_time["search"]["e2e"][i]
        i += 1
        if i % NumberOfTestRun == 0:
            lines.append("avg, " + str(round(avg/NumberOfTestRun, 4)) + "\n")
            avg = 0
    with open("e2e.csv", 'w') as f:
        for line in lines:
            f.write(line)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="your script description")  # description参数可以用于插入描述脚本用途的信息，可以为空
    parser.add_argument('--sdk', '-s', nargs='*', type=argparse.FileType('r'), help='verbose mode')

    args = parser.parse_args()  # 将变量以标签-值的字典形式存入args字典
    parse_log_files(args.sdk[0])

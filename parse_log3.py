import argparse

import pandas


def parse_log_files(files, out):
    all_logs = []
    time_dict = {}
    nb = 1000000
    cost = 0
    for file in files:
        for line in file:
            s = str(line.strip('\n'))
            # s = str(json.loads(s))
            ss = s.split(" ")

            if "nb" in ss:
                nb = int(ss[-1])
                if nb not in time_dict:
                    time_dict[nb] = {}
            if nb not in time_dict:
                time_dict[nb] = {}
            if "interval" in s:
                all_logs.append(s)

                if "guarantee_timestamp" in ss:
                    if "guarantee_timestamp=1" not in time_dict[nb]:
                        time_dict[nb]["guarantee_timestamp=1"] = {}
                    if ss[10] == "start":
                        cost = 0
                    if ss[10] == "end":
                        time_dict[nb]["guarantee_timestamp=1"]["cost"] = cost / 50

                if "graceful" in ss:
                    if ss[10] not in time_dict[nb]:
                        time_dict[nb][ss[10]] = {}
                    if ss[11] == "start":
                        cost = 0
                    if ss[11] == "end":
                        time_dict[nb][ss[10]]["cost"] = cost / 100

            if "search" in ss and "cost:" in ss:
                cost += float(ss[-1])

    field_names = []
    index = list(time_dict.keys())
    for row in time_dict:
        field_names = list(time_dict[row].keys())
        break
    data = {}
    for field in field_names:
        data[field] = []
        for row in time_dict:
            data[field].append(time_dict[row][field]["cost"])

    df = pandas.DataFrame(data=data, index=index)
    df.to_csv(out[0], encoding='gbk')
    # print(all_logs)
    # print(time_dict)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="your script description")  # description参数可以用于插入描述脚本用途的信息，可以为空
    parser.add_argument('--file', '-f', nargs='*', type=argparse.FileType('r'), help='verbose mode')
    parser.add_argument('--output', '-o', nargs='*', help='verbose mode')

    args = parser.parse_args()  # 将变量以标签-值的字典形式存入args字典
    parse_log_files(args.file, args.output)

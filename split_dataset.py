import argparse
import numpy as np

# 100K vectors per file
# file_name: "binary_768d_00000.npy"
vectors_per_new_file = 100000


def split_dataset(files):
    file_number = 0

    for file in files:
        print(file)
        # data = np.fromfile(file, dtype=np.float32, count=vectors_per_old_file * 768)
        data = np.fromfile(file, dtype=np.float32)
        print(len(data))
        rows = len(data)//768
        print(rows)
        data = data[:rows*768]
        data.shape = -1, 768
        print(len(data))
        # return
        for i in range(rows//vectors_per_new_file):
            vectors = data[i * vectors_per_new_file:(i + 1) * vectors_per_new_file, :]
            print(len(vectors))
            if len(vectors) < vectors_per_new_file:
                continue
            np.save("binary_768d_"+str("%05d" % file_number)+".npy", vectors)
            file_number += 1
            print("file_number", file_number)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="your script description")  # description参数可以用于插入描述脚本用途的信息，可以为空
    parser.add_argument('--files', '-f', nargs='*', type=str, help='verbose mode')

    args = parser.parse_args()  # 将变量以标签-值的字典形式存入args字典
    split_dataset(args.files)

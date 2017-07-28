from __future__ import print_function
import sys
import os
from subprocess import call

def put_to_hdfs(input, destination):
    call(["hdfs", "dfs", "-mkdir", "-p", destination])
    all_files = ""
    for file in os.listdir(input):
        if file.endswith(".json"):
            input_file = input+"/"+file
            call(["hdfs", "dfs", "-put", input+"/"+file, destination])
            all_files += input_file+","
    print(all_files[:-1])
if __name__ == "__main__":
    put_to_hdfs(sys.argv[1], sys.argv[2])
import numpy as np

OPERATOR_LOG_FILE_NAME = "operator_log.txt"

latencies = []

with open(OPERATOR_LOG_FILE_NAME, "r+") as f:
    for line in f:
        toks = line.strip().split("|")
        latencies.append(float(toks[2]))

print(np.mean(latencies))
import numpy as np

QUERY_LOG_FILE_NAME = "query_log.txt"

latencies = []

with open(QUERY_LOG_FILE_NAME, "r+") as f:
    for line in f:
        toks = line.strip().split("|")
        latencies.append(float(toks[2]))

print(np.mean(latencies))

# New Run
# Cost aware mean latency: 8.468866348266602e-06
# Non-cost aware meant latency: 8.320093154907226e-06
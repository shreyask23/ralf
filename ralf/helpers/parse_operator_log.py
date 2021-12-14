import numpy as np

OPERATOR_LOG_FILE_NAME = "operator_log.txt"

latencies = []
ignore_lines = 200

with open(OPERATOR_LOG_FILE_NAME, "r+") as f:
    lines = f.readlines()[ignore_lines:]
    for line in lines:
        toks = line.strip().split("|")
        latencies.append(float(toks[2]))

print(np.mean(latencies))

# New Run
# Cost aware mean latency: 0.06617632985115052
# Non-cost aware meant latency: 0.1645273232460022
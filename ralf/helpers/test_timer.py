import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--len", "-l", type=int)
args = parser.parse_args()

WRITE_MESSAGE = "This is a new record!" * args.len + "\n"  # 350000

start_time = time.time()
with open("disk_write.txt", "a+") as f:
    f.write(WRITE_MESSAGE)
end_time = time.time()

print(f"Checking with length {args.len}: ", end_time - start_time)
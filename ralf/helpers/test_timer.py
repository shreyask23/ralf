import time

WRITE_MESSAGE = "This is a new record!"*350000

start_time = time.time()
with open("disk_write.txt", "a+") as f:
    f.write(WRITE_MESSAGE)
end_time = time.time()

print(end_time - start_time)

import datetime
import json


f = open('./logs.txt', 'r')

prev_log = None
# for i in range(5):
    # line = f.readline()
    # print(line)

lines = f.readlines()

problem = False
for line in lines[8:]:
    log_dict = json.loads(line)
    log = int(log_dict['message'])
    timestamp = log_dict['timestamp']
    if prev_log:
        if log - prev_log != 1:
            print(f"Skipping logs between {prev_log} and {log} from {start_time} to {timestamp}")
            problem = True
            start_time = timestamp
    else:
        # print(f"Starting from log {log}")
        start_time = timestamp
    prev_log = log

if not problem:
    print(f'No problems found')
    

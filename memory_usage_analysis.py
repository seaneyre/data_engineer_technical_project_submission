#%%
import tracemalloc
import submission
import gzip
import os
from datetime import datetime
import time

input_data = 'sample.gz'

#%%

def analyse_streaming():

    tracemalloc.start()

    submission.main()

    current, peak = tracemalloc.get_traced_memory()

    print(f"STREAMING: Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")

    tracemalloc.stop()

    return peak / 10**6

def analyse_load():

    tracemalloc.start()

    with gzip.open(input_data, 'rt', encoding="utf8") as f:
        f.readlines()

    current, peak = tracemalloc.get_traced_memory()

    print(f"DATA LOAD: Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")

    tracemalloc.stop()

    return peak / 10**6
#%% 

# Streaming profiling

peaks = []
seconds_runnings = []
i = 0
while i < 5:
    try:
        os.remove("output.db")
    except:
        pass
    
    print(f'Run {i+1}')
    
    i+=1
    
    start = datetime.now()
    
    peak = analyse_streaming()
    
    end = datetime.now()
    
    peaks.append(peak)

    seconds_running = int((end - start).total_seconds())
    
    seconds_runnings.append(seconds_running)

    print(f'Run {i}: {peak}MB, {seconds_running}s')

peak_mean = sum(peaks) / len(peaks)
seconds_mean = sum(seconds_runnings) / len(seconds_runnings)
print(f"Streaming average peak memory load = {peak_mean}MB. Average time taken = {seconds_mean} seconds")
## Streaming average peak memory load = 1.3995996MB. Average time taken = 494.6 seconds

#%%

peaks = []
i = 0
while i < 5:
    i+=1
    peak = analyse_load()

    peaks.append(peak)

peak_mean = sum(peaks) / len(peaks)
print(f"Data Load average peak memory load = {peak_mean}MB.")
## Data Load average peak memory load = 178.20792740000002MB.
#%%
## Profiling Results:
### Streaming average peak memory load = 1.4MB
### Data load average peak memory load = 178MB
### Average time taken = 494.6 seconds
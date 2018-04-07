import pandas as pd
import numpy as np
import os
from pandas import read_csv
folder_path = '/home/hunter/GoogleCluster/task_usage_extract/'
allTask = 0
for file_name in os.listdir(folder_path):
    row_count = sum(1 for row in fileObject)
    allTask += row_count
print allTask
# colnames = ['jobId','numberOfTask'] 
# df = read_csv('results/JobID-numberOfTask.csv', header=None, index_col=False, names=colnames, usecols=[1], engine='python')
# taskJobIdArr = df['numberOfTask'].values
# allTask=0
# for task in taskJobIdArr:
#     allTask += task
# print allTask

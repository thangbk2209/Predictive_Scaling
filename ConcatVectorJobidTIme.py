import pandas as pd
import numpy as np
import os
from pandas import read_csv

colnames = ['jobId','numberOfTask'] 
df = read_csv('results/JobID-numberOfTask.csv', header=None, index_col=False, names=colnames, usecols=[0], engine='python')
JobIdArr = df['jobId'].values
numberOfJob = 0
fout=open("results/vectorTimeJobid.csv","a")
for jobid in JobIdArr:
    numberOfJob += 1

    if numberOfJob == 284:
        break
    f = open("results/%s.csv"%(jobid))
    for line in f:
         fout.write(line)
    f.close() # not really needed
fout.close()

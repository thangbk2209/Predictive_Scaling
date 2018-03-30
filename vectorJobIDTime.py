import pandas as pd
import numpy as np
import os
from pandas import read_csv
folder_path = '/home/hunter/spark/spark-2.2.0-bin-hadoop2.7/thangbk2209/JobId_time/'
name=[]
colnames = ['jobId','numberOfTask'] 
df = read_csv('/home/hunter/spark/spark-2.2.0-bin-hadoop2.7/thangbk2209/Predictive_Scaling/results/JobID-numberOfTask.csv', header=None, index_col=False, names=colnames, usecols=[0], engine='python')
JobIdArr = df['jobId'].values
numberOfJob = 0
for jobid in JobIdArr:
  numberOfJob ++
  arrVectorJobid = []
  if numberOfJob == 284:
    break
  for partNumber in range(0,500):
    file_name = string(jobid)+"_part-00"+str(partNumber).zfill(3)+"-of-00500.csv"
    print file_name
      if os.stat(file_name).st_size == 0:
        arrVectorJobid.append(0)
      else:
        arrVectorJobid.append(1)
  newDf = pd.DataFrame(arrVectorJobid)
# df1 = newDf.replace(np.nan, 0, regex=True)
  newDf.to_csv('/home/hunter/spark/spark-2.2.0-bin-hadoop2.7/thangbk2209/Predictive_Scaling/results/%s.csv'%(jobid), index=False, header=None)


from pyspark.sql.session import SparkSession as spark
import pandas as pd
from pandas import read_csv
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from matplotlib import pyplot
import os
sc = SparkContext(appName="Task_usage")
sql_context = SQLContext(sc)
colnames = ['jobId','numberOfTask'] 
df = read_csv('/home/hunter/spark/spark-2.2.0-bin-hadoop2.7/thangbk2209/Predictive_Scaling/results/JobID-numberOfTask.csv', header=None, index_col=False, names=colnames, usecols=[0], engine='python')
JobIdArr = df['jobId'].values
# folder_path ='/mnt/volume/ggcluster/clusterdata-2011-2/task_usage/'
folder_path = '/home/hunter/GoogleCluster/task_usage_extract/'

dataSchema = StructType([StructField('startTime', StringType(), True),
                         StructField('endTime', StringType(), True),
                         StructField('JobId', LongType(), True),
                         StructField('taskIndex', LongType(), True),
                         StructField('machineId', LongType(), True),
                         StructField('meanCPUUsage', FloatType(), True),
                         # canonical memory usage
                         StructField('CMU', FloatType(), True),
                         # assigned memory usage
                         StructField('AssignMem', FloatType(), True),
                         # unmapped page cache memory usage
                         StructField('unmapped_cache_usage', FloatType(), True),
                         StructField('page_cache_usage', FloatType(), True),
                         StructField('max_mem_usage', FloatType(), True),
                         StructField('mean_diskIO_time', FloatType(), True),
                         StructField('mean_local_disk_space', FloatType(), True),
                         StructField('max_cpu_usage', FloatType(), True),
                         StructField('max_disk_io_time', FloatType(), True),
                         StructField('cpi', FloatType(), True),
                         StructField('mai', FloatType(), True),
                         StructField('sampling_portion', FloatType(), True),
                         StructField('agg_type', FloatType(), True),
                         StructField('sampled_cpu_usage', FloatType(), True)])
numberOfJob = 0
for jobid in JobIdArr:
    numberOfJob += 1
    if jobid == 6336594489 or jobid == 2902878580:
        break;
    
    if numberOfJob <= 1990: 
        for file_name in os.listdir(folder_path):
            df = (
                sql_context.read
                .format('com.databricks.spark.csv')
                .schema(dataSchema)
                .load("%s%s"%(folder_path,file_name))
            )
            df.createOrReplaceTempView("dataFrame")
            sumCPUUsage = sql_context.sql("SELECT JobId, machineId, startTime, endTime, meanCPUUsage,AssignMem,mean_diskIO_time,mean_local_disk_space   from dataFrame where Jobid = '%s' " %jobid)
            # sumCPUUsage.show(5000)"
            schema_df = ["JobId","machineId" ,"startTime", "endTime", "meanCPUUsage","AssignMem","mean_diskIO_time","mean_local_disk_space"]
            sumCPUUsage.toPandas().to_csv('/home/hunter/spark/spark-2.2.0-bin-hadoop2.7/thangbk2209/JobId_time/%s_%s'%(jobid, file_name), index=False, header=None)
            # sumCPUUsage.write.save("results/test.csv", format="csv", columns=schema_df)
sc.stop()

from pyspark.sql.session import SparkSession as spark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from matplotlib import pyplot
import os
sc = SparkContext(appName="Task_usage")
sql_context = SQLContext(sc)

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

# for file_name in os.listdir(folder_path):
df = (
    sql_context.read
    .format('com.databricks.spark.csv')
    .schema(dataSchema)
    .load("%s"%(folder_path))
)
df.createOrReplaceTempView("dataFrame")
# df.printSchema()
for i in range(len(startTimeArr)):
    start = startTimeArr[i][0]
    print start
    sumCPUUsage = sql_context.sql("SELECT startTime/1000000 , endTime/1000000, JobId, taskIndex, machineId, meanCPUUsage,AssignMem,mean_diskIO_time,mean_local_disk_space from dataFrame where startTime=%s order by startTime/1000000 ASC"%(start))

    schema_df = ["startTime","endTime", "JobId","TaskIndex","machineId","meanCPU","meanMemory","AssignMem","mean_diskIO","mean_local_disk_space"]
    sumCPUUsage.toPandas().to_csv('thangbk2209/task_usage/%s.csv'%(start), index=False, header=None)

sc.stop()
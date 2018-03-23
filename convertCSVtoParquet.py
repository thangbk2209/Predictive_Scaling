from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import os

if __name__ == "__main__":
    sc = SparkContext(appName="CSV2Parquet")
    sqlContext = SQLContext(sc)
    folder_path = '/home/hunter/GoogleCluster/task_usage_extract/'
    schema = StructType([StructField('startTime', StringType(), True),
                         StructField('endTime', StringType(), True),
                         StructField('JobId', StringType(), True),
                         StructField('taskIndex', StringType(), True),
                         StructField('machineId', StringType(), True),
                         StructField('meanCPUUsage', StringType(), True),
                         # canonical memory usage
                         StructField('CMU', StringType(), True),
                         # assigned memory usage
                         StructField('AssignMem', StringType(), True),
                         # unmapped page cache memory usage
                         StructField('unmapped_cache_usage', StringType(), True),
                         StructField('page_cache_usage', StringType(), True),
                         StructField('max_mem_usage', StringType(), True),
                         StructField('mean_diskIO_time', StringType(), True),
                         StructField('mean_local_disk_space', StringType(), True),
                         StructField('max_cpu_usage', StringType(), True),
                         StructField('max_disk_io_time', StringType(), True),
                         StructField('cpi', StringType(), True),
                         StructField('mai', StringType(), True),
                         StructField('sampling_portion', StringType(), True),
                         StructField('agg_type', StringType(), True),
                         StructField('sampled_cpu_usage', StringType(), True)])
    for file_name in os.listdir(folder_path):
        file_parquet_name = file_name.split('.')[0]
        rdd = sc.textFile("%s%s"%(folder_path,file_name)).map(lambda line: line.split(","))
        df = sqlContext.createDataFrame(rdd, schema)
        df.write.parquet('/home/hunter/GoogleCluster/task_usage_parquet/%s'%(file_parquet_name))
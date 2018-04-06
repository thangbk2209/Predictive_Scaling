
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
folder_path = '/home/hunter/GoogleCluster/job_events/'

dataSchema = StructType([StructField('timeStamp', LongType(), True),
                         StructField('missingInfo', StringType(), True),
                         StructField('JobId', LongType(), True),
                         StructField('eventType', LongType(), True),
                         StructField('userName', StringType(), True),
                         StructField('schedulingClass', LongType(), True),
                         # canonical memory usage
                         StructField('jobName', StringType(), True),
                         # assigned memory usage
                         StructField('localJobName', StringType(), True)])

for file_name in os.listdir(folder_path):
    df = (
        sql_context.read
        .format('com.databricks.spark.csv')
        .schema(dataSchema)
        .load("%s%s"%(folder_path,file_name))
    )
    df.createOrReplaceTempView("dataFrame")
    sumCPUUsage = sql_context.sql("SELECT distinct JobId from dataFrame")
    # sumCPUUsage.show(5000)
    schema_df = ["Jobid"]
    sumCPUUsage.toPandas().to_csv('thangbk2209/fullJobId/%s'%(file_name), index=False, header=None)
    # sumCPUUsage.write.save("results/test.csv", format="csv", columns=schema_df)
sc.stop()
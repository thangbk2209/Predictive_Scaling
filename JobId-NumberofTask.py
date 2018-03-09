

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
folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/Jobid-NumberOfTask/'

dataSchema = StructType([StructField('JobId', LongType(), True),
                         StructField('taskIndex', LongType(), True))
file_name = 'JobID-numberOfTask.csv'
df = (
    sql_context.read
    .format('com.databricks.spark.csv')
    .schema(dataSchema)
    .load("%s%s"%(folder_path,file_name))
)
df.createOrReplaceTempView("dataFrame")
sumCPUUsage = sql_context.sql("SELECT JobId, sum(taskIndex) from dataFrame group by Jobid")
# sumCPUUsage.show(5000)
schema_df = ["Jobid","numberOfTaskIndex"]
sumCPUUsage.toPandas().to_csv('thangbk2209/Predictive_Scaling/results/%s'%(file_name), index=False, header=None)
# sumCPUUsage.write.save("results/test.csv", format="csv", columns=schema_df)
sc.stop()

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
folder_path = '/home/hunter/GoogleCluster/machine_events/'

dataSchema = StructType([StructField('timeStamp', StringType(), True),
                         StructField('machineId', StringType(), True),
                         StructField('eventType', StringType(), True),
                         StructField('platformId', StringType(), True),
                         StructField('cpu', FloatType(), True),
                         StructField('memory', FloatType(), True)])
file_name = 'machine_events.csv'
df = (
    sql_context.read
    .format('com.databricks.spark.csv')
    .schema(dataSchema)
    .load("%s%s"%(folder_path,file_name))
)
df.createOrReplaceTempView("dataFrame")
sumCPUUsage = sql_context.sql("SELECT distinct machineId from dataFrame")
# sumCPUUsage.show(5000)
schema_df = ["machineId"]
sumCPUUsage.toPandas().to_csv('thangbk2209/Predictive_Scaling/results/machineId.csv', index=False, header=None)
# sumCPUUsage.write.save("results/test.csv", format="csv", columns=schema_df)
sc.stop()
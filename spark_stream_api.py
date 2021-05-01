import findspark
findspark.init("/opt/manual/spark")

import sys
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.streaming import StreamingContext
from pyspark.ml.classification import RandomForestClassificationModel
spark = (SparkSession.builder
.appName("Write to Kafka")
.getOrCreate())

# spark.sparkContext.setLogLevel('ERROR')
checkpoint_dir = "hdfs://localhost:9000/user/train/write_to_kafka"



lines = (spark
.readStream
.format("kafka")
.option("kafka.bootstrap.servers", "localhost:9092")
.option("subscribe", "topic1")
.load())


lines2 = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

model = RandomForestClassificationModel.load(os.path.join(sys.argv[1], 'homecredit_model'))

streamingQuery = (lines2
.writeStream
.format("kafka")
.option("kafka.bootstrap.servers", "localhost:9092")
.option("topic", "topic2")
.outputMode("append")
.option("checkpointLocation", checkpoint_dir)
.start())



streamingQuery.awaitTermination()

#!/bin/sh

####import data on hdfs from postgresql
sqoop import --connect jdbc:postgresql://localhost/homecredit \
--driver org.postgresql.Driver \
--username train --password Ankara06 \
--table application_train \
--m 1 \
--as-parquetfile \
--compress --compression-codec 'gzip' \
--target-dir "application_train"


####import data on hive from postgresql
sqoop import --connect jdbc:postgresql://localhost/homecredit \
--driver org.postgresql.Driver \
--username train --password Ankara06 \
--table previous_application --m 1 \
--hive-import --hive-overwrite  --hive-table homecredit.previous_application \
--target-dir /tmp/previous_application 

####read parquet file on hdfs
df2 = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.option("compression","gzip") \
.parquet("hdfs:///user/train/application_train")



####data generator kafka(producer)
## bootstrap_servers kaldırman gerekir
##--bootstrap_servers=['locahost:9092']  \


python dataframe_to_kafka.py --input "input/iris.csv" \
--sep="," --row_sleep_time 1 --source_file_extension="csv" \
--topic="topic1" \
--repeat 1 --shuffle False



####data generator kafka(consumer)

kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic topic2 --from-beginning


#### spark stream submit (kafka to kafka)(spark stream deployment)

spark-submit --master yarn --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 spark_stream.py

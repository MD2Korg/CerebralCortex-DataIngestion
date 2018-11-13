#!/usr/bin/env bash

#########################################################################################
############################ Environment Configs ########################################
#########################################################################################

# Python3 path
export PYSPARK_PYTHON=python3

# export CerebralCortex path if CerebralCortex is not installed
# export PYTHONPATH="${PYTHONPATH}:/home/ali/IdeaProjects/CerebralCortex-2.0/"

# Update path to libhdfs.so if it's different than /usr/local/hadoop/lib/native/libhdfs.so
# uncooment it if using HDFS as NoSQl storage
#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/hadoop/lib/native/libhdfs.so

#Spark path, uncomment if spark home is not exported else where.
# export SPARK_HOME=/home/ali/spark/spark-2.2.1-bin-hadoop2.7/

#PySpark args (do not change unless you know what you are doing)
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0"

#set spark home, uncomment if spark home is not exported else where.
# export PATH=$SPARK_HOME/bin:$PATH


#########################################################################################
############################ YAML Config Paths and other configs ########################
#########################################################################################

# Provide a comma separated participants UUIDs. All participants' data will be processed if no UUIDs is provided.
PARTICIPANTS=""

# directory path where all the CC configurations are stored
CONFIG_DIRECTORY="/cc_conf/"

# spark master. This will work on local machine only. In case of cloud, provide spark master node URL:port.
SPARK_MASTER="local[*]"

# add -p $PARTICIPANTS at the end of below command if participants' UUIDs are provided
spark-submit --conf spark.streaming.kafka.maxRatePerPartition=10 $PYSPARK_SUBMIT_ARGS main.py -c $CONFIG_DIRECTORY

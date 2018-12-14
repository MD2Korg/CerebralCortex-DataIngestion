#!/usr/bin/env bash

#########################################################################################
############################ Environment Configs ########################################
#########################################################################################

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6

# export CerebralCortex path if CerebralCortex is not installed
#export PYTHONPATH="${PYTHONPATH}:/cerebralcortex/code/ali/CerebralCortex/"

# Update path to libhdfs.so if it's different than /usr/local/hadoop/lib/native/libhdfs.so
# uncooment it if using HDFS as NoSQl storage
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/hadoop/lib/native/libhdfs.so

#Spark path, uncomment if spark home is not exported else where.
#export SPARK_HOME=/home/ali/spark/spark-2.2.1-bin-hadoop2.7/

#PySpark args (do not change unless you know what you are doing)
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 pyspark-shell"

#set spark home, uncomment if spark home is not exported else where.
export PATH=$SPARK_HOME/bin:$PATH


#########################################################################################
############################ YAML Config Paths and other configs ########################
#########################################################################################

# Provide a comma separated participants UUIDs. All participants' data will be processed if no UUIDs is provided.
PARTICIPANTS="7dd2da22-ce55-4b9c-bbeb-04207f849023,81191356-d2ed-420c-a268-283f0df4e3c1,264df5d9-2a44-41e4-81e2-33def40b36b6,c3aa1a67-7341-4a71-8684-5543a0311c9d,6a6451f0-5ca7-483b-b574-72b95fae96d8,d401e56b-18e5-466d-989a-ddc02946a2fd,469d0511-aa6c-4823-bab1-dde54573af20,b1136a9b-1923-4d47-b95b-64253a5200d3,11bc79c5-0bb8-45c6-bd7b-92d06cc70cae,6d6f83a8-c6bc-47eb-b667-59fa5b916528,5218df9c-32a3-4328-9381-3c6ed7e7757f,fcffa88e-ebf7-4cbf-9968-46c1812dc93e,bdb34c90-7dd7-48dd-abbb-fa522f3e5d12,c694a514-8fc7-44f8-a8db-5b6767f1cb13,e4e418a2-e383-4254-abee-170a8e9b5c0f,9ce23450-5007-4fdd-822b-95f9580c21cc,255771c3-0467-48ea-a07b-b5491654f1a1,83d73401-2f8c-43a6-b5ae-a47b3d43ed8c,3d02d44a-56e2-4e02-adfc-0015a40159aa,3022f2d1-5170-4e00-9d21-4480067c9f5e,2d92c4a2-e0fe-443a-b334-20d4ef9e25a9,5accb145-3712-4321-8fff-aaf0ac294c7f,d66bb458-9e8f-4d73-98d1-120b70900f0f,a77111a8-2554-4c45-b405-7e03cd11d627,7eb2028b-8322-44ee-931e-cc2b97e27d1f,2cb6a410-2081-460e-84af-ab005fdd4879,700df92b-7204-400c-898f-c8777e8e8334,5ebbcd27-31e3-4d4b-99d5-083187c68742"

# directory path where all the CC configurations are stored
CONFIG_DIRECTORY="/cerebralcortex/code/config/cc_2_4/"

# spark master. This will work on local machine only. In case of cloud, provide spark master node URL:port.
SPARK_MASTER="spark://dagobah10dot.memphis.edu:7077"
SPARK_UI_PORT=4087

PY_FILES="/cerebralcortex/code/ali/CerebralCortex/dist/MD2K_Cerebral_Cortex-2.4.0-py3.6.egg,dist/MD2K_CerebralCortex_DataIngestion-2.4.0-py3.6.egg"

python3.6 setup.py bdist_egg

# add -pa $PARTICIPANTS at the end of below command if participants' UUIDs are provided
spark-submit --master $SPARK_MASTER --conf spark.ui.port=$SPARK_UI_PORT --total-executor-cores 36 --conf spark.streaming.kafka.maxRatePerPartition=10 --driver-memory 1g --executor-memory 1g --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 --py-files $PY_FILES main.py -c $CONFIG_DIRECTORY -pa $PARTICIPANTS
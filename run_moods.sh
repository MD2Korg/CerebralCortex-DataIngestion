#!/usr/bin/env bash


base_path=$1

#########################################################################################
############################ Environment Configs ########################################
#########################################################################################

# Python3 path
export PYSPARK_PYTHON=/cerebralcortex/kessel_jupyter_virtualenv/cc3_high_performance/bin/python3.6

export PYSPARK_DRIVER_PYTHON=/cerebralcortex/kessel_jupyter_virtualenv/cc3_high_performance/bin/python3.6

# export CerebralCortex path if CerebralCortex is not installed
#export PYTHONPATH="${PYTHONPATH}:/cerebralcortex/code/ali/CerebralCortex/"

# Update path to libhdfs.so if it's different than /usr/local/hadoop/lib/native/libhdfs.so
# uncooment it if using HDFS as NoSQl storage
export LD_LIBRARY_PATH="/usr/local/hadoop/lib/native/libhdfs.so"

#Spark path, uncomment if spark home is not exported else where.
#export SPARK_HOME=/home/ali/spark/spark-2.2.1-bin-hadoop2.7/

#set spark home, uncomment if spark home is not exported else where.
#export PATH=$SPARK_HOME/bin:$PATH

export HADOOP_COMMON_HOME="/usr/local/hadoop"
export HADOOP_COMMON_LIB_NATIVE_DIR="/usr/local/hadoop/lib/native"
export HADOOP_HDFS_HOME="/usr/local/hadoop"
export HADOOP_HOME="/usr/local/hadoop"
export HADOOP_INSTALL="/usr/local/hadoop"
export HADOOP_MAPRED_HOME="/usr/local/hadoop"
export SPARK_HOME="/usr/local/spark"

#########################################################################################
############################ YAML Config Paths and other configs ########################
#########################################################################################

# directory path where all the CC configurations are stored
CONFIG_DIRECTORY="/cerebralcortex/code/config/cc3_moods_conf/"
DAY_TO_PROCESS=`date '+%Y%m%d'` # MMDDYYYY format, single date or multiple dates as csv, 20200405,20200406
HOUR_TO_PROCESS=`date -d '1 hours ago' '+%H'` #, single date or multiple dates as csv, 1,2,3,4
#DAY_TO_PROCESS=20200828
#HOUR_TO_PROCESS=01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,00
BATCH_SIZE=200
STUDY_NAME="moods"

# spark master. This will work on local machine only. In case of cloud, provide spark master node URL:port.
#SPARK_MASTER="spark://dantooine10dot:7077"
SPARK_MASTER="local[5]"
SPARK_UI_PORT=4087

PY_FILES="$base_path/eggs/cerebralcortex_data_ingestion-3.2.0-py3.6.egg"
VIRTUAL_ENV="--conf spark.pyspark.virtualenv.enabled=true  --conf spark.pyspark.virtualenv.type=native --conf spark.pyspark.virtualenv.requirements=/cerebralcortex/kessel_jupyter_virtualenv/requirements.txt --conf spark.pyspark.virtualenv.bin.path=/cerebralcortex/kessel_jupyter_virtualenv/cc3_high_performance/bin --conf spark.pyspark.python=/cerebralcortex/kessel_jupyter_virtualenv/cc3_high_performance/bin/python3.6"

#spark-submit --master $SPARK_MASTER $VIRTUAL_ENV --conf spark.ui.port=$SPARK_UI_PORT --total-executor-cores 50 --driver-memory 10g --executor-memory 1g --py-files $PY_FILES main.py -c $CONFIG_DIRECTORY -bs $BATCH_SIZE -dy $DAY_TO_PROCESS -hr $HOUR_TO_PROCESS -sn $STUDY_NAME
/usr/local/bin/spark-submit --master $SPARK_MASTER $VIRTUAL_ENV --conf spark.ui.port=$SPARK_UI_PORT --total-executor-cores 5 --driver-memory 10g --executor-memory 1g --py-files $PY_FILES "$base_path/main.py" -c $CONFIG_DIRECTORY -bs $BATCH_SIZE -dy $DAY_TO_PROCESS -hr $HOUR_TO_PROCESS -sn $STUDY_NAME

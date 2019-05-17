# Copyright (c) 2017, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from cerebralcortex.core.util.spark_helper import get_or_create_sc
from pyspark.streaming import StreamingContext
from cerebralcortex.core.config_manager.config import Configuration
from cerebralcortex.kernel import Kernel
from core.messaging_service.process_messages import kafka_msg_to_db, mysql_batch_to_db
from core.messaging_service.kafka_consumer import spark_kafka_consumer
import argparse


def run():
    parser = argparse.ArgumentParser(description='CerebralCortex Data Ingestion Pipeline.')
    parser.add_argument("-c", "--config_dir", help="Configurations directory path.", required=True)


    args = vars(parser.parse_args())

    config_dir_path = str(args["config_dir"]).strip()

    # data ingestion configurations
    ingestion_config = Configuration(config_dir_path, "data_ingestion.yml").config

    ping_kafka = ingestion_config["ping_kafka"]
    data_path = ingestion_config["data_ingestion"]["data_dir_path"]

    # Kafka Consumer Configs
    spark_context = get_or_create_sc(type="sparkContext")
    spark_context.setLogLevel("WARN")

    CC = Kernel(config_dir_path)

    if CC.config["messaging_service"]=="none":
        raise Exception("Messaging service is disabled (none) in cerebralcortex.yml. Please update configs.")

    try:
        ping_kafka = int(ping_kafka)
    except:
        raise Exception("ping_kafka value can only be an integer. Please check data_ingestion.yml")

    ssc = StreamingContext(spark_context, ping_kafka)
    kafka_files_stream = CC.create_direct_kafka_stream(["filequeue"], ssc)
    if kafka_files_stream is not None:
        kafka_files_stream.foreachRDD(lambda rdd: kafka_msg_to_db(rdd, data_path, config_dir_path, ingestion_config, CC))

    ssc.start()
    ssc.awaitTermination()




if __name__ == "__main__":
    run()

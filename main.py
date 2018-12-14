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
from cerebralcortex.cerebralcortex import CerebralCortex
from core.messaging_service.process_messages import kafka_msg_to_db, mysql_batch_to_db
from core.messaging_service.kafka_consumer import spark_kafka_consumer
import argparse


def run():
    parser = argparse.ArgumentParser(description='CerebralCortex Kafka Message Handler.')
    parser.add_argument("-c", "--config_dir", help="Configurations directory path.", required=True)
    parser.add_argument("-pa", "--participants",
                        help="Provide a comma separated participants UUIDs. All participants' data will be processed if no UUIDs is provided.", default="",
                        required=False)

    args = vars(parser.parse_args())

    config_dir_path = str(args["config_dir"]).strip()
    participants = args["participants"]

    # data ingestion configurations
    ingestion_config = Configuration(config_dir_path, "data_ingestion.yml").config


    mydb_batch_size = ingestion_config["mysql_batch_size"]

    ping_kafka = ingestion_config["ping_kafka"]
    data_path = ingestion_config["data_ingestion"]["data_dir_path"]
    ingestion_type = ingestion_config["data_ingestion"]["type"]

    # Kafka Consumer Configs
    spark_context = get_or_create_sc(type="sparkContext")
    spark_context.setLogLevel("WARN")

    CC = CerebralCortex(config_dir_path)

    if ingestion_type=="mysql":
        all_days = CC.SqlData.get_all_data_days()
        for day in all_days:
            print("Processing day:", day)
            for replay_batch in CC.SqlData.get_replay_batch(day=day, record_limit=mydb_batch_size, nosql_blacklist=ingestion_config["nosql_blacklist"]):
                new_replay_batch = []
                if participants=="all" or participants=="":
                    new_replay_batch = replay_batch
                else:
                    selected_participants = list(filter(None, participants.split(",")))
                    for rb in replay_batch:
                        if rb["owner_id"] in selected_participants:
                            new_replay_batch.append(rb)
                mysql_batch_to_db(spark_context, new_replay_batch, data_path, config_dir_path, ingestion_config)

    else:
        if CC.config["messaging_service"]=="none":
            raise Exception("Messaging service is disabled (none) in cerebralcortex.yml. Please update configs.")

        try:
            ping_kafka = int(ping_kafka)
        except:
            raise Exception("ping_kafka value can only be an integer. Please check data_ingestion.yml")
        consumer_group_id = "md2k-test"
        broker = str(CC.config["kafka"]["host"])+":"+str(CC.config["kafka"]["port"])
        ssc = StreamingContext(spark_context, ping_kafka)
        kafka_files_stream = spark_kafka_consumer(["filequeue"], ssc, broker, consumer_group_id, CC)
        if kafka_files_stream is not None:
            kafka_files_stream.foreachRDD(lambda rdd: kafka_msg_to_db(rdd, data_path, config_dir_path, ingestion_config, CC))

        ssc.start()
        ssc.awaitTermination()


if __name__ == "__main__":
    run()

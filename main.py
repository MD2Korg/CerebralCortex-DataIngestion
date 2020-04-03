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

from datetime import datetime, timedelta
from core.data_scanner.raw_data_scanner import get_files_list
from core.util.config_parser import get_configs
from core.file_processor.process_msgpack import msgpack_to_pandas
import argparse
import gzip
from core.util.spark_helper import get_or_create_sc
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def save_data(msg, study_name, cc_config):
    files = msg.get("files")
    data = pd.DataFrame()
    for f in files:
        with gzip.open(msg.get("file_path")+"/"+f, 'rb') as input_data:
            pdf = msgpack_to_pandas(input_data)
        data = data.append(pdf, ignore_index=True)
    hdfs_ip = cc_config['hdfs']['host']
    hdfs_port = cc_config['hdfs']['port']
    raw_files_dir = cc_config['hdfs']['raw_files_dir']
    if raw_files_dir[-1:]!="/":
        raw_files_dir = raw_files_dir+"/"

    hdfs_url = raw_files_dir+"study="+study_name+"/stream"+msg.get("stream_name")+"/version="+msg.get("version")+"/user="+msg.get("user_id")+"/"
    try:
        table = pa.Table.from_pandas(data, preserve_index=False)
        fs = pa.hdfs.connect(hdfs_ip, hdfs_port)
        pq.write_to_dataset(table, root_path=hdfs_url, filesystem=fs)
        return True
    except Exception as e:
        raise Exception("Cannot store dataframe: " + str(e))

def run():
    parser = argparse.ArgumentParser(description='CerebralCortex Kafka Message Handler.')
    parser.add_argument("-c", "--config_dir", help="Configurations directory path.", required=True)
    parser.add_argument("-dy", "--day", help="Day date to be processed. Format is MMDDYYYY.", required=True)
    parser.add_argument("-sn", "--study_name",
                        help="Provide a study_name.",
                        default="default",
                        required=False)
    parser.add_argument("-stn", "--stream_names",
                        help="Provide a comma separated stream_names. All stream_names data will be processed if no name is provided.", default=[],
                        required=False)
    parser.add_argument("-uid", "--user_ids",
                        help="Provide a comma separated participants UUIDs. All participants' data will be processed if no UUIDs is provided.",
                        default=[],
                        required=False)
    parser.add_argument("-vr", "--versions",
                        help="Provide a comma separated versions. All versions data will be processed if no version is provided.",
                        default=[],
                        required=False)


    args = vars(parser.parse_args())

    config_dir_path = str(args["config_dir"]).strip()
    study_name = args["study_name"]
    day = args["day"]
    stream_names = args["stream_names"]
    user_ids = args["user_ids"]
    versions = args["versions"]

    ingestion_config = get_configs(config_dir_path, "data_ingestion.yml")
    cc_config = get_configs(config_dir_path, "cerebralcortex.yml")
    raw_data_path = ingestion_config["data_ingestion"]["raw_data_path"]

    files_list = get_files_list(raw_data_path=raw_data_path, study_name=study_name, day=day, stream_names=stream_names, batch_size=2,
                   user_ids=user_ids, versions=versions)
    for files in files_list:

        spark_context = get_or_create_sc()

        message = spark_context.parallelize(files)
        message.foreach(lambda msg: save_data(msg, cc_config))
        print("File Iteration count:", len(files))

if __name__ == "__main__":
    run()

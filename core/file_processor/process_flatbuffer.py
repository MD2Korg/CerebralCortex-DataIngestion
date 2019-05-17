# Copyright (c) 2018, MD2K Center of Excellence
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

import msgpack
import fastparquet
import pandas
import json
import uuid
import os
from deepdiff import DeepDiff
from flask import request
from flask_restplus import Namespace, Resource
from datetime import datetime
import traceback
import gzip


class FlatbufferToDB():
    '''This class is responsible to read flatbuffer files, convert them infot parquet format and insert data in HDFS and/or Influx.'''

    def convert_to_parquet(input_data):
        result = []

        unpacker = msgpack.Unpacker(input_data, use_list=False, raw=False)
        for unpacked in unpacker:
            result.append(list(unpacked))

        return result


    def create_dataframe(data):
        header = data[0]
        data = data[1:]

        if data is None:
            return None
        else:
            df = pandas.DataFrame(data, columns=header)
            df.Timestamp = pandas.to_datetime(df['Timestamp'], unit='us')
            df.Timestamp = df.Timestamp.dt.tz_localize('UTC')
            return df


    def write_parquet(df, file, compressor=None, append=False):
        fastparquet.write(file, df, len(df), compression=compressor, append=append)

    def file_processor(self, msg: dict, zip_filepath: str, influxdb_insert: bool = False, nosql_insert: bool = True):

        """
        Process raw .gz files' data and json metadata files, convert into DataStream object format and store data in CC data-stores
        :param msg: Kafka message in json format
        :param zip_filepath: data folder path where all the gz/json files are located
        :param influxdb_insert: Turn on/off influxdb data ingestion
        :param nosql_insert: Turn on/off nosql data ingestion

        """

        # TODO: Implement flatbuffer parser

        # Save metadata to MySQL
        # self.sql_data.save_stream_metadata(stream_id, name, owner, data_descriptor, execution_context,
        #                                        annotations, stream_type, nosql_data[0].start_time,
        #                                        nosql_data[len(nosql_data) - 1].start_time)

        raise Exception("Flatbuffer is not implemented yet.")
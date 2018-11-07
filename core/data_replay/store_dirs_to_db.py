# Copyright (c) 2018, MD2K Center of Excellence
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


import argparse
import gzip
import json
import os
from datetime import datetime

from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.config_manager.config import Configuration


class ReplayCerebralCortexData:
    def __init__(self, selected_participants, table_name, CC, ingestion_config):
        """
        Constructor
        :param configuration:
        """

        self.CC = CC
        self.table_name = table_name
        self.data_dir = ingestion_config["data_ingestion"]["data_dir_path"]
        self.selected_participants = list(filter(None, selected_participants.split(",")))
        self.lab_participants = []
        self.read_data_dir()

    def read_data_dir(self):
        for stream_dir in os.scandir(self.data_dir):
            if stream_dir.is_dir():
                owner = stream_dir.path[-36:]
                if owner in self.selected_participants:
                    self.scan_stream_dir(stream_dir)

    def scan_stream_dir(self, stream_dir1):
        for day_dir in os.scandir(stream_dir1.path):
            if day_dir.is_dir():
                for stream_dir in os.scandir(day_dir):
                    if stream_dir.is_dir():
                        stream_dir = stream_dir.path
                        tmp = stream_dir.split("/")[-3:]
                        owner_id = tmp[0]
                        day = tmp[1]
                        stream_id = tmp[2]
                        files_list = []
                        dir_size = 0
                        for f in os.listdir(stream_dir):
                            if f.endswith(".gz"):
                                new_filename = (stream_dir + "/" + f).replace(self.data_dir, "")
                                files_list.append(new_filename)
                                dir_size += os.path.getsize(stream_dir + "/" + f)
                        metadata_filename = self.data_dir + files_list[0].replace(".gz", ".json")
                        with open(metadata_filename, 'r') as mtd:
                            metadata = mtd.read()
                        metadata = json.loads(metadata)
                        stream_name = metadata["name"]
                        self.CC.SqlData.add_to_data_replay_table(self.table_name, owner_id, stream_id, stream_name, day,
                                                                 files_list, dir_size, metadata)

    def read_json_file(self, filename):
        with open(filename, "r") as jfile:
            metadata = jfile.read()
        return json.loads(metadata)

    def read_gz_file(self, filename):
        with gzip.open(filename, "r") as gzfile:
            for line in gzfile:
                day = line.decode("utf-8").split(",")[0]
                day = datetime.fromtimestamp(int(day) / 1000)
                day = day.strftime('%Y%m%d')
                break
        return day


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='CerebralCortex Data Replay')
    parser.add_argument('-participants', '--participants',
                        help='Scan all users directories or only for the list provided in the script.', type=str,
                        default="all", required=False)
    parser.add_argument('-c', '--config_dir', help='Configuration directory path', required=True)

    args = vars(parser.parse_args())

    config_dir_path = str(args["config_dir"]).strip()
    selected_participants = str(args["participants"]).strip()

    CC = CerebralCortex(config_dir_path)
    ingestion_config = Configuration(config_dir_path, "data_ingestion.yml").config
    table_name = "data_replay"

    ReplayCerebralCortexData(selected_participants, table_name, CC, ingestion_config)

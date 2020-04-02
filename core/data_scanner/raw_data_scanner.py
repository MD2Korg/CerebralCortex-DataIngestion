# Copyright (c) 2019, MD2K Center of Excellence
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

import argparse
import gzip
import json
import os
from datetime import datetime


def get_files_list(raw_data_path, study_name, stream_names=[], versions=[], user_ids=[], day=None):
    files_list = []
    if day is None:
        raise Exception("Day parameter is missing.")

    if raw_data_path[-1:]!="/":
        raw_data_path = raw_data_path+"/"

    raw_data_path = raw_data_path+"study="+study_name
    for stream_dir in os.scandir(raw_data_path):
        if stream_dir.is_dir():
            if len(stream_names) > 0 and stream_dir.name.replace("stream=", "") not in stream_names:
                continue
            for version_dir in os.scandir(stream_dir.path):
                if version_dir.is_dir():
                    if len(versions) > 0 and int(version_dir.name.replace("version=", "")) not in versions:
                        continue
                    for user_dir in os.scandir(version_dir.path):
                        if user_dir.is_dir():
                            if len(user_ids) > 0 and user_dir.name.replace("user=", "") not in user_ids:
                                continue
                            for day_dir in os.scandir(user_dir.path):
                                if day_dir.name==day:
                                    files_list.append({"stream_name":stream_dir.name, "user_id": user_dir.name, "version": version_dir.name,"file_path":day_dir.path, "files":os.listdir(day_dir.path)})
    return files_list

# dd = get_files_list(raw_data_path="/home/ali/IdeaProjects/MD2K_DATA/CC_APISERVER_DATA/raw/", study_name="default", day="310320")
# print(dd)
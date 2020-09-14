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
import pandas as pd

def msgpack_to_pandas(input_data: object) -> pd.DataFrame:
    """
    Convert msgpack binary file into pandas dataframe

    Args:
        input_data (msgpack): msgpack data file

    Returns:
        dataframe: pandas dataframe

    """
    data = []

    unpacker = msgpack.Unpacker(input_data, use_list=False, raw=False)
    for unpacked in unpacker:
        data.append(list(unpacked))

    header = data[0]
    data = data[1:]

    if data is None:
        return None
    else:
        df = pd.DataFrame(data, columns=header)
        df.columns = df.columns.str.lower()
        df.timestamp = pd.to_datetime(df['timestamp'], unit='us')
        df.timestamp = df.timestamp.dt.tz_localize('UTC')
        df.localtime = pd.to_datetime(df['localtime'], unit='us')
        df.localtime = df.localtime.dt.tz_localize('UTC')
        return df
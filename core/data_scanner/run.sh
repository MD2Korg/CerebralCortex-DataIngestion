#!/usr/bin/env bash

# Provide a comma separated participants UUIDs. All participants' data will be processed if no UUIDs is provided.
PARTICIPANTS=""

# Only one day data for all particpants will be scanned if day is provided. Day format is YYYYMMDD (e.g., 20180517)
DAY=""

# directory path where all the CC configurations are stored
CONFIG_DIRECTORY="/cerebralcortex/code/config/cc_2_4/"

python3 store_dirs_to_db.py -c $CONFIG_DIRECTORY -participants $PARTICIPANTS
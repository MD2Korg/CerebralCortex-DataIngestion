#!/usr/bin/env bash

# Provide a comma separated participants UUIDs. All participants' data will be processed if no UUIDs is provided.
PARTICIPANTS=""

# directory path where all the CC configurations are stored
CONFIG_DIRECTORY="/home/vagrant/CerebralCortex-DockerCompose/cc_config_file/"


python3 store_dirs_to_db.py -c $CONFIG_DIRECTORY -participants $PARTICIPANTS
import yaml
import os


def load_file(filepath: str):
    """
    Helper method to load a yaml file
    :param config_dir_path:

    Args:
        filepath (str): path to a yml configuration file
    """

    with open(filepath, 'r') as ymlfile:
        config = yaml.load(ymlfile)

    if "hdfs" in config and config["hdfs"]["raw_files_dir"] != "" and config["hdfs"]["raw_files_dir"][
        -1] != "/":
        config["hdfs"]["raw_files_dir"] += "/"

    if "filesystem" in config and config["filesystem"]["filesystem_path"] != "" and \
            config["filesystem"]["filesystem_path"][-1] != "/":
        config["filesystem"]["filesystem_path"] += "/"

    if "object_storage" in config and config["object_storage"]["object_storage_path"] != "" and \
            config["object_storage"]["object_storage_path"][-1] != "/":
        config["object_storage"]["object_storage_path"] += "/"

    if "data_dir" in config and config["data_dir"] != "" and config["data_dir"][-1] != "/":
        config["data_dir"] += "/"

    if "log_files_path" in config and config["cc"]["log_files_path"] != "" and \
            config["cc"]["log_files_path"][-1] != "":
        config["cc"]["log_files_path"] += "/"
        if not os.access(config["cc"]["log_files_path"], os.W_OK):
            raise Exception(config["cc"][
                                "log_files_path"] + " path is not writable. Please check your cerebralcortex.yml configurations for 'log_files_path'.")
    return config

def get_configs(config_dir, config_file_name):
    if config_dir[-1] != "/":
        config_dir += "/"

    config_filepath = config_dir + config_file_name

    if not os.path.exists(config_filepath):
        raise Exception(
            config_filepath + " does not exist. Please check configuration directory path and configuration file name.")

    if config_dir is not None:
        config = load_file(config_filepath)
    else:
        config = None

    return config
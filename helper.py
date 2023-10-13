import configparser
import logging
import os
import sys

# function to read configurations.ini file
def read_config():
    config = configparser.ConfigParser()
    config.read('configurations.ini')
    return config

# function to create a logger
def create_logger():
    config = read_config()
    log_file_path = config['Logger']['LogFilePath']
    log_file_name = config['Logger']['LogFileName']
    log_level = config['Logger']['LogLevel']
    log_file = log_file_path + log_file_name
    logging.basicConfig(format='%(asctime)s, %(name)s %(levelname)s %(message)s',
                        level=log_level,
                        handlers=[logging.FileHandler(log_file, mode='w'),
                                  logging.StreamHandler(sys.stdout)])
    return logging
    
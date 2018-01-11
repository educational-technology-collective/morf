# Copyright (c) 2018 The Regents of the University of Michigan
# and the University of Pennsylvania
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Functions for working with (reading writing, modifying) MORF configuration files.
"""

import configparser
import fileinput
import os
from morf.utils import get_bucket_from_url, get_key_from_url


def get_config_properties(config_file ="config.properties"):
    """
    Returns the list of properties as a dict of key/value pairs in the file config.properties.
    :return: A flat (no sections) Python dictionary of properties.
    """
    cf = configparser.ConfigParser()
    try:
        cf.read(config_file)
    except Exception as e:
        print("[ERROR] exception {} reading configurations from file {}".format(e, config_file))
    properties = {}
    for section in cf.sections():
        for item in cf.items(section):
            properties[item[0]] = item[1]
    return properties


def combine_config_files(*args, outfile = "config.properties"):
    """
    Combine multiple config files into single config file located at outfile.
    :param args: names of config files to combine.
    :param outfile: pathname to write to.
    :return: None
    """
    with open(outfile, "w") as fout, fileinput.input(args) as fin:
        for line in fin:
            fout.write(line)
    return


def update_config_fields_in_section(section, config_file = "config.properties", **kwargs):
    """
    Overwrite (or create, if not exists) fields in section of config_file with values provided according to kwargs.
    :param section: section header within config file which contains the field to be modified.
    :param kwargs: named parameters, with values, to overwrite.
    :param config_file: path to config properties; should be valid ConfigParser file
    :return:
    """
    cf = configparser.ConfigParser()
    try:
        cf.read(config_file)
    except Exception as e:
        print("[ERROR] exception {} reading configurations from file {}".format(e, config_file))
    cf_new = configparser.ConfigParser()
    for section in cf.sections():
        for item in cf.items(section):
            try:
                cf_new[section][item[0]] = item[1]
            except KeyError: # section doesn't exist yet
                cf_new[section] = {}
                cf_new[section][item[0]] = item[1]
    for key, value in kwargs.items():
        try:
            cf_new[section][key] = value
        except KeyError:
            print("[ERROR] error updating config file: {}; possibly attempted to update a section that does not exist".format(e))
    try:
        os.remove(config_file)
        with open(config_file, "w") as cfwrite:
            cf_new.write(cfwrite)
    except Exception as e:
        print("[ERROR] error updating config file: {}".format(e))
    return


def fetch_data_buckets_from_config(config_file = "config.properties", data_section = 'data', required_bucket_dir_name = 'morf-data/'):
    """
    Fetch the buckets from data_section of config_file; warn if key does not exactle match directory_name.
    :param config_file: path to config file.
    :param data_section: section of config file with key-value pairs representing institution names and s3 paths.
    :param required_bucket_dir_name: directory or path that should match ALL values in data_section; if not, throws warning.
    :return: list of buckets to iterate over; no directories are returned because these should be uniform across all of the buckets.
    """
    cf = configparser.ConfigParser()
    cf.read(config_file)
    buckets = []
    for item in cf.items(data_section):
        item_url = item[1]
        bucket = get_bucket_from_url(item_url)
        dir = get_key_from_url(item_url)
        if dir != required_bucket_dir_name:
            msg = "[ERROR]: specified path {} does not match required directory name {}; change name of directories to be consistent or specify the correct directory to check for.".format(item_url, required_bucket_dir_name)
            print(msg)
            raise
        else:
            buckets.append(bucket)
    assert len(buckets) >= 1
    return buckets

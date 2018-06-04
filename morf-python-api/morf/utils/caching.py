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
Functions for caching data for MORF jobs.
"""

import os
import subprocess
import shutil
from urllib.parse import urlparse
import logging
from morf.utils.docker import load_docker_image
from morf.utils.log import set_logger_handlers, execute_and_log_output

module_logger = logging.getLogger(__name__)

def cache_s3_to_local(job_config, bucket):
    """
    Cache all data in an s3 bucket to job_config.cache_dir, creating a complete copy of files and directory structure.
    :param job_config: MorfJobConfig object.
    :param bucket: path to s3 bucket.
    :return:
    """
    logger = set_logger_handlers(module_logger, job_config)
    s3bucket = "s3://{}".format(bucket)
    bucket_cache_dir = os.path.join(job_config.cache_dir, bucket)
    # create job_config.cache_dir directory if not exists
    if not os.path.exists(job_config.cache_dir):
        try:
            os.makedirs(job_config.cache_dir)
        except exception as e:
            logger.error("error creating cache: {}".format(e))
            raise
    # execute s3 sync command
    cmd = "{} s3 sync {} {}".format(job_config.aws_exec, s3bucket, bucket_cache_dir)
    logger.info("running {}".format(cmd))
    try:
        subprocess.call(cmd, shell=True)
    except Exception as e:
        logger.warning("exception when executing sync: {}".format(e))
    return


def update_morf_job_cache(job_config):
    """
    Update the raw data cache using the parameters in job_config; if job_config contains multiple raw data buckets, cache all of them.
    :param job_config: MorfJobConfig object.
    :return:
    """
    # cache each bucket in a named directory within job_cache_dir
    for raw_data_bucket in job_config.raw_data_buckets:
        cache_s3_to_local(job_config, raw_data_bucket)
    return


def fetch_from_cache(job_config, cache_file_path, dest_dir):
    """
    Fetch a file from the cache for job_config into dest_dir, if it exists.
    :param job_config:
    :param cache_file_path: string, relative path to file in cache (this is identical to the directory path in s3; e.g. "/bucket/path/to/somefile.csv"
    :param dest_dir: absolute path of directory to fetch file into (will be created if not exists)
    :return: path to fetched file (string); return None if cache is not used.
    """
    logger = set_logger_handlers(module_logger, job_config)
    logger.info("fetching file {} from cache".format(cache_file_path))
    abs_cache_file_path = os.path.join(getattr(job_config, "cache_dir", None), cache_file_path)
    if hasattr(job_config, "cache_dir") and os.path.exists(abs_cache_file_path):
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        dest_fp = shutil.copy(abs_cache_file_path, dest_dir)
    else:
        logger.warning("file {} does not exist in cache".format(abs_cache_file_path))
        dest_fp = None
    return dest_fp


def docker_cloud_login(job_config):
    """
    Log into docker cloud using creds in job_config.
    :param job_config: MorfJobConfig object.
    :return: None
    """
    cmd = "docker login --username={} --password={}".format(job_config.docker_cloud_username, job_config.docker_cloud_password)
    logger = set_logger_handlers(module_logger, job_config)
    execute_and_log_output(cmd, logger)
    return


def docker_cloud_push(job_config, image_uuid):
    """
    Push image to Docker Cloud repo in job_config; tagging the image with its morf_id.
    :param job_config: MorfJobConfig object
    :param image_uuid: Docker image uuid
    :return: None
    """
    logger = set_logger_handlers(module_logger, job_config)
    docker_cloud_repo_and_tag_path = "{}:{}".format(job_config.docker_cloud_repo, job_config.morf_id)
    # tag the docker image using the morf_id
    tag_cmd = "docker tag {} {}".format(image_uuid, docker_cloud_repo_and_tag_path)
    execute_and_log_output(tag_cmd, logger)
    # push the image to docker cloud
    push_cmd = "docker push {}".format(docker_cloud_repo_and_tag_path)
    execute_and_log_output(push_cmd, logger)
    return docker_cloud_repo_and_tag_path


def cache_to_docker_hub(job_config, dir, image_name):
    """
    Push image to MORF repo in Docker Hub.
    :param job_config: MorfJobConfig object.
    :return: None
    """
    logger = set_logger_handlers(module_logger, job_config)
    image_uuid = load_docker_image(dir, job_config, logger, image_name)
    docker_cloud_login(job_config)
    docker_cloud_repo_and_tag_path = docker_cloud_push(job_config, image_uuid)
    return docker_cloud_repo_and_tag_path

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
Functions to manage push/pull of MORF files to s3.
"""

import os
import subprocess
import logging
from morf.utils.log import set_logger_handlers

module_logger = logging.getLogger(__name__)


def fetch_mode_files(job_config, dest_dir, mode=None):
    """
    Fetch the files for mode from job_config.
    :param job_config:
    :param dest_dir: directory to download files to
    :param mode:
    :return:
    """
    if not mode:
        mode = job_config.mode
    # list objects for mode
    # for each object, download it
    return


def sync_s3_bucket_cache(job_config, bucket):
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


def sync_s3_job_cache(job_config, modes=("extract", "extract-holdout", "train", "test")):
    """
    Sync data in s3 just for this specific job (better for large buckets or when the entire bucket is not actually needed).
    :param job_config:
    :param bucket:
    :param modes: modes to update cache for; by default to all modes
    :return:
    """
    bucket = job_config.proc_data_bucket
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
    for m in modes:
        s3_prefix = make_s3_key_path(job_config, mode=m)
        mode_cache_dir = os.path.join(bucket_cache_dir, job_config.user_id, job_config.job_id, m)
        # execute s3 sync command
        cmd = "{} s3 sync {}/{} {}".format(job_config.aws_exec, s3bucket, s3_prefix, mode_cache_dir)
        logger.info("running {}".format(cmd))
        try:
            subprocess.call(cmd, shell=True)
        except Exception as e:
            logger.warning("exception when executing sync: {}".format(e))
    return


def make_s3_key_path(job_config, course = None, filename = None, session = None, mode = None, job_id = None):
    """
    Create a key path following MORF's subdirectory organization and any non-null parameters provided.
    :param job_config: MorfJobConfig object.
    :param course: course slug (string).
    :param filename: file name (string; base filename only - no path).
    :param session: course session (string).
    :param mode: optional mode to override job_config.mode attribute (string).
    :return: key path (string) for use in s3.
    """
    if not mode:
        mode = job_config.mode
    if not job_id: # users have option to specify another job_id for forking features
        job_id = job_config.job_id
    job_attributes = [job_config.user_id, job_id, mode, course, session, filename]
    active_attributes = [x for x in job_attributes if x is not None]
    key = "/".join(active_attributes)
    return key
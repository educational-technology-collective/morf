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
from urllib.parse import urlparse
import logging

logger = logging.getLogger()

def cache_s3_to_local(bucket, local_dest):
    """
    Cache all data in an s3 bucket to local_dest, creating a complete copy of files and directory structure.
    :param bucket: path to s3 bucket.
    :param local_dest: local destination to cache to (string). If it does not exist, it will be created.
    :return:
    """
    # check paths
    s3_url = urlparse(bucket)
    assert s3_url.scheme == "s3", "specify a valid path to an s3 bucket"
    # create local_dest directory if not exists
    if not os.path.exists(local_dest):
        try:
            os.makedirs(local_dest)
        except exception as e:
            logger.error("error creating cache: {}".format(e))
            raise
    # execute s3 sync command
    cmd = "aws s3 sync {} {}".format(bucket, local_dest)
    logger.info("running {}".format(cmd))
    subprocess.call(cmd, shell=True)
    return


def update_morf_job_cache(job_config):
    """
    Update the raw data cache using the parameters in job_config; if job_config contains multiple raw data buckets, cache all of them.
    :param job_config: MorfJobConfig object.
    :return:
    """
    job_cache_dir = job_config.cache_dir
    # cache each bucket in a named directory within job_cache_dir
    for raw_data_bucket in job_config.raw_data_buckets:
        s3bucket = "s3://{}".format(raw_data_bucket)
        bucket_cache_dir = os.path.join(job_cache_dir, raw_data_bucket)
        cache_s3_to_local(s3bucket, bucket_cache_dir)
    return
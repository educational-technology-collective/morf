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
    for raw_data_bucket in job_config.raw_data_buckets:
        s3bucket = "s3://{}".format(raw_data_bucket)
        cache_s3_to_local(s3bucket, job_cache_dir)
    return
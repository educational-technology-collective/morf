import os
import subprocess
from urllib.parse import urlparse

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
    local_url = urlparse(local_dest)
    assert  local_url.scheme == "file" and os.path.isdir(local_dest), "specify a valid local path to a directory"
    # create local_dest directory if not exists
    if not os.path.exists(local_dest):
        os.makedirs(local_dest)
    # execute s3 sync command
    cmd = "aws s3 sync {} {}".format(bucket, local_dest)
    logger.info("running {}".format(cmd))
    subprocess.call(cmd, shell=True)
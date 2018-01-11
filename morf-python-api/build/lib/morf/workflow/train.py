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
Training functions for the MORF 2.0 API. For more information about the API, see the documentation [HERE].
"""

from morf.utils import *
from morf.utils.api_utils import *
from morf.utils.job_runner_utils import run_job
from morf.utils.alerts import send_email_alert
from morf.utils.config import get_config_properties, fetch_data_buckets_from_config
import boto3
from multiprocessing import Pool

# define module-level variables from config.properties
proc_data_bucket = get_config_properties()["proc_data_bucket"]
docker_url = get_config_properties()["docker_url"]
user_id = get_config_properties()["user_id"]
job_id = get_config_properties()["job_id"]
email_to = get_config_properties()["email_to"]
aws_access_key_id = get_config_properties()["aws_access_key_id"]
aws_secret_access_key = get_config_properties()["aws_secret_access_key"]
# create s3 connection object for communicating with s3
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)

mode = "train"


def train_all(label_type):
    """
    Train a single overall model using the entire dataset using the Docker image.
    :param label_type:  label type provided by user.
    :return: None
    """
    check_label_type(label_type)
    raw_data_buckets = fetch_data_buckets_from_config()
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(proc_data_bucket, user_id, job_id, mode)
    run_job(docker_url, mode, None, user_id, job_id, None, "all", raw_data_buckets=raw_data_buckets, label_type=label_type)
    send_email_alert(aws_access_key_id,
                     aws_secret_access_key,
                     job_id,
                     user_id,
                     status=mode,
                     emailaddr_to=email_to)
    return


def train_course(label_type, raw_data_dir = "morf-data/"):
    """
    Trains one model per course using the Docker image.
    :param label_type:  label type provided by user.
    :raw_data_dir: path to directory in all data buckets where course-level directories are located; this should be uniform for every raw data bucket.
    :return: None
    """
    check_label_type(label_type)
    raw_data_buckets = fetch_data_buckets_from_config()
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(proc_data_bucket, user_id, job_id, mode)
    # for each bucket, call job_runner once per course with --mode=train and --level=course
    for raw_data_bucket in raw_data_buckets:
        print("[INFO] processing bucket {}".format(raw_data_bucket))
        with Pool() as pool:
            for course in fetch_complete_courses(s3, raw_data_bucket, raw_data_dir, n_train=1):
                pool.apply_async(run_job, [docker_url, mode, course, user_id, job_id, None, "course", raw_data_bucket, label_type])
            pool.close()
            pool.join()
    send_email_alert(aws_access_key_id,
                     aws_secret_access_key,
                     job_id,
                     user_id,
                     status=mode,
                     emailaddr_to=email_to)
    return


def train_session(label_type, raw_data_dir = "morf-data/"):
    """
    Train one model per session of the course using the Docker image.
    :param label_type:  label type provided by user.
    :return: None
    """
    check_label_type(label_type)
    raw_data_buckets = fetch_data_buckets_from_config()
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(proc_data_bucket, user_id, job_id, mode)
    # for each bucket, call job_runner once per session with --mode=train and --level=session
    for raw_data_bucket in raw_data_buckets:
        with Pool() as pool:
            for course in fetch_complete_courses(s3, raw_data_bucket, raw_data_dir, n_train=1):
                for run in fetch_sessions(s3, raw_data_bucket, raw_data_dir, course, fetch_holdout_session_only=False):
                    pool.apply_async(run_job, [docker_url, mode, course, user_id, job_id, run, "session", raw_data_bucket, label_type])
            pool.close()
            pool.join()
    send_email_alert(aws_access_key_id,
                     aws_secret_access_key,
                     job_id,
                     user_id,
                     status=mode,
                     emailaddr_to=email_to)
    return
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
Feature extraction functions for the MORF 2.0 API. For more information about the API, see the documentation.
"""


from morf.utils.job_runner_utils import run_job
from morf.utils import make_s3_key_path
from morf.utils.api_utils import *
from morf.utils.config import get_config_properties, fetch_data_buckets_from_config
from morf.utils.alerts import send_email_alert
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
s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)


def extract_all():
    """
    Extract features using the docker image across all courses and all sessions except holdout.
    :return:
    """
    mode = "extract"
    raw_data_buckets = fetch_data_buckets_from_config()
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(proc_data_bucket, user_id, job_id, mode)
    # only call job_runner once with --mode-extract and --level=all; this will load ALL data up and run the docker image
    run_job(docker_url, mode, course = None, user=user_id, job_id=job_id, session=None, level="all", raw_data_buckets=raw_data_buckets)
    result_file = collect_all_results(s3, raw_data_buckets, proc_data_bucket, mode, user_id, job_id)
    upload_key = make_s3_key_path(user_id, job_id, mode, course=None, filename= result_file)
    upload_file_to_s3(result_file, bucket=proc_data_bucket, key=upload_key)
    os.remove(result_file)
    send_email_alert(aws_access_key_id,
                     aws_secret_access_key,
                     job_id,
                     user_id,
                     status=mode,
                     emailaddr_to=email_to)
    return


def extract_course(raw_data_dir = "morf-data/"):
    """
    Extract features using the Docker image, building individual feature sets for each course.
    :return:
    """
    mode = "extract"
    raw_data_buckets = fetch_data_buckets_from_config()
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(proc_data_bucket, user_id, job_id, mode)
    # call job_runner once percourse with --mode=extract and --level=course
    for raw_data_bucket in raw_data_buckets:
        print("[INFO] processing bucket {}".format(raw_data_bucket))
        with Pool() as pool:
            for course in fetch_courses(s3, raw_data_bucket, raw_data_dir):
                pool.apply_async(run_job, [docker_url, mode, course, user_id, job_id, None, "course", raw_data_bucket])
            pool.close()
            pool.join()
    result_file = collect_course_results(s3, raw_data_buckets, proc_data_bucket, mode, user_id, job_id)
    upload_key = make_s3_key_path(user_id, job_id, mode, course=None, filename=result_file)
    upload_file_to_s3(result_file, bucket=proc_data_bucket, key=upload_key)
    os.remove(result_file)
    send_email_alert(aws_access_key_id,
                     aws_secret_access_key,
                     job_id,
                     user_id,
                     status=mode,
                     emailaddr_to=email_to)
    return


def extract_session(labels = False, raw_data_dir = "morf-data/", label_type = "labels-train", multithread = True):
    """
    Extract features using the Docker image, building individual feature sets for each "session" or iteration of the course.
    :labels: flag for whether this is a job to generate output labels; if so, the collected result file is copied back into the raw data folder in s3 (as labels-train.csv).
    :raw_data_dir: path to directory in all data buckets where course-level directories are located; this should be uniform for every raw data bucket.
    :return:
    """
    mode="extract"
    raw_data_buckets = fetch_data_buckets_from_config()
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(proc_data_bucket, user_id, job_id, mode)
    ## for each bucket, call job_runner once per session with --mode=extract and --level=session
    for raw_data_bucket in raw_data_buckets:
        print("[INFO] processing bucket {}".format(raw_data_bucket))
        if multithread:
            with Pool() as pool:
                for course in fetch_courses(s3, raw_data_bucket, raw_data_dir):
                    for run in fetch_sessions(s3, raw_data_bucket, raw_data_dir, course, 
                                              fetch_holdout_session_only=False):
                        pool.apply_async(run_job, [docker_url, mode, course, user_id, job_id, run, "session", 
                                                   raw_data_bucket])
                pool.close()
                pool.join()
        else: # do job in serial; this is useful for debugging
            for course in fetch_courses(s3, raw_data_bucket, raw_data_dir):
                for run in fetch_sessions(s3, raw_data_bucket, raw_data_dir, course, fetch_holdout_session_only=False):
                    run_job(docker_url, mode, course, user_id, job_id, run, "session", raw_data_bucket)
    if not labels: # normal feature extraction job; collects features across all buckets and upload to proc_data_bucket
        result_file = collect_session_results(s3, raw_data_buckets, proc_data_bucket, mode, user_id, job_id)
        upload_key = "{}/{}/extract/{}".format(user_id, job_id, result_file)
        upload_file_to_s3(result_file, bucket = proc_data_bucket, key = upload_key)
    if labels: # label extraction job; copy file into raw course data dir instead of proc_data_bucket, creating separate label files for each bucket
        for raw_data_bucket in raw_data_buckets:
            result_file = collect_session_results(s3, [raw_data_bucket], proc_data_bucket, mode, user_id, job_id)
            upload_key = raw_data_dir + "{}.csv".format(label_type)
            upload_file_to_s3(result_file, bucket = raw_data_bucket, key = upload_key)
    os.remove(result_file)
    send_email_alert(aws_access_key_id,
                     aws_secret_access_key,
                     job_id,
                     user_id,
                     status=mode,
                     emailaddr_to=email_to)
    return


def extract_holdout_all():
    """
    Extract features using the Docker image across all courses and all sessions of holdout data.
    :return:
    """
    mode = "extract-holdout"
    raw_data_buckets = fetch_data_buckets_from_config()
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(proc_data_bucket, user_id, job_id, mode)
    # only call job_runner once with --mode-extract and --level=all; this will load ALL data up and run the docker image
    run_job(docker_url, mode, course=None, user=user_id, job_id=job_id, session=None, level="all", 
            raw_data_buckets=raw_data_buckets)
    result_file = collect_all_results(s3, raw_data_buckets, proc_data_bucket, mode, user_id, job_id)
    upload_key = make_s3_key_path(user_id, job_id, mode, course=None, filename= result_file)
    upload_file_to_s3(result_file, bucket=proc_data_bucket, key=upload_key)
    os.remove(result_file)
    send_email_alert(aws_access_key_id,
                     aws_secret_access_key,
                     job_id,
                     user_id,
                     status=mode,
                     emailaddr_to=email_to)
    return


def extract_holdout_course(raw_data_dir = "morf-data/"):
    """
    Extract features using the Docker image across each course of holdout data.
    :return:
    """
    mode = "extract-holdout"
    raw_data_buckets = fetch_data_buckets_from_config()
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(proc_data_bucket, user_id, job_id, mode)
    # call job_runner once percourse with --mode=extract and --level=course
    for raw_data_bucket in raw_data_buckets:
        print("[INFO] processing bucket {}".format(raw_data_bucket))
        with Pool() as pool:
            for course in fetch_courses(s3, raw_data_bucket, raw_data_dir):
                pool.apply_async(run_job, [docker_url, mode, course, user_id, job_id, None, "course", raw_data_bucket])
            pool.close()
            pool.join()
    result_file = collect_course_results(s3, raw_data_buckets, proc_data_bucket, mode, user_id, job_id)
    upload_key = make_s3_key_path(user_id, job_id, mode, course= None, filename= result_file)
    upload_file_to_s3(result_file, bucket=proc_data_bucket, key=upload_key)
    os.remove(result_file)
    send_email_alert(aws_access_key_id,
                     aws_secret_access_key,
                     job_id,
                     user_id,
                     status=mode,
                     emailaddr_to=email_to)
    return


def extract_holdout_session(labels = False, raw_data_dir = "morf-data/", label_type = "labels-train", multithread = True):
    """
    Extract features using the Docker image across each session of holdout data.
    :labels: flag for whether this is a job to generate output labels; if so, the collected result file is copied back into the raw data folder in s3 (as labels-test.csv).
    :return: None
    """
    mode="extract-holdout"
    # call job_runner once per session with --mode=extract-holdout and --level=session
    raw_data_buckets = fetch_data_buckets_from_config()
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(proc_data_bucket, user_id, job_id, mode)
    for raw_data_bucket in raw_data_buckets:
        print("[INFO] processing bucket {}".format(raw_data_bucket))
        if multithread:
            with Pool() as pool:
                for course in fetch_courses(s3, raw_data_bucket, raw_data_dir):
                    holdout_run = fetch_sessions(s3, raw_data_bucket, raw_data_dir, course, 
                                                 fetch_holdout_session_only=True)[0] # only use holdout run; unlisted
                    pool.apply_async(run_job, [docker_url, mode, course, user_id, job_id, holdout_run, "session", 
                                               raw_data_bucket])
                pool.close()
                pool.join()
        else: # do job in serial; this is useful for debugging
            for course in fetch_courses(s3, raw_data_bucket, raw_data_dir):
                holdout_run = fetch_sessions(s3, raw_data_bucket, raw_data_dir, course, 
                                             fetch_holdout_session_only=True)[0]  # only use holdout run; unlisted
                run_job(docker_url, mode, course, user_id, job_id, holdout_run, "session", raw_data_bucket)
    if not labels: # normal feature extraction job; collects features across all buckets and upload to proc_data_bucket
        result_file = collect_session_results(s3, raw_data_buckets, proc_data_bucket, mode, user_id, job_id, 
                                              holdout=True)
        upload_key = "{}/{}/{}/{}".format(user_id, job_id, mode, result_file)
        upload_file_to_s3(result_file, bucket = proc_data_bucket, key = upload_key)
    if labels: # label extraction job; copy file into raw course data dir instead of proc_data_bucket, creating separate label files for each bucket
        for raw_data_bucket in raw_data_buckets:
            result_file = collect_session_results(s3, [raw_data_bucket], proc_data_bucket, mode, user_id, job_id, 
                                                  holdout = True)
            upload_key = raw_data_dir + "{}.csv".format(label_type)
            upload_file_to_s3(result_file, bucket = raw_data_bucket, key = upload_key)
    os.remove(result_file)
    send_email_alert(aws_access_key_id,
                     aws_secret_access_key,
                     job_id,
                     user_id,
                     status=mode,
                     emailaddr_to=email_to)
    return





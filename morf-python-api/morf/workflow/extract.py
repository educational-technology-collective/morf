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
from morf.utils import make_s3_key_path, generate_archive_filename, copy_s3_file
from morf.utils.api_utils import *
from morf.utils.config import get_config_properties, fetch_data_buckets_from_config, MorfJobConfig
from morf.utils.alerts import send_email_alert
import boto3
from multiprocessing import Pool

# define module-level variables for config.properties
CONFIG_FILENAME = "config.properties"


def extract_all():
    """
    Extract features using the docker image across all courses and all sessions except holdout.
    :return:
    """
    mode = "extract"
    level = "all"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    # only call job_runner once with --mode-extract and --level=all; this will load ALL data up and run the docker image
    run_job(job_config, None, None, level, raw_data_buckets=job_config.raw_data_buckets)
    result_file = collect_all_results(job_config)
    upload_key = make_s3_key_path(job_config, filename=result_file)
    upload_file_to_s3(result_file, bucket=job_config.proc_data_bucket, key=upload_key)
    os.remove(result_file)
    send_email_alert(job_config)
    return


def extract_course(raw_data_dir="morf-data/", multithread = True):
    """
    Extract features using the Docker image, building individual feature sets for each course.
    :return:
    """
    mode = "extract"
    level = "course"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    # call job_runner once percourse with --mode=extract and --level=course
    for raw_data_bucket in job_config.raw_data_buckets:
        print("[INFO] processing bucket {}".format(raw_data_bucket))
        courses = fetch_courses(job_config, raw_data_bucket, raw_data_dir)
        if multithread:
            reslist = []
            with Pool(job_config.max_num_cores) as pool:
                for course in courses:
                    poolres = pool.apply_async(run_job, [job_config, course, None, level, raw_data_bucket])
                    reslist.append(poolres)
                pool.close()
                pool.join()
            for res in reslist:
                print(res.get())
        else: # do job in serial; this is useful for debugging
            for course in courses:
                run_job(job_config, course, None, level, raw_data_bucket)
    result_file = collect_course_results(job_config)
    upload_key = make_s3_key_path(job_config, filename=result_file)
    upload_file_to_s3(result_file, bucket=job_config.proc_data_bucket, key=upload_key)
    os.remove(result_file)
    send_email_alert(job_config)
    return


def extract_session(labels=False, raw_data_dir="morf-data/", label_type="labels-train", multithread=True):
    """
    Extract features using the Docker image, building individual feature sets for each "session" or iteration of the course.
    :labels: flag for whether this is a job to generate output labels; if so, the collected result file is copied back into the raw data folder in s3 (as labels-train.csv).
    :raw_data_dir: path to directory in all data buckets where course-level directories are located; this should be uniform for every raw data bucket.
    :label_type: type of outcome label to use (string).
    :multithread: whether to run job in parallel (multithread = false can be useful for debugging).
    :return:
    """
    level = "session"
    mode = "extract"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    # # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    ## for each bucket, call job_runner once per session with --mode=extract and --level=session
    for raw_data_bucket in job_config.raw_data_buckets:
        print("[INFO] processing bucket {}".format(raw_data_bucket))
        courses = fetch_courses(job_config, raw_data_bucket, raw_data_dir)
        if multithread:
            reslist = []
            with Pool(job_config.max_num_cores) as pool:
                for course in courses:
                    for session in fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course,
                                                  fetch_holdout_session_only=False):
                        poolres = pool.apply_async(run_job, [job_config, course, session, level, raw_data_bucket])
                        reslist.append(poolres)
                pool.close()
                pool.join()
            for res in reslist:
                print(res.get())
        else:  # do job in serial; this is useful for debugging
            for course in courses:
                for session in fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course,
                                              fetch_holdout_session_only=False):
                    run_job(job_config, course, session, level, raw_data_bucket)
    if not labels:  # normal feature extraction job; collects features across all buckets and upload to proc_data_bucket
        result_file = collect_session_results(job_config)
        upload_key = "{}/{}/extract/{}".format(job_config.user_id, job_config.job_id, result_file)
        upload_file_to_s3(result_file, bucket=job_config.proc_data_bucket, key=upload_key)
    else:  # label extraction job; copy file into raw course data dir instead of proc_data_bucket, creating separate label files for each bucket
        for raw_data_bucket in job_config.raw_data_buckets:
            result_file = collect_session_results(job_config, raw_data_buckets=[raw_data_bucket])
            upload_key = raw_data_dir + "{}.csv".format(label_type)
            upload_file_to_s3(result_file, bucket=raw_data_bucket, key=upload_key)
    os.remove(result_file)
    send_email_alert(job_config)
    return


def extract_holdout_all():
    """
    Extract features using the Docker image across all courses and all sessions of holdout data.
    :return:
    """
    mode = "extract-holdout"
    level = "all"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    # only call job_runner once with --mode-extract and --level=all; this will load ALL data up and run the docker image
    run_job(job_config, None, None, level, raw_data_buckets=job_config.raw_data_buckets)
    result_file = collect_all_results(job_config)
    upload_key = make_s3_key_path(job_config, filename=result_file)
    upload_file_to_s3(result_file, bucket=job_config.proc_data_bucket, key=upload_key)
    os.remove(result_file)
    send_email_alert(job_config)
    return


def extract_holdout_course(raw_data_dir="morf-data/", multithread = True):
    """
    Extract features using the Docker image across each course of holdout data.
    :return:
    """
    mode = "extract-holdout"
    level = "course"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    job_config.initialize_s3()
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    # call job_runner once percourse with --mode=extract and --level=course
    for raw_data_bucket in job_config.raw_data_buckets:
        print("[INFO] processing bucket {}".format(raw_data_bucket))
        courses = fetch_courses(job_config, raw_data_bucket, raw_data_dir)
        if multithread:
            reslist = []
            with Pool(job_config.max_num_cores) as pool:
                for course in courses:
                    holdout_session = fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course,
                                                 fetch_holdout_session_only=True)[0]  # only use holdout run; unlisted
                    poolres = pool.apply_async(run_job, [job_config, course, holdout_session, level, raw_data_bucket])
                    reslist.append(poolres)
                pool.close()
                pool.join()
            for res in reslist:
                print(res.get())
        else: # do job in serial; this is useful for debugging
            for course in courses:
                holdout_session = fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course,
                                                 fetch_holdout_session_only=True)[0]  # only use holdout run; unlisted
                run_job(job_config, course, holdout_session, level, raw_data_bucket)
    result_file = collect_course_results(job_config)
    upload_key = make_s3_key_path(job_config, filename=result_file)
    upload_file_to_s3(result_file, bucket=job_config.proc_data_bucket, key=upload_key)
    os.remove(result_file)
    send_email_alert(job_config)
    return


def extract_holdout_session(labels=False, raw_data_dir="morf-data/", label_type="labels-train", multithread=True):
    """
    Extract features using the Docker image across each session of holdout data.
    :labels: flag for whether this is a job to generate output labels; if so, the collected result file is copied back into the raw data folder in s3 (as labels-test.csv).
    :return: None
    """
    mode = "extract-holdout"
    level = "session"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    job_config.update_mode(mode)
    job_config.initialize_s3()
    # call job_runner once per session with --mode=extract-holdout and --level=session
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    for raw_data_bucket in job_config.raw_data_buckets:
        print("[INFO] processing bucket {}".format(raw_data_bucket))
        courses = fetch_courses(job_config, raw_data_bucket, raw_data_dir)
        if multithread:
            reslist = []
            with Pool(job_config.max_num_cores) as pool:
                for course in courses:
                    holdout_run = fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course,
                                                 fetch_holdout_session_only=True)[0]  # only use holdout run; unlisted
                    poolres = pool.apply_async(run_job, [job_config, course, holdout_run, level, raw_data_bucket])
                pool.close()
                pool.join()
            for res in reslist:
                print(res.get())
        else:  # do job in serial; this is useful for debugging
            for course in courses:
                holdout_run = fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course,
                                             fetch_holdout_session_only=True)[0]  # only use holdout run; unlisted
                run_job(job_config, course, holdout_run, level, raw_data_bucket)
    if not labels:  # normal feature extraction job; collects features across all buckets and upload to proc_data_bucket
        result_file = collect_session_results(job_config, holdout=True)
        upload_key = "{}/{}/{}/{}".format(job_config.user_id, job_config.job_id, job_config.mode, result_file)
        upload_file_to_s3(result_file, bucket=job_config.proc_data_bucket, key=upload_key)
    else:  # label extraction job; copy file into raw course data dir instead of proc_data_bucket, creating separate label files for each bucket
        for raw_data_bucket in job_config.raw_data_buckets:
            result_file = collect_session_results(job_config, raw_data_buckets=[raw_data_bucket])
            upload_key = raw_data_dir + "{}.csv".format(label_type)
            upload_file_to_s3(result_file, bucket=raw_data_bucket, key=upload_key)
    os.remove(result_file)
    send_email_alert(job_config)
    return


def fork_features(job_id_to_fork, raw_data_dir = "morf-data/"):
    """
    Copies features from job_id_to_fork into current job_id.
    :param job_id_to_fork: string, name of job_id (must be from same user).
    :return: None.
    """
    job_config = MorfJobConfig(CONFIG_FILENAME)
    #todo: multithread this
    for mode in ["extract", "extract-holdout"]:
        job_config.update_mode(mode)
        clear_s3_subdirectory(job_config)
        for raw_data_bucket in job_config.raw_data_buckets:
            print("[INFO] forking features from bucket {} mode {}".format(raw_data_bucket, mode))
            courses = fetch_courses(job_config, raw_data_bucket, raw_data_dir)
            for course in courses:
                for session in fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course,
                                              fetch_holdout_session_only = mode == "extract-holdout"):
                    # get current location of file, with old jobid name
                    prev_job_archive_filename = generate_archive_filename(job_config, course = course, session = session, mode = mode, job_id = job_id_to_fork)
                    # get location of prev archive file in s3
                    prev_job_key = make_s3_key_path(job_config, filename=prev_job_archive_filename, course=course, session=session, mode=mode, job_id=job_id_to_fork)
                    prev_job_s3_url = "s3://{}/{}".format(job_config.proc_data_bucket, prev_job_key)
                    # make new location of file, with new jobid name
                    current_job_archive_filename = generate_archive_filename(job_config, course=course, session=session,
                                                                          mode=mode)
                    # copy frmo current location to new location
                    current_job_key = make_s3_key_path(job_config, filename=current_job_archive_filename, course=course,
                                                    session=session, mode=mode)
                    current_job_s3_url = "s3://{}/{}".format(job_config.proc_data_bucket, current_job_key)
                    copy_s3_file(job_config, sourceloc = prev_job_s3_url, destloc = current_job_s3_url)
        # after copying individual extraction results, copy collected feature file
        result_file = collect_session_results(job_config, holdout = mode == "extract-holdout")
        upload_key = "{}/{}/{}/{}".format(job_config.user_id, job_config.job_id, job_config.mode, result_file)
        upload_file_to_s3(result_file, bucket=job_config.proc_data_bucket, key=upload_key)
    return


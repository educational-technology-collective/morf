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


import gzip
import logging
import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import urllib.request
from urllib.parse import urlparse

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from morf.utils.caching import fetch_from_cache, make_course_session_cache_dir_fp
from morf.utils.log import set_logger_handlers, execute_and_log_output
# create logger
from morf.utils.s3interface import make_s3_key_path

module_logger = logging.getLogger(__name__)



def unarchive_file(src, dest, remove=True):
    """
    Untar or un-gzip a file from src into dest. Supports file extensions: .zip, .tgz, .gz.
    :param src: path to source file to unarchive (string).
    :param dest: directory to unarchive result into (string).
    :param remove: should file be removed after it is unarchived?
    :return: None
    """
    success = False
    if src.endswith(".zip") or src.endswith(".tgz"):
        tar = tarfile.open(src)
        tar.extractall(dest)
        tar.close()
        success = True
        outpath = os.path.join(dest, os.path.basename(src))
    elif src.endswith(".gz"):
        with gzip.open(src, "rb") as f_in:
            destfile = os.path.basename(src)[:-3] # source file without '.gz' extension
            destpath = os.path.join(dest, destfile)
            with open(destpath, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        success = True
        outpath = destpath
    else:
        raise NotImplementedError("Passed in a file with an extension not supported by unarchive_file: {}".format(src))
    # cleanup after unarchive
    if success and remove:
        try:
            os.remove(src)
        except Exception as e:
            print("[ERROR] error removing file {}: {}".format(src, e))
    return outpath


def clean_filename(src):
    """
    Rename file, removing any non-alphanumeric characters.
    :param src: file to rename.
    :return: None
    """
    src_dir, src_file = os.path.split(src)
    clean_src_file = re.sub('[\(\)\s&]', '', src_file)
    clean_src_path = os.path.join(src_dir, clean_src_file)
    try:
        os.rename(src, clean_src_path)
    except Exception as e:
        print("[ERROR] error renaming file: {}".format(e))
    return


def get_bucket_from_url(url):
    """
    Parse S3 url to get bucket name
    :param url: string url for S3 file.
    :return: bucket name (string)
    """
    return re.search("^s3://([^/]+)", url).group(1)


def get_key_from_url(url):
    """
    Parse S3 url to get key name (i.e., rest of path besides s3://bucketname )
    :param url: string url for S3 file.
    :return: key name (string)
    """
    return re.search("^s3://[^/]+/(.+)", url).group(1)


def download_from_s3(bucket, key, s3, dir = os.getcwd(), dest_filename = None, job_config = None):
    """
    Downloads a file from s3 into dir and returns its path as a string for optional use.
    :param bucket: an s3 bucket name (string).
    :param key: key of a file in bucket (string).
    :param s3: boto3.client object for s3 connection.
    :param dir: directory where file should be downloaded (string); will be created if does not exist
    :param dest_filename: base name for file.
    :return: Path to downloaded file inside dir (string).
    """
    if job_config:
        logger = set_logger_handlers(module_logger, job_config)
    else:
        logger = module_logger
    if not dest_filename:
        dest_filename = os.path.basename(key)
    if not os.path.exists(dir):
        os.makedirs(dir)
    with open(os.path.join(dir, dest_filename), "wb") as resource:
        try:
            s3.download_fileobj(bucket, key, resource)
        except ClientError as ce:
            logger.error("boto ClientError downloading from location s3://{}/{}: {}".format(bucket, key, ce))
            raise
        except Exception as e:
            logger.error("error downloading from location s3://{}/{}: {}".format(bucket, key, e))
            raise
    dest_path = os.path.join(dir, dest_filename)
    return dest_path


def initialize_tar(fp, s3, dest_dir = None):
    """
    Prepare tar file at fp, either downloading from s3 or just fetching tar name if file is local.
    :param fp: path to .tar file.
    :param s3: boto3.client object with appropriate access credentials.
    :param dest_dir: if tar file is located in s3, location to download file to.
    :return:
    """
    file_url = urlparse(fp)
    if file_url.scheme == "file":
        tar_path = file_url.path
    if file_url.scheme == "s3":
        bucket = get_bucket_from_url(fp)
        bucketkey = get_key_from_url(fp)
        tar_path = download_from_s3(bucket, bucketkey, s3, dest_dir)
    return tar_path


def set_all_file_permissions(dir):
    """
    Set permissions to 777 for all files in dir.
    :param dir: directory to set permissions for.
    :return:
    """
    for root, dirs, files in os.walk(dir):
        for d in dirs:
            os.chmod(os.path.join(root, d), 0o777)
        for f in files:
            os.chmod(os.path.join(root, f), 0o777)
    return


def fetch_courses(job_config, data_bucket, data_dir ="morf-data/"):
    """
    Fetch list of course names in data_bucket/data_dir.
    :param job_config: MorfJobConfig object.
    :param data_bucket: name of bucket containing data; s3 should have read/copy access to this bucket.
    :param data_dir: path to directory in data_bucket that contains course-level directories of raw data.
    :return: courses; list of course names as strings.
    """
    s3 = job_config.initialize_s3()
    if not data_dir.endswith("/"):
        data_dir = "{0}/".format(data_dir)
    bucket_objects = s3.list_objects(Bucket=data_bucket, Prefix=data_dir, Delimiter="/")
    courses = [item.get("Prefix").split("/")[1] for item in bucket_objects.get("CommonPrefixes")]
    return courses


def fetch_sessions(job_config, data_bucket, data_dir, course, fetch_holdout_session_only = False, fetch_all_sessions = False):
    """
    Fetch course sessions in data_bucket/data_dir. By default, fetches only training sessions (not holdout session).
    :param job_config: MorfJobConfig object.
    :param data_bucket: name of bucket containing data; s3 should have read/copy access to this bucket.
    :param data_dir: path to directory in data_bucket that contains course-level directories of raw data.
    :param course: string; name of course (should match course-level directory name in s3 directory tree).
    :param fetch_holdout_session_only: logical; return only holdout (final) session.
    :param fetch_all_sessions: logical; return all sessions (training and holdout).
    :return: list of session numbers as strings.
    """
    assert (not (fetch_holdout_session_only & fetch_all_sessions)), "choose one - fetch holdout sessions or fetch all sessions"
    s3 = job_config.initialize_s3()
    if not data_dir.endswith("/"):
        data_dir = data_dir + "/"
    course_bucket_objects = s3.list_objects(Bucket=data_bucket, Prefix="".join([data_dir, course, "/"]), Delimiter="/")
    sessions = [item.get("Prefix").split("/")[2] for item in course_bucket_objects.get("CommonPrefixes")]
    sessions = sorted(sessions, key = lambda x: x[-3:]) # handles session numbers like "2012-001" by keeping leading digits before "-" but only sorts on last 3 digits
    if fetch_all_sessions: # return complete list of sessions
        result = sessions
    else:
        holdout_session = sessions.pop(-1)
        if fetch_holdout_session_only == True:
            result = [holdout_session] # return only holdout session, but as a list, so type is consistent
        else:
            result = sessions # return list of sessions without holdout session
    return tuple(result)


def fetch_complete_courses(job_config, data_bucket, data_dir ="morf-data/", n_train=1):
    """
    Fetch names of courses in data_bucket/data_dir which have at least n_train training sessions and one holdout session.
    :param job_config: MorfJobConfig object
    :param data_bucket: name of bucket containing data; s3 should have read/copy access to this bucket.
    :param data_dir: path to directory in data_bucket that contains course-level directories of raw data.
    :param n_train: minimum number of training sessions a course needs to have in order to be returned.
    :return: list of course names.
    """
    complete_courses = []
    for course in fetch_courses(job_config, data_bucket, data_dir):
        training_sessions = fetch_sessions(job_config, data_bucket, data_dir, course, fetch_holdout_session_only=False)
        testing_session = fetch_sessions(job_config, data_bucket, data_dir, course, fetch_holdout_session_only=True)
        if (len(testing_session) == 1) and (len(training_sessions) >= n_train):
            complete_courses.append(course)
    return complete_courses


def fetch_all_complete_courses_and_sessions(job_config, data_dir ="morf-data/", n_train=1):
    """
    Returns a list of tuples, where the first element in each tuple is the course name, and the second element is the course sessions.
    :param job_config: MorfJobConfig object.
    :param data_dir: name of directory within buckets containing raw course data.
    :param n_train: Number of training sessions each course must have (if n_train = 1, courses must have one training and one testing session).
    :return: list of tuples, described above.
    """
    complete_courses = []
    for data_bucket in job_config.raw_data_buckets:
        for course in fetch_courses(job_config, data_bucket, data_dir):
            training_sessions = fetch_sessions(job_config, data_bucket, data_dir, course,fetch_holdout_session_only=False)
            testing_session = fetch_sessions(job_config, data_bucket, data_dir, course, fetch_holdout_session_only=True)
            if (len(testing_session) == 1) and (len(training_sessions) >= n_train):
                sessions = training_sessions + testing_session
                complete_courses.append((course, sessions))
    return complete_courses


def download_raw_course_data(job_config, bucket, course, session, input_dir, data_dir, course_date_file_name = "coursera_course_dates.csv"):
    """
    Download all raw course files for course and session into input_dir.
    :param job_config: MorfJobConfig object.
    :param bucket: bucket containing raw data.
    :param course: id of course to download data for.
    :param session: id of session to download data for.
    :param input_dir: input directory.
    :param data_dir: directory in bucket that contains course-level data.
    :param course_date_file_name: name of csv file in bucket which contains course start/end dates.
    :return: None
    """
    s3 = job_config.initialize_s3()
    logger = set_logger_handlers(module_logger, job_config)
    course_date_file_url =  "s3://{}/{}/{}".format(bucket, data_dir, course_date_file_name)
    session_input_dir = os.path.join(input_dir, course, session)
    os.makedirs(session_input_dir)
    for obj in boto3.resource("s3", aws_access_key_id=job_config.aws_access_key_id, aws_secret_access_key=job_config.aws_secret_access_key)\
            .Bucket(bucket).objects.filter(Prefix="{}/{}/{}/".format(data_dir, course, session)):
        filename = obj.key.split("/")[-1]
        filename = re.sub('[\s\(\)":!&]', "", filename)
        filepath = os.path.join(session_input_dir, filename)
        try:
            with open(filepath, "wb") as resource:
                s3.download_fileobj(bucket, obj.key, resource)
        except:
            logger.warning("skipping empty object in bucket {} key {}".format(bucket, obj.key))
            continue
    dates_bucket = get_bucket_from_url(course_date_file_url)
    dates_key = get_key_from_url(course_date_file_url)
    dates_file = dates_key.split("/")[-1]
    s3.download_file(dates_bucket, dates_key, os.path.join(session_input_dir, dates_file))
    return


def fetch_raw_course_data(job_config, bucket, course, session, input_dir, data_dir ="morf-data/"):
    """
    Fetch raw course data from job_config.cache_dir, if exists; otherwise fetch from s3.
    :param job_config: MorfJobConfig object
    :param bucket: bucket containing raw data.
    :param course: id of course to download data for.
    :param session: id of session to download data for.
    :param input_dir: input directory.
    :param data_dir: directory in bucket that contains course-level data.
    :return: None
    """
    logger = set_logger_handlers(module_logger, job_config)
    course_date_file = "coursera_course_dates.csv"
    session_input_dir = os.path.join(input_dir, course, session)
    if hasattr(job_config, "cache_dir"):
        course_session_cache_dir = make_course_session_cache_dir_fp(job_config, bucket, data_dir, course, session)
        try:
            logger.info("copying data from cached location {} to {}".format(course_session_cache_dir, session_input_dir))
            shutil.copytree(course_session_cache_dir, session_input_dir)
            course_date_file = os.path.join(job_config.cache_dir, bucket, data_dir, course_date_file)
            shutil.copy(course_date_file, session_input_dir)
        except Exception as e:
            logger.error("exception while attempting to copy from cache: {}".format(e))
    else:
        download_raw_course_data(job_config, bucket=raw_data_bucket,
                                 course=course, session=session, input_dir=input_dir,
                                 data_dir=data_dir)
    # unzip all of the sql files and remove any parens from filename
    for item in os.listdir(session_input_dir):
        if item.endswith(".sql.gz"):
            item_path = os.path.join(session_input_dir, item)
            unarchive_res = unarchive_file(item_path, session_input_dir)
            clean_filename(unarchive_res)
    return


def initialize_session_labels(job_config, bucket, course, session, label_type, dest_dir, data_dir, use_cache = True):
    """
    Fetch labels file and extract results for course and session into labels.csv.
    :param job_config: MorfJobConfig object.
    :param bucket: bucket with extracted data.
    :param course: course to fetch data for.
    :param session: session to fetch data for.
    :param label_type: valid label type to place in 'label' column
    :param dest_dir: directory to load data into. This should be same directory mounted to Docker image in docker run command.
    :param data_dir: directory in bucket containing course-level data directories.
    :param use_cache: if true, will attempt to use cached labels file.
    :return: Path to labels (string).
    """
    logger = set_logger_handlers(module_logger, job_config)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    # fetch mode; need to handle special cases of cv
    if job_config.mode == "cv" and session in fetch_sessions(job_config, bucket, data_dir, course, fetch_holdout_session_only=True):  # this is holdout session; use the "test" labels
        mode = "test"
    elif job_config.mode == "cv":  # this is not holdout session; use the "train" labels
        mode = "train"
    else:
        mode = job_config.mode
    # create filename
    label_csv = "labels-{}.csv".format(mode) # file with labels for ALL courses
    key = data_dir + label_csv
    # fetch from cache, or download if not using cache
    if hasattr(job_config, "cache_dir") and use_cache:
        logger.info("fetching labels file from cache for course {} session {} mode {}".format(course, session, mode))
        # fetch file from cache
        cache_file_path = "/".join([bucket, key])
        label_csv_fp = fetch_from_cache(job_config, cache_file_path, dest_dir)
    else:
        logger.info("fetching labels file from s3 for course {} session {} mode {}".format(course, session, mode))
        s3 = job_config.initialize_s3()
        label_csv_fp = download_from_s3(bucket, key, s3, dest_dir, label_csv, job_config)
    # read dataframe and filter for correct course/session/labels
    df = pd.read_csv(label_csv_fp, dtype=object)
    course_label_df = df.loc[(df["course"] == course) & (df["session"] == session) & (df["label_type"] == label_type)]\
        .copy()
    course_label_df.drop(["course", "session", "label_type"], axis = 1, inplace=True)
    course_label_csv_fp = os.path.join(dest_dir, make_label_csv_name(course, session))
    course_label_df.to_csv(course_label_csv_fp, index=False)
    os.remove(label_csv_fp)
    return course_label_csv_fp


def initialize_labels(job_config, bucket, course, session, label_type, dest_dir, data_dir, level = "session"):
    """

    :param job_config:
    :param bucket:
    :param course:
    :param session:
    :param label_type:
    :param dest_dir:
    :param data_dir:
    :param level:
    :return:
    """
    if level == "session": #initialize labels for individual session
        label_csv_fp = initialize_session_labels(job_config, bucket, course, session, label_type, dest_dir, data_dir)
    elif level == "course": # initialize labels for all sessions in course in a single file
        for session in fetch_sessions(job_config, bucket, data_dir, course, fetch_all_sessions=True):
            initialize_session_labels(job_config, bucket, course, session, label_type, os.path.join(dest_dir, session), data_dir)
        label_csv_fp = aggregate_session_input_data("labels", dest_dir)
    elif level == "all": # initialize labels for all courses in bucket into a single file
        course_label_df_list = []
        for course in fetch_courses(job_config, bucket, data_dir):
            for session in fetch_sessions(job_config, bucket, data_dir, course, fetch_all_sessions=True):
                initialize_session_labels(job_config, bucket, course, session, label_type,
                                          os.path.join(dest_dir, session), data_dir)
            course_label_csv_fp = aggregate_session_input_data("labels", dest_dir, course=course)
            course_label_df = pd.read_csv(course_label_csv_fp, dtype=object)
            course_label_df["course"] = course
            course_label_df_list.append(course_label_df)
            os.remove(course_label_csv_fp)
        label_csv_fp = os.path.join(dest_dir, "labels.csv")
        pd.concat(course_label_df_list).to_csv(label_csv_fp, index = False)
    return label_csv_fp


def filter_train_test_data(job_config, course, session, input_dir, feature_csv, remove=True):
    """
    Filter feature_csv to include only data from the specified course and session.
    :param job_config: MorfJobConfig object.
    :param course: course slug.
    :param session: session number.
    :param input_dir: input directorty which should contain feature_csv at input_dir/course/session location.
    :param feature_csv: base name of feature csv file.
    :param remove: indicator for whether feature_csv should be removed after its results are filtered.
    :return: None
    """
    logger = set_logger_handlers(module_logger, job_config)
    session_input_dir = os.path.join(input_dir, course, session)
    local_feature_csv = os.path.join(session_input_dir, feature_csv)
    try:
        logger.info("reading feature file from {} and filtering for features from course {} session {}".format(local_feature_csv, course, session))
        temp_df = pd.read_csv(local_feature_csv, dtype=object)
        outfile = os.path.join(session_input_dir, make_feature_csv_name(course, session))
        temp_df[(temp_df["course"] == course) & (temp_df["session"] == session)].drop(["course", "session"], axis=1) \
            .to_csv(outfile, index=False)
        if remove:
            os.remove(local_feature_csv)
    except Exception as e:
        logger.warning("exception while filtering feature file to create train/test data for course {} session {}: {}".format(course, session, e))
    return


def download_train_test_data(job_config, raw_data_bucket, raw_data_dir, course, session, input_dir, label_type):
    """
    Download pre-extracted train or test data (specified by mode) for course/session into input_dir.
    :param job_config: MorfJobConfig object.
    :param raw_data_bucket: bucket containing raw data.
    :param raw_data_dir: directory in raw_data_bucket containing course-level data.
    :param course: course to fetch data for.
    :param session: session to fetch data for.
    :param input_dir: /input directory to load data into. This should be same directory mounted to Docker image.
    :param label_type: valid label type to reatin for 'label' column of MORF-provided labels.
    :return: None
    """
    logger = set_logger_handlers(module_logger, job_config)
    s3 = job_config.initialize_s3()
    proc_data_bucket = job_config.proc_data_bucket
    if job_config.mode == "train" or (job_config.mode == "cv" and session in fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course)):
        fetch_mode = "extract"
    if job_config.mode == "test" or (job_config.mode == "cv" and session in fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course, fetch_holdout_session_only=True)):
        fetch_mode = "extract-holdout"
    logger.info(" fetching {} data for course {} session {}".format(fetch_mode, course, session))
    session_input_dir = os.path.join(input_dir, course, session)
    os.makedirs(session_input_dir)
    # download features file
    feature_csv = generate_archive_filename(job_config, mode=fetch_mode, extension="csv")
    key = "{}/{}/{}/{}".format(job_config.user_id, job_config.job_id, fetch_mode, feature_csv)
    download_from_s3(proc_data_bucket, key, s3, session_input_dir, job_config=job_config)
    # read features file and filter to only include specific course/session
    filter_train_test_data(job_config, course, session, input_dir, feature_csv)
    if job_config.mode in ("train", "cv"):  # download labels only if training or cv job; otherwise no labels needed
        initialize_labels(job_config, raw_data_bucket, course, session, label_type, dest_dir=session_input_dir,
                          data_dir=raw_data_dir)
    return


def fetch_train_test_data(job_config, raw_data_bucket, raw_data_dir, course, session, input_dir, label_type):
    """
    Fetch train and test data from job_config.cache_dir, if exists; otherwise fetch from s3.
    :return:
    """
    logger = set_logger_handlers(module_logger, job_config)
    proc_data_bucket = getattr(job_config, "proc_data_bucket")
    session_input_dir = os.path.join(input_dir, course, session)
    # find mode to fetch data for
    if job_config.mode == "train":
        fetch_mode = "extract"
    elif job_config.mode == "test":
        fetch_mode = "extract-holdout"
    else:
        logger.error("attempting to fetch train/test data while in mode {}".format(job_config.mode))
    # fetch train/test from cache, if exists; otherwise fetch from s3
    if hasattr(job_config, "cache_dir"):
        cache_dir = getattr(job_config, "cache_dir")
        feature_file_src_fname = generate_archive_filename(job_config, extension='csv', mode=fetch_mode)
        feature_file_dest_fname = make_feature_csv_name(job_config.user_id, job_config.job_id, job_config.mode)
        feature_file_cache_fp = os.path.join(cache_dir, proc_data_bucket, job_config.user_id, job_config.job_id, fetch_mode, feature_file_src_fname)
        feature_file_dest_fp = os.path.join(session_input_dir, feature_file_dest_fname)
        try:
            logger.info("copying feature data from cached location {} to {}".format(feature_file_cache_fp, feature_file_dest_fp))
            os.makedirs(session_input_dir, exist_ok=True)
            shutil.copy(feature_file_cache_fp, feature_file_dest_fp)
            filter_train_test_data(job_config, course, session, input_dir, feature_file_dest_fname)
        except Exception as e:
            logger.error("exception while attempting to copy train/test data from cache: {}".format(e))
        # labels
        if job_config.mode in ( "train", "cv"):  # download labels only if training or cv job; otherwise no labels needed
            initialize_labels(job_config, raw_data_bucket, course, session, label_type, dest_dir=session_input_dir, data_dir=raw_data_dir)
    else:
        download_train_test_data(job_config, raw_data_bucket, raw_data_dir, course, session, input_dir, label_type)
    return


def initialize_raw_course_data(job_config, raw_data_bucket, level, mode,
                               data_dir ="morf-data", course = None, session = None, input_dir ="./input"):
    """
    Initialize input directory of raw course data for extract or extract-holdout jobs.
    :param s3: boto3.client object for s3 connection.
    :param aws_access_key_id: aws_access_key_id.
    :param aws_secret_access_key: aws_secret_access_key.
    :param raw_data_bucket: bucket containing raw data.
    :param mode: one of {extract, extract-holdout}. Defines whether holdout sessions or training sessions should be used.
    :param course: id of course to download data for.
    :param session: id of session to download data for.
    :param input_dir: input directory.
    :param level: level of extraction {session, course, all}.
    :param data_dir: path to directory in bucket that contains course-level data directories.
    :param course_date_file_name: name of csv file located at bucket/data_dir containing course start and end dates.
    :return: None
    """
    if level == "all": # there is a unique course date file for each bucket
        # download all data; every session of every course
        for bucket in raw_data_bucket:
            for course in fetch_courses(job_config, bucket):
                if mode == "extract":
                    sessions = fetch_sessions(job_config, bucket, data_dir, course)
                    for session in sessions:
                        fetch_raw_course_data(job_config, bucket=bucket, course=course, session=session, input_dir=input_dir)
                if mode == "extract-holdout":
                    holdout_session = fetch_sessions(job_config, bucket, data_dir, course, fetch_holdout_session_only=True)[0]
                    fetch_raw_course_data(job_config, bucket=bucket, course=course, session=holdout_session, input_dir=input_dir)
    elif level == "course":
        # download all data for every session of course
        if mode == "extract":
            sessions = fetch_sessions(job_config, raw_data_bucket, data_dir, course)
            for session in sessions:
                fetch_raw_course_data(job_config, bucket=raw_data_bucket, course=course, session=session, input_dir=input_dir)
        if mode == "extract-holdout":
            holdout_session = fetch_sessions(job_config, raw_data_bucket, data_dir, course, fetch_holdout_session_only=True)[0]
            fetch_raw_course_data(job_config, bucket=raw_data_bucket, course=course, session=holdout_session, input_dir=input_dir)
    elif level == "session":
        # download only specific session
        fetch_raw_course_data(job_config, bucket=raw_data_bucket, course=course, session=session, input_dir=input_dir)
    return


def initialize_train_test_data(job_config, raw_data_bucket, level, label_type, course = None, session = None, input_dir ='./input', raw_data_dir = 'morf-data/'):
    """
    Mounts data in /input/course/session directories for MORF API train/test jobs.
    :param job_config: MorfJobConfig object.
    :param raw_data_bucket: S3 bucket containing raw data (used to find all sessions of course).
    :param level: level of job; should be in [all, course, session].
    :param course: course (if level in [course, session]).
    :param session: session number (if level == session).
    :param input_dir: path to temporary /input directory to be mounted in Docker image.
    :param data_dir: path to directory in raw_data_bucket containing course-level directories.
    :return: None
    """
    mode = job_config.mode
    if level == "all": # download data for every course and session
        # download all data; every session of every course
        for bucket in raw_data_bucket:
            for course in fetch_courses(job_config, bucket):
                if mode == "train":
                    sessions = fetch_sessions(job_config, bucket, raw_data_dir, course)
                elif mode == "test":
                    sessions = fetch_sessions(job_config, bucket, raw_data_dir, course, fetch_holdout_session_only=True)
                for session in sessions:
                    fetch_train_test_data(job_config, bucket, raw_data_dir, course, session, input_dir, label_type)
    if level == "course": # download data for every session of course
        if mode == "train":
            sessions = fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course)
        elif mode == "test":
            sessions = fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course, fetch_holdout_session_only=True)
        for session in sessions:
            fetch_train_test_data(job_config, raw_data_bucket, raw_data_dir, course, session, input_dir, label_type)
    if level == "session": # download data for this session only
        fetch_train_test_data(job_config, raw_data_bucket, raw_data_dir, course, session, input_dir, label_type)
    return


def upload_file_to_s3(file, bucket, key, job_config=None, remove_on_success = False):
    """
    Upload file to bucket + key in S3.
    :param file: name or path to file.
    :param bucket: bucket to upload to.
    :param key: key to upload to in bucket.
    :param job_config: MorfJobConfig object; used for logging.
    :return: None
    """
    logger = set_logger_handlers(module_logger, job_config)
    session = boto3.Session()
    s3_client = session.client("s3")
    tc = boto3.s3.transfer.TransferConfig()
    t = boto3.s3.transfer.S3Transfer(client=s3_client, config=tc)
    logger.info("uploading {} to s3://{}/{}".format(file, bucket, key))
    try:
        t.upload_file(file, bucket, key)
        if remove_on_success:
            os.remove(file)
    except Exception as e:
        logger.warn("error caching configurations: {}".format(e))
    return


def delete_s3_keys(job_config, prefix = None):
    """
    Delete any files in s3 bucket matching prefix.
    :param s3:
    :param bucket:
    :param prefix:
    :return:
    """
    logger = set_logger_handlers(module_logger, job_config)
    # begin
    cmd = "{} s3 rm --recursive s3://{}".format(job_config.aws_exec, prefix)
    execute_and_log_output(cmd, logger)
    return


def clear_s3_subdirectory(job_config, course = None, session = None, mode = None):
    """
    Clear all files for user_id, job_id, and mode; used to wipe s3 subdirectory before uploading new files.
    :job_config: MorfJobConfig object.
    :param course:
    :param session:
    :return:
    """
    if not mode: # clear s3 subdirectory for specified mode, not for current mode of job
        mode = job_config.mode
    logger = set_logger_handlers(module_logger, job_config)
    s3_prefix = "/".join([x for x in [job_config.proc_data_bucket, job_config.user_id, job_config.job_id, mode, course, session] if x is not None]) + "/"
    logger.info(" clearing previous job data at s3://{}".format(s3_prefix))
    delete_s3_keys(job_config, prefix = s3_prefix)
    return


def download_model_from_s3(job_config, bucket, key, dest_dir):
    """
    Download and untar a model file from S3; or print a warning message if it doesn't exist.
    :return:
    """
    logger = set_logger_handlers(module_logger, job_config)
    s3 = job_config.initialize_s3()
    mod_url = 's3://{}/{}'.format(bucket, key)
    logger.info(" downloading compressed model file from bucket {} key {}".format(bucket, key))
    try:
        tar_path = initialize_tar(mod_url, s3=s3, dest_dir=dest_dir)
        unarchive_file(tar_path, dest_dir)
    except:
        logger.error("error downloading model file from s3; trained model(s) for this course may not exist. Skipping.")
    return


def download_models(job_config, course, dest_dir, level, session = None):
    """
    Download and untar archived file of pre-trained models for specified user_id/job_id/course.
    :param job_config: MorfJobConfig object.
    :param course: course: course slug for job (string).
    :param dest_dir: location to download models to; this should be /input directory mounted to Docker image.
    :param level: Level for job.
    :param session: Session id for session-level jobs.
    :return: None
    """
    logger = set_logger_handlers(module_logger, job_config)
    bucket = job_config.proc_data_bucket
    user_id = job_config.user_id
    aws_access_key_id = job_config.aws_access_key_id
    aws_secret_access_key = job_config.aws_secret_access_key
    job_id = job_config.job_id
    if level == "all":
        # just one model file
        mod_archive_file = generate_archive_filename(job_config, mode = "train")
        key = make_s3_key_path(job_config, mode ="train", filename = mod_archive_file)
        download_model_from_s3(job_config, bucket, key, dest_dir)
    elif level in ["course","session"]: # model files might be in either course- or session-level directories
        train_files = [obj.key
                       for obj in boto3.resource("s3", aws_access_key_id=aws_access_key_id,
                                                         aws_secret_access_key=aws_secret_access_key)
                           .Bucket(bucket).objects.filter(Prefix="/".join([user_id, job_id, "train"]))
                       if ".tgz" in obj.key.split("/")[-1]  # fetch trained model files only
                       and "train" in obj.key.split("/")[-1]
                       and course in obj.key.split("/")[-1]]
        for key in train_files:
            download_model_from_s3(job_config, bucket, key, dest_dir)
    else:
        logger.error("the procedure for executing this job is unsupported in this version of MORF.")
        raise
    return


def fetch_file(s3, dest_dir, remote_file_url, dest_filename = None, job_config=None):
    """
    Fetch a file into dest_dir.
    :param s3: boto3.client object for s3 connection.
    :param dest_dir: directory to download file to (string).
    :param remote_file_url: url of remote file; must be either file://, s3, or http format (string).
    :param dest_filename: base name of file to use (otherwise defaults to current file name) (string).
    :param job_config: MorfJobConfig object; used for logging.
    :return:
    """
    logger = set_logger_handlers(module_logger, job_config)
    logger.info("retrieving file {} to {}".format(remote_file_url, dest_dir))
    try:
        if not dest_filename:
            dest_filename = os.path.basename(remote_file_url)
        dest_fp = os.path.join(dest_dir, dest_filename)
        url = urlparse(remote_file_url)
        if url.scheme == "file":
            shutil.copyfile(url.path, dest_fp)
        elif url.scheme == "s3":
            bucket = url.netloc
            key = url.path[1:]  # ignore initial /
            download_from_s3(bucket, key, s3, dest_dir, dest_filename = dest_filename, job_config=job_config)
        elif url.scheme == "https":
            urllib.request.urlretrieve(remote_file_url, dest_fp)
        else:
            logger.error(
            "A URL which was not s3:// or file:// or https:// was passed in for a file location, this is not supported. {}"
                .format(remote_file_url))
            sys.exit(-1)
    except Exception as e:
        logger.error("{} when attempting to fetch and copy file at {}".format(e, remote_file_url))
    return dest_fp


def generate_archive_filename(job_config, course=None, session=None, extension ="tgz", mode = None, job_id = None):
    """
    Generate filenames using a consistent and uniquely identifiable format based on user_id, job_id, mode, course, session.
    :param job_config: MorfJobConfig object.
    :param course: course id (string).
    :param session: session number (string).
    :param extension: extension of file to generate name for (the part after the '.'; i.e. 'tgz', 'csv', etc.)
    :param mode: mode for job; only needed if overriding current mode of job_config (string).
    :return: name of file (string).
    """
    if not mode:
        mode = job_config.mode
    if not job_id: # users have option to specify another job_id for forking features
        job_id = job_config.job_id
    job_attributes = [job_config.user_id, job_id, mode, course, session]
    active_attributes = [x for x in job_attributes if x is not None]
    archive_file = '-'.join(active_attributes) + "." + extension
    return archive_file


def make_output_archive_file(output_dir, job_config, course=None, session = None):
    """
    Archive output_dir into archive file, and return name of archive file.
    :param output_dir: directory to compress into archive_file.
    :param mode: mode for job (string); one of: {extract, test, train}.
    :param user_id: user_id for job (string).
    :param job_id: job_id for job (string).
    :param course: course: name of course for job (string).
    :param session: session number of course (string) (optional, only needed when mode == extract).
    :return: name of archive file (string).
    """
    logger = set_logger_handlers(module_logger, job_config)
    archive_file = generate_archive_filename(job_config, course, session)
    # archive results; only save directory structure relative to output_dir (NOT absolute directory structure)
    logger.info(" archiving results to {} as {}".format(output_dir, archive_file))
    # todo: use python tarfile here
    cmd = "tar -cvf {} -C {} .".format(archive_file, output_dir)
    subprocess.call(cmd, shell = True, stdout=open(os.devnull, "wb"), stderr=open(os.devnull, "wb"))
    return archive_file


def move_results_to_destination(archive_file, job_config, course = None, session = None):
    """
    Moves tar of output file to destination, either local file path or s3 url.
    :param archive_file: name of archive output file to move (string).
    :param job_config: MorfJobConfig object.
    :return: None.
    """
    logger = set_logger_handlers(module_logger, job_config)
    bucket = job_config.proc_data_bucket
    key = make_s3_key_path(job_config, filename=archive_file, course = course, session = session)
    logger.info(" uploading results to bucket {} key {}".format(bucket, key))
    s3 = boto3.client('s3')
    try:
        s3.upload_file(archive_file, bucket, key)
    except Exception as e:
        logger.error("error uploading result file: {}".format(e))
    os.remove(archive_file)
    return


def fetch_result_file(job_config, dir, course = None, session = None):
    """
    Download and untar result file for user_id, job_id, mode, and (optional) course and session from job_config.proc_data_bucket.
    :param job_config: MorfJobConfig object.
    :param course: course shorname.
    :param session: session number.
    :return:  None.
    """
    logger = set_logger_handlers(module_logger, job_config)
    s3 = job_config.initialize_s3()
    bucket = job_config.proc_data_bucket
    archive_file = generate_archive_filename(job_config, course, session)
    key = make_s3_key_path(job_config, course=course, session=session,
                           filename=archive_file)
    dest = os.path.join(dir, archive_file)
    logger.info("fetching s3://{}/{}".format(bucket, key))
    with open(dest, 'wb') as resource:
        try:
            s3.download_fileobj(bucket, key, resource)
        except Exception as e:
            logger.warning("exception while fetching results for mode {} course {} session {}:{}".format(job_config.mode, course, session, e))
    unarchive_file(dest, dir)
    return


def initialize_input_output_dirs(working_dir):
    """
    Create local input and output directories in working_dir.
    :param working_dir: local or relative filepath to create input and output directory in.
    :return: None
    """
    input_dir = os.path.join(working_dir, "input")
    if not os.path.exists(input_dir):
        os.mkdir(input_dir)
    output_dir =os.path.join(working_dir, "output")
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    os.chmod(output_dir,stat.S_ISUID | stat.S_ISGID | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRWXG | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP)
    return (input_dir, output_dir)


def remove_readonly(func, path, _):
    """
    Clear the readonly bit and reattempt the removal
    https://docs.python.org/3/library/shutil.html?highlight=shutil#rmtree-example
    """
    os.chmod(path, stat.S_IWRITE)
    func(path)
    return


def cache_job_file_in_s3(job_config, bucket = None, filename ="config.properties"):
    """
    Cache job files in s3 bucket.
    :param job_config: MorfJobConfig object.
    :param bucket: S3 bucket name (string); if not provided then job_config.proc_data_bucket is used.
    :param filename: name of file to upload as (string).
    :return: None
    """
    if not bucket:
        bucket = job_config.proc_data_bucket
    key = make_s3_key_path(job_config, filename = filename)
    upload_file_to_s3(filename, bucket, key, job_config)
    return


def copy_s3_file(job_config, sourceloc, destloc):
    """
    Copy file in location "from" to location "to".
    :param to:
    :return:
    """
    #todo: check format of urls; should be s3
    logger = set_logger_handlers(module_logger, job_config)
    s3 = job_config.initialize_s3()
    assert get_bucket_from_url(sourceloc) == get_bucket_from_url(destloc), "can only copy files within same s3 bucket"
    logger.info(" copying file from {} to {}".format(sourceloc, destloc))
    copy_source = {'Bucket': get_bucket_from_url(sourceloc), 'Key': get_key_from_url(sourceloc)}
    s3.copy_object(CopySource=copy_source, Bucket=get_bucket_from_url(destloc), Key=get_key_from_url(destloc))
    return


def make_feature_csv_name(*args):
    basename = "features.csv"
    csvname = "_".join([str(x) for x in args] + [basename])
    return csvname


def make_label_csv_name(*args):
    basename = "labels.csv"
    csvname = "_".join([str(x) for x in args] + [basename])
    return csvname


def aggregate_session_input_data(file_type, course_dir, course = None):
    """
    Aggregate all csv data files matching pattern within course_dir (recursive file search), and write to a single file in input_dir.
    :param type: {"labels" or "features"}.
    :param course_dir: course directory containing session-level subdirectories which contain data
    :return:
    """
    if not course:
        course = os.path.basename(course_dir)
    valid_types = ("features", "labels")
    assert file_type in valid_types, "[ERROR] specify either features or labels as type."
    df_out = pd.DataFrame()
    # read file from each session, and concatenate into df_out
    for root, dirs, files in os.walk(course_dir, topdown=False):
        for session in dirs:
            session_csv = "_".join([course, session, file_type]) + ".csv"
            session_feats = os.path.join(root, session, session_csv)
            session_df = pd.read_csv(session_feats)
            df_out = pd.concat([df_out, session_df])
            os.remove(session_feats)
            session_dir = os.path.join(root, session)
            if not os.listdir(session_dir): # if session_dir is now empty, remove it
                os.rmdir(session_dir)
    # write single csv file
    if file_type == "features":
        outfile = make_feature_csv_name(course, file_type)
    elif file_type == "labels":
        outfile = "{}_{}.csv".format(course, file_type) #todo: use make_label_csv_name after updating that function
    outpath = os.path.join(course_dir, outfile)
    df_out.to_csv(outpath, index=False)
    return outpath



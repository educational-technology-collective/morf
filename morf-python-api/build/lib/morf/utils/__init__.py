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


import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import urllib.request

import boto3
import pandas as pd
from urllib.parse import urlparse


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


def download_from_s3(bucket, key, s3, dir = os.getcwd(), dest_filename = None):
    """
    Downloads a file from s3 into dir and returns its path as a string for optional use.
    :param bucket: an s3 bucket name (string).
    :param key: key of a file in bucket (string).
    :param s3: boto3.client object for s3 connection.
    :param dir: directory where file should be downloaded (string).
    :param dest_filename: base name for file.
    :return: Path to downloaded file inside dir (string).
    """
    if not dest_filename:
        dest_filename = os.path.basename(key)
    with open(os.path.join(dir, dest_filename), "wb") as resource:
        s3.download_fileobj(bucket, key, resource)
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


def fetch_sessions(job_config, data_bucket, data_dir, course, fetch_holdout_session_only = False):
    """
    Fetch course sessions in data_bucket/data_dir.
    :param job_config: MorfJobConfig object.
    :param data_bucket: name of bucket containing data; s3 should have read/copy access to this bucket.
    :param data_dir: path to directory in data_bucket that contains course-level directories of raw data.
    :param course: string; name of course (should match course-level directory name in s3 directory tree).
    :param fetch_holdout_session_only: logical; used to determine whether to fetch holdout (final) session or a list of all training sessions (all other sessions besides holdout).
    :return: list of session numbers as strings.
    """
    s3 = job_config.initialize_s3()
    if not data_dir.endswith("/"):
        data_dir = data_dir + "/"
    course_bucket_objects = s3.list_objects(Bucket=data_bucket, Prefix="".join([data_dir, course, "/"]), Delimiter="/")
    sessions = [item.get("Prefix").split("/")[2] for item in course_bucket_objects.get("CommonPrefixes")]
    sessions = sorted(sessions, key = lambda x: x[-3:]) # handles session numbers like "2012-001" by keeping leading digits before "-" but only sorts on last 3 digits
    holdout_session = sessions.pop(-1)
    if fetch_holdout_session_only == True:
        return [holdout_session]
    else:
        return sessions


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


def download_raw_course_data(s3, aws_access_key_id, aws_secret_access_key, bucket, course, session, input_dir,
                             course_date_file_url, data_dir):
    """
    Download all raw course files for course and session into input_dir.
    :param s3: boto3.client object for s3 connection.
    :param aws_access_key_id: aws_access_key_id.
    :param aws_secret_access_key: aws_secret_access_key.
    :param bucket: bucket containing raw data.
    :param course: id of course to download data for.
    :param session: id of session to download data for.
    :param input_dir: input directory.
    :param course_date_file_url: url of course date file.
    :param data_dir: directory in bucket that contains course-level data.
    :return: None
    """
    session_input_dir = os.path.join(input_dir, course, session)
    os.makedirs(session_input_dir)
    for obj in boto3.resource("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)\
            .Bucket(bucket).objects.filter(Prefix="{}/{}/{}/".format(data_dir, course, session)):
        filename = obj.key.split("/")[-1]
        filename = re.sub('[\s\(\)":!&]', "", filename)
        filepath = "{}/{}".format(session_input_dir, filename)
        try:
            with open(filepath, "wb") as resource:
                s3.download_fileobj(bucket, obj.key, resource)
        except:
            print("[WARNING] skipping empty object in bucket {} key {}".format(bucket, obj.key))
            continue
    dates_bucket = get_bucket_from_url(course_date_file_url)
    dates_key = get_key_from_url(course_date_file_url)
    dates_file = dates_key.split("/")[-1]
    s3.download_file(dates_bucket, dates_key, os.path.join(session_input_dir, dates_file))
    # unzip all of the sql files
    unzip_sql_cmd = """for i in `find {} -name "*.sql.gz"`; do gunzip "$i" ; done""".format(session_input_dir)
    subprocess.call(unzip_sql_cmd, shell = True, stdout=open(os.devnull, "wb"), stderr=open(os.devnull, "wb"))
    return


def initialize_labels(s3, aws_access_key_id, aws_secret_access_key, bucket, course, session, mode, label_type, dest_dir, data_dir):
    """
    Download labels file and extract results for course and session into labels.csv.
    :param s3: boto3.client object for s3 connection.
    :param aws_access_key_id: aws_access_key_id.
    :param aws_secret_access_key: aws_secret_access_key.
    :param bucket: bucket with extracted data.
    :param course: course to fetch data for.
    :param session: session to fetch data for.
    :param mode: mode of job.
    :param label_type: valid label type to place in 'label' column
    :param dest_dir: directory to load data into. This should be same directory mounted to Docker image in docker run command.
    :param data_dir: directory in bucket containing course-level data directories.
    :return: None
    """
    label_csv = "labels-{}.csv".format(mode) # file with labels for ALL courses
    label_csv_fp = "{}/{}".format(dest_dir, label_csv)
    course_label_csv_fp = "{}/{}_{}_labels.csv".format(dest_dir, course, session)
    key = data_dir + label_csv
    with open(label_csv_fp, "wb") as resource:
        s3.download_fileobj(bucket, key, resource)
    df = pd.read_csv(label_csv_fp, dtype=object)
    course_label_df = df.loc[(df["course"] == course) & (df["session"] == session) & (df["label_type"] == label_type)]\
        .copy()
    course_label_df.drop(["course", "session", "label_type"], axis = 1, inplace=True)
    course_label_df.to_csv(course_label_csv_fp, index=False)
    os.remove(label_csv_fp)
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
    s3 = job_config.initialize_s3()
    aws_access_key_id = job_config.aws_access_key_id
    aws_secret_access_key = job_config.aws_secret_access_key
    proc_data_bucket = job_config.proc_data_bucket
    mode = job_config.mode
    user_id = job_config.user_id
    job_id = job_config.job_id
    if mode == "train":
        fetch_mode = "extract"
    if mode == "test":
        fetch_mode = "extract-holdout"
    print("[INFO] fetching {} data for course {} session {}".format(fetch_mode, course, session))
    session_input_dir = os.path.join(input_dir, course, session)
    os.makedirs(session_input_dir)
    # download features file
    feature_csv = generate_archive_filename(job_config, mode=fetch_mode, extension="csv")
    key = "{}/{}/{}/{}".format(user_id, job_id, fetch_mode, feature_csv)
    download_from_s3(proc_data_bucket, key, s3, session_input_dir)
    # read features file and filter to only include specific course/session
    local_feature_csv = os.path.join(session_input_dir, feature_csv)
    temp_df = pd.read_csv(local_feature_csv, dtype = object)
    outfile = os.path.join(session_input_dir, "{}_{}_features.csv".format(course, session))
    temp_df[(temp_df["course"] == course) & (temp_df["session"] == session)].drop(["course", "session"], axis = 1)\
        .to_csv(outfile, index = False)
    os.remove(local_feature_csv)
    if mode == "train": #download labels only if training job; otherwise no labels needed
        initialize_labels(s3, aws_access_key_id, aws_secret_access_key, raw_data_bucket, course, session, mode,
                          label_type, dest_dir = session_input_dir, data_dir = raw_data_dir)
    return


def initialize_raw_course_data(job_config, raw_data_bucket, level, mode,
                               data_dir ="morf-data", course = None, session = None, input_dir ="./input",
                               course_date_file_name ="coursera_course_dates.csv"):
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
    s3 = job_config.initialize_s3()
    aws_access_key_id = job_config.aws_access_key_id
    aws_secret_access_key = job_config.aws_secret_access_key
    if level != "all": # course_date_file_url is unique across the entire job

        course_date_file_url = "s3://{}/{}/{}".format(raw_data_bucket, data_dir, course_date_file_name)
    if level == "all": # there is a unique course date file for each bucket
        # download all data; every session of every course
        for bucket in raw_data_bucket:
            course_date_file_url = "s3://{}/{}/{}".format(bucket, data_dir, course_date_file_name)
            for course in fetch_courses(job_config, bucket):
                if mode == "extract":
                    sessions = fetch_sessions(job_config, bucket, data_dir, course)
                    for session in sessions:
                        download_raw_course_data(s3, aws_access_key_id, aws_secret_access_key, bucket=bucket,
                                                 course=course, session=session, input_dir=input_dir,
                                                 course_date_file_url=course_date_file_url, data_dir=data_dir)
                if mode == "extract-holdout":
                    holdout_session = fetch_sessions(job_config, bucket, data_dir, course, fetch_holdout_session_only=True)[0]
                    download_raw_course_data(s3, aws_access_key_id, aws_secret_access_key, bucket=bucket,
                                             course=course, session=holdout_session, input_dir=input_dir,
                                             course_date_file_url=course_date_file_url, data_dir=data_dir)
    elif level == "course":
        # download all data for every session of course
        if mode == "extract":
            sessions = fetch_sessions(job_config, raw_data_bucket, data_dir, course)
            for session in sessions:
                download_raw_course_data(s3, aws_access_key_id, aws_secret_access_key, bucket=raw_data_bucket,
                                         course=course, session=session, input_dir=input_dir,
                                         course_date_file_url=course_date_file_url, data_dir=data_dir)
        if mode == "extract-holdout":
            holdout_session = fetch_sessions(job_config, raw_data_bucket, data_dir, course, fetch_holdout_session_only=True)[0]
            download_raw_course_data(s3, aws_access_key_id, aws_secret_access_key, bucket=raw_data_bucket,
                                     course=course, session=holdout_session, input_dir=input_dir,
                                     course_date_file_url=course_date_file_url, data_dir=data_dir)
    elif level == "session":
        # download only specific session
        download_raw_course_data(s3, aws_access_key_id, aws_secret_access_key, bucket=raw_data_bucket,
                                 course=course, session=session, input_dir=input_dir,
                                 course_date_file_url=course_date_file_url, data_dir=data_dir)
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
                    download_train_test_data(job_config, bucket, raw_data_dir, course, session, input_dir, label_type)
    if level == "course": # download data for every session of course
        if mode == "train":
            sessions = fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course)
        elif mode == "test":
            sessions = fetch_sessions(job_config, raw_data_bucket, raw_data_dir, course, fetch_holdout_session_only=True)
        for session in sessions:
            download_train_test_data(job_config, raw_data_bucket, raw_data_dir, course, session, input_dir, label_type)
    if level == "session": # download data for this session only
        download_train_test_data(job_config, raw_data_bucket, raw_data_dir, course, session, input_dir, label_type)
    return


def upload_file_to_s3(file, bucket, key):
    """
    Upload file to bucket + key in S3.
    :param file: name or path to file.
    :param bucket: bucket to upload to.
    :param key: key to upload to in bucket.
    :return: None
    """
    session = boto3.Session()
    s3_client = session.client("s3")
    tc = boto3.s3.transfer.TransferConfig()
    t = boto3.s3.transfer.S3Transfer(client=s3_client, config=tc)
    print("[INFO] uploading {} to s3://{}/{}".format(file, bucket, key))
    try:
        t.upload_file(file, bucket, key)
    except Exception as e:
        print("[WARNING] error caching configurations: {}".format(e))
    return


def delete_s3_keys(bucket, prefix = None):
    """
    Delete any files in s3 bucket matching prefix.
    :param s3:
    :param bucket:
    :param prefix:
    :return:
    """
    s3 = boto3.resource('s3')
    objects_to_delete = s3.meta.client.list_objects(Bucket=bucket, Prefix=prefix)
    delete_keys = {'Objects': []}
    delete_keys['Objects'] = [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]
    try:
        s3.meta.client.delete_objects(Bucket=bucket, Delete=delete_keys)
        # s3.delete_keys(keys_to_clear)
    except Exception as e:
        print("[ERROR]: exception when cleaning S3 bucket: {}; continuing".format(e))
    return


def clear_s3_subdirectory(job_config, course = None, session = None):
    """
    Clear all files for user_id, job_id, and mode; used to wipe s3 subdirectory before uploading new files.
    :job_config: MorfJobConfig object.
    :param course:
    :param session:
    :return:
    """
    s3_prefix = "/".join([x for x in [job_config.user_id, job_config.job_id, job_config.mode, course, session] if x is not None]) + "/"
    print("[INFO] clearing previous job data at s3://{}".format(s3_prefix))
    delete_s3_keys(job_config.proc_data_bucket, prefix = s3_prefix)
    return


def compile_test_results(s3, courses, bucket, user_id, job_id, temp_dir = "./temp/"):
    """
    Aggregate model testing results from individual courses, if they exist.
    :param s3: boto3.client object for s3 connection.
    :param courses: courses to compile results for.
    :param bucket: get_bucket_from_url(get_config_properties()['output_data_location'])
    :param user_id: user id for job.
    :param job_id: job id for job.
    :param temp_dir: temporary directory location; cleaned up after course is processed
    :return: None
    """
    summary_df_list = []
    for course in courses:
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
        archive_file = "{}-{}-{}-{}.tgz".format(user_id, job_id, "test", course)
        key = "{}/{}/{}/{}/{}".format(user_id, job_id, "test", course, archive_file)
        dest = temp_dir+archive_file
        # download file
        with open(dest, "wb") as fil:
            # copy docker image into working directory
            try:
                s3.download_fileobj(bucket, key, fil)
                print("[INFO] fetching test summary for course {}".format(course))
            except:
                print("[WARNING] no test data found for course {}; skipping".format(course))
                shutil.rmtree(temp_dir)
                continue
        #untar file
        tar = tarfile.open(dest)
        tar.extractall(temp_dir)
        tar.close()
        # find all model summary files and read into dataframe
        course_summary_file = "{}{}_test_summary.csv".format(temp_dir, course)
        try:
            course_summary_df = pd.read_csv(course_summary_file)
            course_summary_df["course"] = course
            summary_df_list.append(course_summary_df)
        except FileNotFoundError:
            print("[WARNING] no test summary found for course {}; skipping".format(course))
        shutil.rmtree(temp_dir)
    master_summary_df = pd.concat(summary_df_list, axis = 0)
    master_summary_filename = "{}_{}_model_performace_summary.csv".format(user_id, job_id)
    master_summary_df.to_csv(master_summary_filename, index = False, header = True)
    upload_key = "{}/{}/test/{}".format(user_id, job_id, master_summary_filename)
    print("[INFO] uploading results to s3://{}/{}".format(bucket, upload_key))
    upload_file_to_s3(master_summary_filename, bucket, upload_key)
    os.remove(master_summary_filename)
    return None


def download_model_from_s3(bucket, key, s3, dest_dir):
    """
    Download and untar a model file from S3; or print a warning message if it doesn't exist.
    :return:
    """
    mod_url = 's3://{}/{}'.format(bucket, key)
    print("[INFO] downloading compressed model file from bucket {} key {}".format(bucket, key))
    try:
        tar_path = initialize_tar(mod_url, s3=s3, dest_dir=dest_dir)
        tar = tarfile.open(tar_path)
        tar.extractall(dest_dir)
        tar.close()
    except:
        sys.exit(
            "[WARNING] error downloading model file from s3; trained model(s) for this course may not exist. Skipping.")
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
    bucket = job_config.proc_data_bucket
    user_id = job_config.user_id
    s3 = job_config.initialize_s3()
    aws_access_key_id = job_config.aws_access_key_id
    aws_secret_access_key = job_config.aws_secret_access_key
    job_id = job_config.job_id
    if level == "all":
        # just one model file
        mod_archive_file = generate_archive_filename(job_config, mode = "train")
        key = make_s3_key_path(job_config, mode = "train", filename = mod_archive_file)
        download_model_from_s3(bucket, key, s3, dest_dir)
    elif level in ["course","session"]: # model files might be in either course- or session-level directories
        train_files = [obj.key
                       for obj in boto3.resource("s3", aws_access_key_id=aws_access_key_id,
                                                         aws_secret_access_key=aws_secret_access_key)
                           .Bucket(bucket).objects.filter(Prefix="/".join([user_id, job_id, "train"]))
                       if ".tgz" in obj.key.split("/")[-1]  # fetch trained model files only
                       and "train" in obj.key.split("/")[-1]
                       and course in obj.key.split("/")[-1]]
        for key in train_files:
            download_model_from_s3(bucket, key, s3, dest_dir)
    else:
        print("[ERROR] the procedure for executing this job is unsupported in this version of MORF.")
        raise
    return


def fetch_file(s3, dest_dir, remote_file_url, dest_filename = None):
    """

    :param s3: boto3.client object for s3 connection.
    :param dest_dir: directory to download file to (string).
    :param remote_file_url: url of remote file; must be either file://, s3, or http format (string).
    :param dest_filename: base name of file to use (otherwise defaults to current file name) (string).
    :return:
    """
    print("[INFO] retrieving file {} to {}".format(remote_file_url, dest_dir))
    try:
        if not dest_filename:
            dest_filename = os.path.basename(remote_file_url)
        url = urlparse(remote_file_url)
        if url.scheme == "file":
            shutil.copyfile(url.path, os.path.join(dest_dir, dest_filename))
        elif url.scheme == "s3":
            bucket = url.netloc
            key = url.path[1:]  # ignore initial /
            download_from_s3(bucket, key, s3, dest_dir, dest_filename = dest_filename)
        elif url.scheme == "https":
            urllib.request.urlretrieve(remote_file_url, os.path.join(dest_dir, dest_filename))
        else:
            print(
            "[ERROR] A URL which was not s3:// or file:// or https:// was passed in for a file location, this is not supported. {}"
                .format(remote_file_url))
            sys.exit(-1)
    except Exception as e:
        print("[ERROR] {} when attempting to fetch and copy file at {}".format(e, remote_file_url))
    return


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
    archive_file = generate_archive_filename(job_config, course, session)
    # archive results; only save directory structure relative to output_dir (NOT absolute directory structure)
    print("[INFO] archiving results to {} as {}".format(output_dir, archive_file))
    # todo: use python tarfile here
    cmd = "tar -cvf {} -C {} .".format(archive_file, output_dir)
    subprocess.call(cmd, shell = True, stdout=open(os.devnull, "wb"), stderr=open(os.devnull, "wb"))
    return archive_file


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


def move_results_to_destination(archive_file, job_config, course = None, session = None):
    """
    Moves tar of output file to destination, either local file path or s3 url.
    :param archive_file: name of archive output file to move (string).
    :param job_config: MorfJobConfig object.
    :return: None.
    """
    bucket = job_config.proc_data_bucket
    key = make_s3_key_path(job_config, filename=archive_file, course = course, session = session)
    print("[INFO] uploading results to bucket {} key {}".format(bucket, key))
    session = boto3.Session()
    s3_client = session.client("s3")
    tc = boto3.s3.transfer.TransferConfig()
    t = boto3.s3.transfer.S3Transfer(client=s3_client, config=tc)
    t.upload_file(archive_file, bucket, key)
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
    s3 = job_config.initialize_s3()
    bucket = job_config.proc_data_bucket
    archive_file = generate_archive_filename(job_config, course, session)
    key = make_s3_key_path(job_config, course=course, session=session,
                           filename=archive_file)
    dest = os.path.join(dir, archive_file)
    print("[INFO] fetching s3://{}/{}".format(bucket, key))
    with open(dest, 'wb') as resource:
        s3.download_fileobj(bucket, key, resource)
    tar = tarfile.open(dest)
    tar.extractall(dir)
    tar.close()
    os.remove(dest)
    return


def initialize_input_output_dirs(working_dir):
    """
    Create local input and output directories in working_dir.
    :param working_dir: local or relative filepath to create input and output directory in.
    :return: None
    """
    input_dir = "{0}/input/".format(working_dir)
    os.mkdir(input_dir)
    output_dir = "{0}/output/".format(working_dir)
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
    upload_file_to_s3(filename, bucket, key)
    return


def copy_s3_file(job_config, sourceloc, destloc):
    """
    Copy file in location "from" to location "to".
    :param to:
    :return:
    """
    #todo: check format of urls; should be s3
    s3 = job_config.initialize_s3()
    assert get_bucket_from_url(sourceloc) == get_bucket_from_url(destloc), "can only copy files within same s3 bucket"
    print("[INFO] copying file from {} to {}".format(sourceloc, destloc))
    copy_source = {'Bucket': get_bucket_from_url(sourceloc), 'Key': get_key_from_url(sourceloc)}
    s3.copy_object(CopySource=copy_source, Bucket=get_bucket_from_url(destloc), Key=get_key_from_url(destloc))
    return

